const _ = require('lodash'),
	utils = require('../util/Utils'),
	constants = require('../util/constants'),
	defaultMailboxFactory = require('./mailbox'),
	EventEmitter = require('events').EventEmitter,
	fileName = __filename,
	logger = require('pomelo-logger').getLogger('pomelo-rpc', fileName);

const STATE_INITED = 1;    // station has inited
const STATE_STARTED = 2;   // station has started
const STATE_CLOSED = 3;    // station has closed

/**
 * Mail station constructor.
 *
 * @param {Object} opts construct parameters
 */
class MailStation extends EventEmitter
{
	constructor(opts)
	{
		super();
		this.opts = opts;
		// remote server info map, key: server id, value: info
		this.servers = {};
		// remote server info map, key: serverType, value: servers array
		this.serversMap = {};
		// remote server online map, key: server id, value: 0/offline 1/online
		this.onlines = {};
		this.mailboxFactory = opts.mailboxFactory || defaultMailboxFactory;

		// filters
		this.befores = [];
		this.afters = [];

		// pending request queues
		this.pendings = {};
		this.pendingSize = opts.pendingSize || constants.DEFAULT_PARAM.DEFAULT_PENDING_SIZE;

		// connecting remote server mailbox map
		this.connecting = {};

		// working mailbox map
		this.mailboxes = {};

		this.state = STATE_INITED;
	}

	/**
	 * Init and start station. Connect all mailbox to remote servers.
	 *
	 * @param  {Function} cb(err) callback function
	 * @return {Void}
	 */
	start(cb)
	{
		if (this.state > STATE_INITED)
		{
			utils.InvokeCallback(cb, new Error('station has started.'));
			return;
		}
		process.nextTick(() =>
		{
			this.state = STATE_STARTED;
			utils.InvokeCallback(cb);
		});
	}

	/**
	 * Stop station and all its mailboxes
	 *
	 * @param  {Boolean} force whether stop station forcely
	 * @return {Void}
	 */
	stop(force)
	{
		if (this.state !== STATE_STARTED)
		{
			logger.warn('[pomelo-rpc] client is not running now.');
			return;
		}
		this.state = STATE_CLOSED;

		const closeAll = () =>
		{
			for (const id in this.mailboxes)
			{
				this.mailboxes[id].close();
			}
		};
		if (force)
		{
			closeAll();
		}
		else
		{
			setTimeout(closeAll, constants.DEFAULT_PARAM.GRACE_TIMEOUT);
		}
	}

	/**
	 * Add a new server info into the mail station and clear
	 * the blackhole associated with the server id if any before.
	 *
	 * @param {Object} serverInfo server info such as {id, host, port}
	 */
	addServer(serverInfo)
	{
		if (!serverInfo || !serverInfo.id)
		{
			return;
		}

		const id = serverInfo.id;
		const type = serverInfo.serverType;
		this.servers[id] = serverInfo;
		this.onlines[id] = 1;

		if (!this.serversMap[type])
		{
			this.serversMap[type] = [];
		}
		this.serversMap[type].push(id);
		this.emit('addServer', id);
	}

	/**
	 * Batch version for add new server info.
	 *
	 * @param {Array} serverInfos server info list
	 */
	addServers(serverInfos)
	{
		if (!serverInfos || !serverInfos.length)
		{
			return;
		}
		let i = 0;
		const l = serverInfos.length;
		for (i = 0; i < l; i++)
		{
			this.addServer(serverInfos[i]);
		}
	}

	/**
	 * Remove a server info from the mail station and remove
	 * the mailbox instance associated with the server id.
	 *
	 * @param  {String|Number} id server id
	 */
	removeServer(id)
	{
		this.onlines[id] = 0;
		const mailbox = this.mailboxes[id];
		if (mailbox)
		{
			mailbox.close();
			delete this.mailboxes[id];
		}
		this.emit('removeServer', id);
	}

	/**
	 * Batch version for remove remote servers.
	 *
	 * @param  {Array} ids server id list
	 */
	removeServers(ids)
	{
		if (!ids || !ids.length)
		{
			return;
		}

		let i = 0;
		const l = ids.length;
		for (i = 0; i < l; i++)
		{
			this.removeServer(ids[i]);
		}
	}

	/**
	 * Clear station infomation.
	 *
	 */
	clearStation()
	{
		this.onlines = {};
		this.serversMap = {};
	}

	/**
	 * Replace remote servers info.
	 *
	 * @param {Array} serverInfos server info list
	 */
	replaceServers(serverInfos)
	{
		this.clearStation();
		if (!serverInfos || !serverInfos.length)
		{
			return;
		}
		let i = 0;
		const l = serverInfos.length;
		for (i = 0; i < l; i++)
		{
			const id = serverInfos[i].id;
			const type = serverInfos[i].serverType;
			this.onlines[id] = 1;
			if (!this.serversMap[type])
			{
				this.serversMap[type] = [];
			}
			this.servers[id] = serverInfos[i];
			this.serversMap[type].push(id);
		}
	}

	/**
	 * Dispatch rpc message to the mailbox
	 *
	 * @param  {Object}   tracer   rpc debug tracer
	 * @param  {String}   serverId remote server id
	 * @param  {Object}   msg      rpc invoke message
	 * @param  {Object}   opts     rpc invoke option args
	 * @param  {Function} cb       callback function
	 * @return {Void}
	 */
	dispatch(...args)
	{
		const tracer = args[0], serverId = args[1], msg = args[2], opts = args[3], cb = args[4];
		tracer.info('client', fileName, 'dispatch', 'dispatch rpc message to the mailbox');
		tracer.cb = cb;
		if (this.state !== STATE_STARTED)
		{
			tracer.error('client', fileName, 'dispatch', 'client is not running now');
			logger.error('[pomelo-rpc] client is not running now.');
			this.emit('error', constants.RPC_ERROR.SERVER_NOT_STARTED, tracer, serverId, msg, opts);
			return;
		}

		const mailbox = this.mailboxes[serverId];
		if (!mailbox)
		{
			tracer.debug('client', fileName, 'dispatch', 'mailbox is not exist');
			// try to connect remote server if mailbox instance not exist yet
			if (!MailstationUtility.LazyConnect(tracer, this, serverId, this.mailboxFactory, cb))
			{
				tracer.error('client', fileName, 'dispatch', `fail to find remote server:${serverId}`);
				logger.error(`[pomelo-rpc] fail to find remote server:${serverId}`);
				this.emit('error', constants.RPC_ERROR.NO_TRAGET_SERVER, tracer, serverId, msg, opts);
			}
			// push request to the pending queue
			MailstationUtility.AddToPending(tracer, this, serverId, args);
			return;
		}

		if (this.connecting[serverId])
		{
			tracer.debug('client', fileName, 'dispatch', 'request add to connecting');
			// if the mailbox is connecting to remote server
			MailstationUtility.AddToPending(tracer, this, serverId, args);
			return;
		}

		const send = (tracer, err, serverId, msg, opts) =>
		{
			tracer.info('client', fileName, 'send', 'get corresponding mailbox and try to send message');
			const mailbox = this.mailboxes[serverId];
			if (err)
			{
				MailstationUtility.ErrorHandler(tracer, this, err, serverId, msg, opts, true, cb);
				return;
			}
			if (!mailbox)
			{
				tracer.error('client', fileName, 'send', `can not find mailbox with id:${serverId}`);
				logger.error(`[pomelo-rpc] could not find mailbox with id:${serverId}`);
				this.emit('error', constants.RPC_ERROR.FAIL_FIND_MAILBOX, tracer, serverId, msg, opts);
				return;
			}
			mailbox.send(tracer, msg, opts, (...args) =>
			{
				const tracerSend = args[0];
				const sendErr = args[1];
				if (sendErr)
				{
					logger.error('[pomelo-rpc] fail to send message');
					this.emit('error', constants.RPC_ERROR.FAIL_SEND_MESSAGE, tracer, serverId, sendErr, opts);
					return;
				}
				const argsArray = _.drop(args, 2);
				MailstationUtility.DoFilter(tracerSend, null, serverId, msg, opts, this.afters, 0, 'after', (tracer, err, serverId, msg, opts) =>
				{
					if (err)
					{
						MailstationUtility.ErrorHandler(tracer, this, err, serverId, msg, opts, false, cb);
					}
					utils.ApplyCallback(cb, argsArray);
				});
			});
		};

		MailstationUtility.DoFilter(tracer, null, serverId, msg, opts, this.befores, 0, 'before', send);
	}

	/**
	 * Add a before filter
	 *
	 * @param  {[type]} filter [description]
	 * @return {[type]}        [description]
	 */
	before(filter)
	{
		if (Array.isArray(filter))
		{
			this.befores = this.befores.concat(filter);
			return;
		}
		this.befores.push(filter);
	}

	/**
	 * Add after filter
	 *
	 * @param  {[type]} filter [description]
	 * @return {[type]}        [description]
	 */
	after(filter)
	{
		if (Array.isArray(filter))
		{
			this.afters = this.afters.concat(filter);
			return;
		}
		this.afters.push(filter);
	}

	/**
	 * Add before and after filter
	 *
	 * @param  {[type]} filter [description]
	 * @return {[type]}        [description]
	 */
	filter(filter)
	{
		this.befores.push(filter);
		this.afters.push(filter);
	}

	/**
	 * Try to connect to remote server
	 *
	 * @param  {Object}   tracer   rpc debug tracer
	 * @return {String}   serverId remote server id
	 * @param  {Function}   cb     callback function
	 */
	connect(tracer, serverId, cb)
	{
		const mailbox = this.mailboxes[serverId];
		mailbox.connect(tracer, (err) =>
		{
			if (err)
			{
				tracer.error('client', fileName, 'connect', `fail to connect to remote server: ${serverId}`);
				logger.error(`[pomelo-rpc] mailbox fail to connect to remote server: ${serverId}`);
				if (this.mailboxes[serverId])
				{
					delete this.mailboxes[serverId];
				}
				this.emit('error', constants.RPC_ERROR.FAIL_CONNECT_SERVER, tracer, serverId, null, this.opts);
				return;
			}
			mailbox.on('close', (id) =>
			{
				const mbox = this.mailboxes[id];
				if (mbox)
				{
					mbox.close();
					delete this.mailboxes[id];
				}
				this.emit('close', id);
			});
			delete this.connecting[serverId];
			MailstationUtility.FlushPending(tracer, this, serverId);
		});
	}
}

class MailstationUtility
{
	/**
	 * Do before or after filter
	 */
	static DoFilter(tracer, err, serverId, msg, opts, filters, index, operate, cb)
	{
		if (index < filters.length)
		{
			tracer.info('client', fileName, 'DoFilter', `do ${operate} filter ${filters[index].name}`);
		}
		if (index >= filters.length || Boolean(err))
		{
			utils.InvokeCallback(cb, tracer, err, serverId, msg, opts);
			return;
		}
		const filter = filters[index];
		let filterFunction = null;
		if (typeof filter === 'function')
		{
			filterFunction = filter;
		}
		else if (typeof filter[operate] === 'function')
		{
			filterFunction = filter[operate];
		}

		if (filterFunction === 'function')
		{
			filterFunction(serverId, msg, opts, (target, message, options) =>
			{
				index++;
				// compatible for pomelo filter next(err) method
				if (utils.GetObjectClass(target) === 'Error')
				{
					MailstationUtility.DoFilter(tracer, target, serverId, msg, opts, filters, index, operate, cb);
				}
				else
				{
					MailstationUtility.DoFilter(tracer, null, target || serverId, message || msg, options || opts, filters, index, operate, cb);
				}
			});
			return;
		}
		index++;
		MailstationUtility.DoFilter(tracer, err, serverId, msg, opts, filters, index, operate, cb);
	}

	static LazyConnect(tracer, station, serverId, factory, cb)
	{
		tracer.info('client', fileName, 'LazyConnect', 'create mailbox and try to connect to remote server');
		const server = station.servers[serverId];
		const online = station.onlines[serverId];
		if (!server)
		{
			logger.error('[pomelo-rpc] unknown server: %s', serverId);
			return false;
		}
		if (!online || online !== 1)
		{
			logger.error('[pomelo-rpc] server is not online: %s', serverId);
		}
		const mailbox = factory.create(server, station.opts);
		station.connecting[serverId] = true;
		station.mailboxes[serverId] = mailbox;
		station.connect(tracer, serverId, cb);
		return true;
	}

	static AddToPending(tracer, station, serverId, args)
	{
		tracer.info('client', fileName, 'AddToPending', 'add pending requests to pending queue');
		let pending = station.pendings[serverId];
		if (!pending)
		{
			pending = station.pendings[serverId] = [];
		}
		if (pending.length > station.pendingSize)
		{
			tracer.debug('client', fileName, 'AddToPending', `station pending too much for: ${serverId}`);
			logger.warn('[pomelo-rpc] station pending too much for: %s', serverId);
			return;
		}
		pending.push(args);
	}

	static FlushPending(tracer, station, serverId, cb)
	{
		tracer.info('client', fileName, 'FlushPending', 'flush pending requests to dispatch method');
		const pending = station.pendings[serverId];
		const mailbox = station.mailboxes[serverId];
		if (!pending || !pending.length)
		{
			return;
		}
		if (!mailbox)
		{
			tracer.error('client', fileName, 'FlushPending', `fail to flush pending messages for empty mailbox: ${serverId}`);
			logger.error(`[pomelo-rpc] fail to flush pending messages for empty mailbox: ${serverId}`);
		}
		for (let i = 0, l = pending.length; i < l; i++)
		{
			station.dispatch(...pending[i]);
		}
		delete station.pendings[serverId];
	}

	static ErrorHandler(tracer, station, err, serverId, msg, opts, flag, cb)
	{
		if (station.handleError)
		{
			station.handleError(err, serverId, msg, opts);
		}
		else
		{
			logger.error('[pomelo-rpc] rpc filter error with serverId: %s, err: %j', serverId, err.stack);
			station.emit('error', constants.RPC_ERROR.FILTER_ERROR, tracer, serverId, msg, opts);
		}
	}

	/**
	 * Mail station factory function.
	 *
	 * @param  {Object} opts construct paramters
	 *           opts.servers {Object} global server info map. {serverType: [{id, host, port, ...}, ...]}
	 *           opts.mailboxFactory {Function} mailbox factory function
	 * @return {Object}      mail station instance
	 */
	static Create(opts)
	{
		return new MailStation(opts || {});
	}
}

module.exports.create = MailstationUtility.Create;
