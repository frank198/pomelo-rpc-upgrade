'use strict';
const logger = require('pomelo-logger-upgrade').getLogger('pomelo_rpc', 'MailStation');
const EventEmitter = require('events').EventEmitter;
const constants = require('../util/constants');

const STATE_INITED = 1; // station has inited
const STATE_STARTED = 2; // station has started
const STATE_CLOSED = 3; // station has closed

class MailStation extends EventEmitter
{
    constructor(opts)
    {
        super();
        this.opts = opts || {};
        this.servers = {}; // remote server info map, key: server id, value: info
        this.serversMap = {}; // remote server info map, key: serverType, value: servers array
        this.onlines = {}; // remote server online map, key: server id, value: 0/offline 1/online
        this.mailboxFactory = opts.mailboxFactory || MailStation.GetServer(opts.rpcType);

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

    static GetServer(rpcType)
    {
        switch (rpcType) {
            case 'grpc':
                return require('./mailboxes/GrpcHttp2Client');
            case 'thrift':
                return require('./mailboxes/ThriftClient');
        }
        return require('./mailboxes/mqtt-mailbox');
    }

    /**
	 * Add a before filter
	 *
	 * @param  {[type]} filter [description]
	 * @return {[type]}        [description]
	 */
    before(filter)
    {
        this.befores = this.befores.concat(filter);
    }

    /**
	 * Add after filter
	 *
	 * @param  {[type]} filter [description]
	 * @return {[type]}        [description]
	 */
    after(filter)
    {
        this.afters = this.afters.concat(filter);
    }

    /**
	 * Add before and after filter
	 *
	 * @param  {[type]} filter [description]
	 * @return {[type]}        [description]
	 */
    filter(filter)
    {
        this.befores = this.befores.concat(filter);
        this.afters = this.afters.concat(filter);
    }

    /**
	 * Try to connect to remote server
	 *
	 * @return {string}   serverId remote server id
	 * @param  {function}   cb     callback function
	 */
    connect(serverId, cb)
    {
        const mailbox = this.mailboxes[serverId];
        mailbox.connect((err) =>
        {
            if (err)
            {
                // tracer && tracer.error('client', __filename, 'connect', `fail to connect to remote server: ${serverId}`);
                logger.error(`[pomelo-rpc] mailbox fail to connect to remote server: ${serverId}`);
                if (this.mailboxes[serverId])
                {
                    delete this.mailboxes[serverId];
                }
                // this.emit('error', constants.RPC_ERROR.FAIL_CONNECT_SERVER, tracer, serverId, null, this.opts);
                this.emit('error', constants.RPC_ERROR.FAIL_CONNECT_SERVER, cb, serverId, null, this.opts);
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
            this.flushPending(serverId);
        });
    }

    /**
	 * Init and start station. Connect all mailbox to remote servers.
	 * @param  {function} cb(err) callback function
	 */
    start(cb)
    {
        if (this.state > STATE_INITED)
        {
            cb(new Error('station has started.'));
            return;
        }

        process.nextTick(() =>
        {
            this.state = STATE_STARTED;
            cb();
        });
    }

    /**
	 * Stop station and all its mailboxes
	 *
	 * @param  {Boolean} force whether stop station forcely
	 */
    stop(force)
    {
        if (this.state !== STATE_STARTED)
        {
            logger.warn('[pomelo-rpc] client is not running now.');
            return;
        }
        this.state = STATE_CLOSED;
        if (force)
        {
            this.closeAll();
        }
        else
        {
            setTimeout(this.closeAll.bind(this), constants.DEFAULT_PARAM.GRACE_TIMEOUT);
        }
    }

    closeAll()
    {
        const keys = Object.keys(this.mailboxes);
        for (let i = 0, l = keys.length; i < l; i++) {
            const key = keys[i];
            if (this.mailboxes[key])
                this.mailboxes[key].close();
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

        if (this.serversMap[type].indexOf(id) < 0)
        {
            this.serversMap[type].push(id);
        }
        this.emit('addServer', id);
    }

    /**
	 * Batch version for add new server info.
	 *
	 * @param {Array} serverInfoList server info list
	 */
    addServers(serverInfoList)
    {
        if (!serverInfoList || !serverInfoList.length)
        {
            return;
        }

        for (let i = 0, l = serverInfoList.length; i < l; i++)
        {
            this.addServer(serverInfoList[i]);
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

        for (let i = 0, l = ids.length; i < l; i++)
        {
            this.removeServer(ids[i]);
        }
    }

    /**
	 * Clear station information.
	 */
    clearStation()
    {
        this.onlines = {};
        this.serversMap = {};
    }

    /**
	 * Replace remote servers info.
	 *
	 * @param {Array} serverInfoList server info list
	 */
    replaceServers(serverInfoList)
    {
        this.clearStation();
        if (!serverInfoList || !serverInfoList.length)
        {
            return;
        }

        for (let i = 0, l = serverInfoList.length; i < l; i++)
        {
            const id = serverInfoList[i].id;
            const type = serverInfoList[i].serverType;
            this.onlines[id] = 1;
            if (!this.serversMap[type])
            {
                this.serversMap[type] = [];
            }
            this.servers[id] = serverInfoList[i];
            if (this.serversMap[type].indexOf(id) < 0)
            {
                this.serversMap[type].push(id);
            }
        }
    }

    /**
	 * Dispatch rpc message to the mailbox
	 *
	 * @param  {String}   serverId remote server id
	 * @param  {Object}   msg      rpc invoke message
	 * @param  {Object}   opts     rpc invoke option args
	 * @param  {Function} cb       callback function
	 */
    dispatch(serverId, msg, opts, cb)
    {
        const callBack = cb;
        // tracer && tracer.info('client', __filename, 'dispatch', 'dispatch rpc message to the mailbox');
        // tracer && (tracer.cb = cb);
        if (this.state !== STATE_STARTED)
        {
            // tracer && tracer.error('client', __filename, 'dispatch', 'client is not running now');
            logger.error('[pomelo-rpc] client is not running now.');
            // this.emit('error', constants.RPC_ERROR.SERVER_NOT_STARTED, tracer, serverId, msg, opts);
            this.emit('error', constants.RPC_ERROR.SERVER_NOT_STARTED, callBack, serverId, msg, opts);
            return;
        }

        const mailbox = this.mailboxes[serverId];
        if (!mailbox)
        {
            // tracer && tracer.debug('client', __filename, 'dispatch', 'mailbox is not exist');
            // try to connect remote server if mailbox instance not exist yet
            // if (!this.lazyConnect(tracer, serverId, this.mailboxFactory, cb))
            if (!this.lazyConnect(serverId, this.mailboxFactory, cb))
            {
                // tracer && tracer.error('client', __filename, 'dispatch', `fail to find remote server:${serverId}`);
                logger.error(`[pomelo-rpc] fail to find remote server:${serverId}`);
                // this.emit('error', constants.RPC_ERROR.NO_TRAGET_SERVER, tracer, serverId, msg, opts);
                this.emit('error', constants.RPC_ERROR.NO_TRAGET_SERVER, callBack, serverId, msg, opts);
            }
            // push request to the pending queue
            // this.addToPending(tracer, serverId, arguments);
            this.addToPending(serverId, arguments);
            return;
        }

        if (this.connecting[serverId])
        {
            // tracer && tracer.debug('client', __filename, 'dispatch', 'request add to connecting');
            // if the mailbox is connecting to remote server
            // this.addToPending(tracer, serverId, arguments);
            this.addToPending(serverId, arguments);
            return;
        }

        const send = (err, serverId, msg, opts) =>
        {
            // tracer && tracer.info('client', __filename, 'send', 'get corresponding mailbox and try to send message');
            const mailbox = this.mailboxes[serverId];
            if (err)
            {
                // return this.errorHandler(tracer, err, serverId, msg, opts, true, cb);
                return this.errorHandler(err, serverId, msg, opts, true, cb);
            }
            if (!mailbox)
            {
                // tracer && tracer.error('client', __filename, 'send', `can not find mailbox with id:${serverId}`);
                logger.error(`[pomelo-rpc] could not find mailbox with id:${serverId}`);
                // this.emit('error', constants.RPC_ERROR.FAIL_FIND_MAILBOX, tracer, serverId, msg, opts);
                this.emit('error', constants.RPC_ERROR.FAIL_FIND_MAILBOX, callBack, serverId, msg, opts);
                return;
            }
            mailbox.send(msg, opts, (send_err, args) =>
            {
                if (send_err)
                {
                    logger.error('[pomelo-rpc] fail to send message %s', send_err.stack || send_err.message);
                    // this.emit('error', constants.RPC_ERROR.FAIL_SEND_MESSAGE, tracer, serverId, msg, opts);
                    this.emit('error', constants.RPC_ERROR.FAIL_SEND_MESSAGE, callBack, serverId, msg, opts);
                    cb && cb(send_err);
                    // utils.applyCallback(cb, send_err);
                    return;
                }
                // var args = arguments[2];
                // MailStation.DoFilter(tracer, null, serverId, msg, opts, this.afters, 0, 'after', function(tracer, err, serverId, msg, opts)
                MailStation.DoFilter(null, serverId, msg, opts, this.afters, 0, 'after', (err, serverId, msg, opts) =>
                {
                    if (err)
                    {
                        // this.errorHandler(tracer, err, serverId, msg, opts, false, cb);
                        this.errorHandler(err, serverId, msg, opts, false, cb);
                    }
                    if (cb && typeof cb === 'function')
                    {
                        cb(...args);
                    }
                });
            });
        };

        MailStation.DoFilter(null, serverId, msg, opts, this.befores, 0, 'before', send);
    }

    // lazyConnect(tracer, serverId, factory, cb)
    lazyConnect(serverId, factory, cb)
    {
        // tracer && tracer.info('client', __filename, 'lazyConnect', 'create mailbox and try to connect to remote server');
        const server = this.servers[serverId];
        const online = this.onlines[serverId];
        if (!server)
        {
            logger.error('[pomelo-rpc] unknown server: %s', serverId);
            return false;
        }
        if (!online || online !== 1)
        {
            logger.error('[pomelo-rpc] server is not online: %s', serverId);
            return false;
        }
        const mailbox = factory.create(server, this.opts);
        this.connecting[serverId] = true;
        this.mailboxes[serverId] = mailbox;
        this.connect(serverId, cb);
        return true;
    }

    addToPending(serverId, args)
    {
        // tracer && tracer.info('client', __filename, 'addToPending', 'add pending requests to pending queue');
        let pending = this.pendings[serverId];
        if (!pending)
        {
            pending = this.pendings[serverId] = [];
        }
        if (pending.length > this.pendingSize)
        {
            // tracer && tracer.debug('client', __filename, 'addToPending', `mailStation pending too much for: ${serverId}`);
            logger.warn('[pomelo-rpc] mailStation pending too much for: %s', serverId);
            return;
        }
        pending.push(args);
    }

    flushPending(serverId, cb)
    {
        // tracer && tracer.info('client', __filename, 'flushPending', 'flush pending requests to dispatch method');
        const pending = this.pendings[serverId];
        const mailbox = this.mailboxes[serverId];
        if (!pending || !pending.length)
        {
            return;
        }
        if (!mailbox)
        {
            // tracer && tracer.error('client', __filename, 'flushPending', `fail to flush pending messages for empty mailbox: ${serverId}`);
            logger.error(`[pomelo-rpc] fail to flush pending messages for empty mailbox: ${serverId}`);
        }
        for (let i = 0, l = pending.length; i < l; i++)
        {
            this.dispatch(...pending[i]);
        }
        delete this.pendings[serverId];
    }

    errorHandler(err, serverId, msg, opts, flag, cb)
    {
        if (this.handleError)
        {
            this.handleError(err, serverId, msg, opts);
        }
        else
        {
            logger.error('[pomelo-rpc] rpc filter error with serverId: %s, err: %j', serverId, err.stack);
            this.emit('error', constants.RPC_ERROR.FILTER_ERROR, null, serverId, msg, opts);
            // this.emit('error', constants.RPC_ERROR.FILTER_ERROR, tracer, serverId, msg, opts);
        }
    }

    /**
	 * Do before or after filter
	 */
    static DoFilter(err, serverId, msg, opts, filters, index, operate, cb)
    {
        if (index < filters.length)
        {
            // tracer && tracer.info('client', __filename, 'doFilter', `do ${operate} filter ${filters[index].name}`);
        }
        if (index >= filters.length || Boolean(err))
        {
            cb(err, serverId, msg, opts);
            return;
        }
        const filter = filters[index];
        if (typeof filter === 'function')
        {
            filter(serverId, msg, opts, function(target, message, options)
            {
                index++;
                // compatible for pomelo filter next(err) method
                if (target instanceof Error)
                {
                    MailStation.DoFilter(target, serverId, msg, opts, filters, index, operate, cb);
                }
                else
                {
                    MailStation.DoFilter(null, target || serverId, message || msg, options || opts, filters, index, operate, cb);
                }
            });
            return;
        }
        if (typeof filter[operate] === 'function')
        {
            filter[operate](serverId, msg, opts, function(target, message, options)
            {
                index++;
                if (target instanceof Error)
                {
                    MailStation.DoFilter(target, serverId, msg, opts, filters, index, operate, cb);
                }
                else
                {
                    MailStation.DoFilter(null, target || serverId, message || msg, options || opts, filters, index, operate, cb);
                }
            });
            return;
        }
        index++;
        MailStation.DoFilter(err, serverId, msg, opts, filters, index, operate, cb);
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
module.exports.create = function(opts)
{
    return new MailStation(opts || {});
};
