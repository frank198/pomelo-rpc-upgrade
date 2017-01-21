const logger = require('pomelo-logger').getLogger('pomelo-rpc', 'mqtt-mailbox'),
	EventEmitter = require('events').EventEmitter,
	constants = require('../../util/constants'),
	Tracer = require('../../util/tracer'),
	MqttCon = require('mqtt-connection'),
	utils = require('../../util/Utils'),
	util = require('util'),
	net = require('net');

const CONNECT_TIMEOUT = 2000;

class MailBox extends EventEmitter
{
	constructor(server, opts)
	{
		super();
		this.curId = 0;
		this.id = server.id;
		this.host = server.host;
		this.port = server.port;
		this.requests = {};
		this.timeout = {};
		this.queue = [];
		this.bufferMsg = opts.bufferMsg;
		this.keepalive = opts.keepalive || constants.DEFAULT_PARAM.KEEPALIVE;
		this.interval = opts.interval || constants.DEFAULT_PARAM.INTERVAL;
		this.timeoutValue = opts.timeout || constants.DEFAULT_PARAM.CALLBACK_TIMEOUT;
		this.keepaliveTimer = null;
		this.lastPing = -1;
		this.lastPong = -1;
		this.connected = false;
		this.closed = false;
		this.opts = opts;
		this.serverId = opts.context.serverId;
	}

	connect(tracer, cb)
	{
		tracer && tracer.info('client', __filename, 'connect', 'mqtt-mailbox try to connect');
		if (this.connected)
		{
			tracer && tracer.error('client', __filename, 'connect', 'mailbox has already connected');
			return cb(new Error('mailbox has already connected.'));
		}

		const stream = net.connect(this.port, this.host);
		this.socket = MqttCon(stream);

		const connectTimeout = setTimeout(() =>
		{
			logger.error('rpc client %s connect to remote server %s timeout', this.serverId, this.id);
			this.emit('close', this.id);
		}, CONNECT_TIMEOUT);

		this.socket.connect({
			clientId : `MQTT_RPC_${Date.now()}`
		}, () =>
		{
			if (this.connected)
			{
				return;
			}

			clearTimeout(connectTimeout);
			this.connected = true;
			if (this.bufferMsg)
			{
				this._interval = setInterval(() =>
				{
					MailboxUtility.Flush(this);
				}, this.interval);
			}

			this.setupKeepAlive();
			cb();
		});

		this.socket.on('publish', pkg =>
		{
			pkg = pkg.payload.toString();
			try
			{
				pkg = JSON.parse(pkg);
				if (pkg instanceof Array)
				{
					MailboxUtility.ProcessMsgs(this, pkg);
				}
				else
				{
					MailboxUtility.ProcessMsg(this, pkg);
				}
			}
			catch (err)
			{
				logger.error('rpc client %s process remote server %s message with error: %s', this.serverId, this.id, err.stack);
			}
		});

		this.socket.on('error', err =>
		{
			logger.error('rpc socket %s is error, remote server %s host: %s, port: %s, error: %s', this.serverId, this.id, this.host, this.port, err.stack);
			this.emit('close', this.id);
		});

		this.socket.on('pingresp', () =>
		{
			this.lastPong = Date.now();
		});

		this.socket.on('disconnect', reason =>
		{
			logger.error('rpc socket %s is disconnect from remote server %s, reason: %s', this.serverId, this.id, reason);
			const reqs = this.requests;
			for (const id in reqs)
			{
				const ReqCb = reqs[id];
				ReqCb(tracer, new Error(`${this.serverId} disconnect with remote server ${this.id}`));
			}
			this.emit('close', this.id);
		});
	}

	/**
	 * close mailbox
	 */
	close()
	{
		if (this.closed)
		{
			return;
		}
		this.closed = true;
		this.connected = false;
		if (this._interval)
		{
			clearInterval(this._interval);
			this._interval = null;
		}
		this.socket.destroy();
	}

	/**
	 * send message to remote server
	 *
	 * @param msg {service:"", method:"", args:[]}
	 * @param opts {} attach info to send method
	 * @param cb declaration decided by remote interface
	 */
	send(tracer, msg, opts, cb)
	{
		tracer && tracer.info('client', __filename, 'send', 'mqtt-mailbox try to send');
		if (!this.connected)
		{
			tracer && tracer.error('client', __filename, 'send', 'mqtt-mailbox not init');
			cb(tracer, new Error(`${this.serverId} mqtt-mailbox is not init ${this.id}`));
			return;
		}

		if (this.closed)
		{
			tracer && tracer.error('client', __filename, 'send', 'mailbox has already closed');
			cb(tracer, new Error(`${this.serverId} mqtt-mailbox has already closed ${this.id}`));
			return;
		}

		const id = this.curId++;
		this.requests[id] = cb;
		MailboxUtility.SetCbTimeout(this, id, tracer, cb);

		let pkg;
		if (tracer && tracer.isEnabled)
		{
			pkg = {
				traceId : tracer.id,
				seqId   : tracer.seq,
				source  : tracer.source,
				remote  : tracer.remote,
				id      : id,
				msg     : msg
			};
		}
		else
		{
			pkg = {
				id  : id,
				msg : msg
			};
		}
		if (this.bufferMsg)
		{
			MailboxUtility.EnQueue(this, pkg);
		}
		else
		{
			MailboxUtility.DoSend(this.socket, pkg);
		}
	}

	setupKeepAlive()
	{
		this.keepaliveTimer = setInterval(() =>
		{
			this.checkKeepAlive();
		}, this.keepalive);
	}

	checkKeepAlive()
	{
		if (this.closed)
		{
			return;
		}

		// console.log('checkKeepAlive lastPing %d lastPong %d ~~~', this.lastPing, this.lastPong);
		const now = Date.now();
		const KEEP_ALIVE_TIMEOUT = this.keepalive * 2;
		if (this.lastPing > 0)
		{
			if (this.lastPong < this.lastPing)
			{
				if (now - this.lastPing > KEEP_ALIVE_TIMEOUT)
				{
					logger.error('mqtt rpc client %s checkKeepAlive timeout from remote server %s for %d lastPing: %s lastPong: %s', this.serverId, this.id, KEEP_ALIVE_TIMEOUT, this.lastPing, this.lastPong);
					this.emit('close', this.id);
					this.lastPing = -1;
					// this.close();
				}
			}
			else
			{
				this.socket.pingreq();
				this.lastPing = Date.now();
			}
		}
		else
		{
			this.socket.pingreq();
			this.lastPing = Date.now();
		}
	}
}

class MailboxUtility
{
	static EnQueue(mailbox, msg)
	{
		mailbox.queue.push(msg);
	}

	static Flush(mailbox)
	{
		if (!mailbox || mailbox.closed || !mailbox.queue.length)
		{
			return;
		}
		MailboxUtility.DoSend(mailbox.socket, mailbox.queue);
		mailbox.socket.emit('message', mailbox.queue);
		mailbox.queue = [];
	}

	static DoSend(socket, msg)
	{
		socket.publish({
			topic   : 'rpc',
			payload : JSON.stringify(msg)
		});
	}

	static ProcessMsgs(mailbox, pkgs)
	{
		for (let i = 0, l = pkgs.length; i < l; i++)
		{
			MailboxUtility.ProcessMsg(mailbox, pkgs[i]);
		}
	}

	static ProcessMsg(mailbox, pkg)
	{
		const pkgId = pkg.id;
		MailboxUtility.ClearCbTimeout(mailbox, pkgId);
		const cb = mailbox.requests[pkgId];
		if (!cb)
		{
			return;
		}
		delete mailbox.requests[pkgId];

		const rpcDebugLog = mailbox.opts.rpcDebugLog;
		let tracer = null;
		const sendErr = null;
		if (rpcDebugLog)
		{
			tracer = new Tracer(mailbox.opts.rpcLogger, mailbox.opts.rpcDebugLog, mailbox.opts.clientId, pkg.source, pkg.resp, pkg.traceId, pkg.seqId);
		}
		const pkgResp = pkg.resp;

		cb(tracer, sendErr, pkgResp);
	}

	static SetCbTimeout(mailbox, id, tracer, cb)
	{
		const timer = setTimeout(() =>
		{
			logger.warn('rpc request is timeout, id: %s, host: %s, port: %s', id, mailbox.host, mailbox.port);
			MailboxUtility.ClearCbTimeout(mailbox, id);
			if (mailbox.requests[id])
			{
				delete mailbox.requests[id];
			}
			const eMsg = util.format('rpc %s callback timeout %d, remote server %s host: %s, port: %s', mailbox.serverId, mailbox.timeoutValue, id, mailbox.host, mailbox.port);
			logger.error(eMsg);
			cb(tracer, new Error(eMsg));
		}, mailbox.timeoutValue);
		mailbox.timeout[id] = timer;
	}

	static ClearCbTimeout(mailbox, id)
	{
		if (!mailbox.timeout[id])
		{
			logger.warn('timer is not exsits, id: %s, host: %s, port: %s', id, mailbox.host, mailbox.port);
			return;
		}
		clearTimeout(mailbox.timeout[id]);
		delete mailbox.timeout[id];
	}
}

/**
 * Factory method to create mailbox
 *
 * @param {Object} server remote server info {id:"", host:"", port:""}
 * @param {Object} opts construct parameters
 *                      opts.bufferMsg {Boolean} msg should be buffered or send immediately.
 *                      opts.interval {Boolean} msg queue flush interval if bufferMsg is true. default is 50 ms
 */
module.exports.create = function(server, opts)
{
	return new MailBox(server, opts || {});
};