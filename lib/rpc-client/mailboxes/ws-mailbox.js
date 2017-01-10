const utils = require('../../util/Utils');
const client = require('socket.io-client');
const Tracer = require('../../util/tracer');
const constants = require('../../util/constants');
const EventEmitter = require('events').EventEmitter;
const logger = require('pomelo-logger').getLogger('pomelo-rpc', __filename);

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
		this.interval = opts.interval || constants.DEFAULT_PARAM.INTERVAL;
		this.timeoutValue = opts.timeout || constants.DEFAULT_PARAM.CALLBACK_TIMEOUT;
		this.connected = false;
		this.opts = opts;
	}

	connect(tracer, cb)
	{
		tracer.info('client', __filename, 'connect', 'ws-mailbox try to connect');
		if (this.connected)
		{
			tracer.error('client', __filename, 'connect', 'mailbox has already connected');
			utils.InvokeCallback(cb, new Error('mailbox has already connected.'));
			return;
		}
		let host = this.host;
		if (host.indexOf('http://') === -1)
		{
			host = `http://${host}`;
		}
		this.socket = client.connect(`${host}:${this.port}`, {
			'forceNew'  : true,
			'reconnect' : false});
		this.socket.on('message', (pkg) =>
		{
			try
			{
				if (pkg instanceof Array)
				{
					MailboxUtility.ProcessMsgs(this, pkg);
				}
				else
				{
					MailboxUtility.ProcessMsg(this, pkg);
				}
			}
			catch (e)
			{
				console.error('rpc client process message with error: %j', e.stack);
			}
		});

		this.socket.on('connect', () =>
		{
			if (this.connected)
			{
				return;
			}
			this.connected = true;
			if (this.bufferMsg)
			{
				this._interval = setInterval(() =>
				{
					MailboxUtility.Flush(this);
				}, this.interval);
			}
			utils.InvokeCallback(cb);
		});

		this.socket.on('error', (err) =>
		{
			logger.error(`rpc socket is error, remote server host: ${this.host}, port: ${this.port}`);
			this.emit('close', this.id);
			utils.InvokeCallback(cb, err);
		});

		this.socket.on('disconnect', (reason) =>
		{
			logger.error('rpc socket is disconnect, reason: %s', reason);
			const reqs = this.requests;
			let cb;
			for (const id in reqs)
			{
				cb = reqs[id];
				utils.InvokeCallback(cb, tracer, new Error('disconnect with remote server.'));
			}
			this.emit('close', this.id);
		});

		this.socket.on('connect_timeout', (err) =>
		{
			logger.error(`rpc socket is connect timeout, host: ${this.host}, port: ${this.port}`);
			this.emit('close', this.id);
			utils.InvokeCallback(cb, err);
		});

		this.socket.on('connect_error', (err) =>
		{
			logger.error(`rpc socket is error, remote server host: ${this.host}, port: ${this.port}`);
			this.emit('close', this.id);
			utils.InvokeCallback(cb, err);
		});
	}

	/**
	 * close mailbox
	 */
	close()
	{
		if (this._interval)
		{
			clearInterval(this._interval);
			this._interval = null;
		}
		this.socket.disconnect();
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
		tracer.info('client', __filename, 'send', 'ws-mailbox try to send');
		if (!this.connected)
		{
			tracer.error('client', __filename, 'send', 'ws-mailbox not init');
			utils.InvokeCallback(cb, tracer, new Error('ws-mailbox is not init'));
			return;
		}

		const id = this.curId++;
		this.requests[id] = cb;
		MailboxUtility.SetCbTimeout(this, id, tracer, cb);

		let pkg;
		if (tracer.isEnabled)
		{
			pkg = {
				traceId : tracer.id,
				seqId   : tracer.seq,
				source  : tracer.source,
				remote  : tracer.remote,
				id      : id,
				msg     : msg};
		}
		else
		{
			pkg = {
				id  : id,
				msg : msg};
		}
		if (this.bufferMsg)
		{
			MailboxUtility.EnQueue(this, pkg);
		}
		else
		{
			this.socket.emit('message', pkg);
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
		if (!mailbox || !mailbox.queue.length)
		{
			return;
		}
		mailbox.socket.emit('message', mailbox.queue);
		mailbox.queue = [];
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
		MailboxUtility.ClearCbTimeout(mailbox, pkg.id);
		const cb = mailbox.requests[pkg.id];
		if (!cb)
		{
			return;
		}
		delete mailbox.requests[pkg.id];

		const tracer = new Tracer(mailbox.opts.rpcLogger, mailbox.opts.rpcDebugLog, mailbox.opts.clientId, pkg.source, pkg.resp, pkg.traceId, pkg.seqId);
		const args = [tracer, null];

		pkg.resp.forEach((arg) =>
		{
			args.push(arg);
		});

		cb(...args);
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
			logger.error('rpc callback timeout, remote server host: %s, port: %s', mailbox.host, mailbox.port);
			utils.InvokeCallback(cb, tracer, new Error('rpc callback timeout'));
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