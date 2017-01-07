const EventEmitter = require('events').EventEmitter;
const utils = require('../../util/Utils');
const Composer = require('../../util/Composer');
const net = require('net');
const Tracer = require('../../util/tracer');
const DEFAULT_CALLBACK_TIMEOUT = 10 * 1000;
const DEFAULT_INTERVAL = 50;

class MailBox extends EventEmitter
{
	constructor(server, opts)
    {
		super();
		this.opts = opts || {};
		this.id = server.id;
		this.host = server.host;
		this.port = server.port;
		this.socket = null;
		this.composer = new Composer({maxLength: opts.pkgSize});
		this.requests = {};
		this.timeout = {};
		this.curId = 0;
		this.queue = [];
		this.bufferMsg = opts.bufferMsg;
		this.interval = opts.interval || DEFAULT_INTERVAL;
		this.timeoutValue = opts.timeout || DEFAULT_CALLBACK_TIMEOUT;
		this.connected = false;
		this.closed = false;
	}

	connect(tracer, cb)
    {
		tracer.info('client', __filename, 'connect', 'tcp-mailbox try to connect');
		if (this.connected)
        {
			utils.InvokeCallback(cb, new Error('mailbox has already connected.'));
			return;
		}

		this.socket = net.connect({
			port : this.port,
			host : this.host}, (err) =>
        {
            // success to connect
			this.connected = true;
			if (this.bufferMsg)
			{
                // start flush interval
				this._interval = setInterval(() =>
                {
					MailboxUtility.Flush(this);
				}, this.interval);
			}
			utils.InvokeCallback(cb, err);
		});

		this.composer.on('data', (data) =>
        {
			const pkg = JSON.parse(data.toString());
			if (pkg instanceof Array)
			{
				MailboxUtility.ProcessMsgs(this, pkg);
			}
			else
            {
				MailboxUtility.ProcessMsg(this, pkg);
			}
		});

		this.socket.on('data', (data) =>
        {
			this.composer.feed(data);
		});

		this.socket.on('error', (err) =>
        {
			if (!this.connected)
            {
				utils.InvokeCallback(cb, err);
				return;
			}
			this.emit('error', err, this);
		});

		this.socket.on('end', () =>
        {
			this.emit('close', this.id);
		});

        // TODO: reconnect and heartbeat
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
		if (this._interval)
        {
			clearInterval(this._interval);
			this._interval = null;
		}
		if (this.socket)
        {
			this.socket.end();
			this.socket = null;
		}
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
		tracer.info('client', __filename, 'send', 'tcp-mailbox try to send');
		if (!this.connected)
        {
			utils.InvokeCallback(cb, tracer, new Error('not init.'));
			return;
		}

		if (this.closed)
        {
			utils.InvokeCallback(cb, tracer, new Error('mailbox alread closed.'));
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
			this.socket.write(this.composer.compose(JSON.stringify(pkg)));
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
		if (mailbox.closed || !mailbox.queue.length)
        {
			return;
		}
		mailbox.socket.write(mailbox.composer.compose(JSON.stringify(mailbox.queue)));
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
			MailboxUtility.ClearCbTimeout(mailbox, id);
			if (mailbox.requests[id])
            {
				delete mailbox.requests[id];
			}
			tracer.error('rpc callback timeout, remote server host: %s, port: %s', mailbox.host, mailbox.port);
			utils.InvokeCallback(cb, tracer, new Error('rpc callback timeout'));
		}, mailbox.timeoutValue);
		mailbox.timeout[id] = timer;
	}

	static ClearCbTimeout(mailbox, id)
    {
		if (!mailbox.timeout[id])
        {
			console.warn('timer not exists, id: %s', id);
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
