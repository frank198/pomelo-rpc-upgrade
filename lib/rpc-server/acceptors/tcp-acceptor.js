const EventEmitter = require('events').EventEmitter;
const utils = require('../../util/Utils');
const net = require('net');
const Composer = require('../../util/Composer');
const Tracer = require('../../util/tracer');

class Acceptor extends EventEmitter
{
	constructor(opts, cb)
    {
		super();
		opts = opts || {};
		this.bufferMsg = opts.bufferMsg;
		this.interval = opts.interval;  // flush interval in ms
	    this.rpcDebugLog = opts.rpcDebugLog;
		this.pkgSize = opts.pkgSize;
		this._interval = null;          // interval object
		this.server = null;
		this.sockets = {};
		this.msgQueues = {};
		this.cb = cb;
	}

	listen(port)
    {
        // check status
		if (this.inited)
		{
			utils.InvokeCallback(this.cb, new Error('already inited.'));
			return;
		}
		this.inited = true;

		this.server = net.createServer();
		this.server.listen(port);

		this.server.on('error', (err) =>
        {
			this.emit('error', err, this);
		});

		this.server.on('connection', (socket) =>
        {
			this.sockets[socket.id] = socket;
			socket.composer = new Composer({maxLength: this.pkgSize});

			socket.on('data', (data) =>
            {
				socket.composer.feed(data);
			});

			socket.composer.on('data', (data) =>
            {
				const pkg = JSON.parse(data.toString());
				if (pkg instanceof Array)
                {
					AcceptorUtility.ProcessMsgs(socket, this, pkg);
				}
				else
                {
					AcceptorUtility.ProcessMsg(socket, this, pkg);
				}
			});

			socket.on('close', () =>
            {
				delete this.sockets[socket.id];
				delete this.msgQueues[socket.id];
			});
		});

		if (this.bufferMsg)
		{
			this._interval = setInterval(() =>
            {
				AcceptorUtility.Flush(this);
			}, this.interval);
		}
	}

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
		try
        {
			this.server.close();
		}
		catch (err)
        {
			console.error('rpc server close error: %j', err.stack);
		}
		this.emit('closed');
	}
}

class AcceptorUtility
{
	static Create(opts, cb)
    {
		return new Acceptor(opts || {}, cb);
	}

	static CloneError(origin)
    {
		return {
			msg   : origin.msg,
			stack : origin.stack
		};
	}

	static ProcessMsgs(socket, acceptor, pkgs)
    {
		for (let i = 0, l = pkgs.length; i < l; i++)
		{
			AcceptorUtility.ProcessMsg(socket, acceptor, pkgs[i]);
		}
	}

	static ProcessMsg(socket, acceptor, pkg)
    {
		let tracer = null;
		if (acceptor.rpcDebugLog)
	    {
		    tracer = new Tracer(acceptor.rpcLogger, acceptor.rpcDebugLog, pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId);
		    tracer.info('server', __filename, 'processMsg', 'tcp-acceptor receive message and try to process message');
	    }

		acceptor.cb.call(null, tracer, pkg.msg, (...args) =>
        {
	        const errorArg = args[0];
	        if (errorArg && errorArg instanceof Error)
	        {
		        args[0] = AcceptorUtility.CloneError(errorArg);
	        }
			let resp;
			if (tracer && tracer.isEnabled)
			{
				resp = {
					traceId : tracer.id,
					seqId   : tracer.seq,
					source  : tracer.source,
					id      : pkg.id,
					resp    : Array.from(args)};
			}
			else
            {
				resp = {
					id   : pkg.id,
					resp : Array.from(args)};
			}
			if (acceptor.bufferMsg)
			{
				AcceptorUtility.EnQueue(socket, acceptor, resp);
			}
			else
            {
				socket.write(socket.composer.compose(JSON.stringify(resp)));
			}
		});
	}

	static EnQueue(socket, acceptor, msg)
    {
		let queue = acceptor.msgQueues[socket.id];
		if (!queue)
        {
			queue = acceptor.msgQueues[socket.id] = [];
		}
		queue.push(msg);
	}

	static Flush(acceptor)
    {
		const sockets = acceptor.sockets, queues = acceptor.msgQueues;
		let queue, socket;
		for (const socketId in queues)
        {
			socket = sockets[socketId];
			if (!socket)
            {
                // clear pending messages if the socket not exist any more
				delete queues[socketId];
				continue;
			}
			queue = queues[socketId];
			if (!queue.length)
            {
				continue;
			}
			socket.write(socket.composer.compose(JSON.stringify(queue)));
			queues[socketId] = [];
		}
	}
}

/**
 * create acceptor
 *
 * @param opts init params
 * @param cb(tracer, msg, cb) callback function that would be invoked when new message arrives
 */
module.exports.create = AcceptorUtility.Create;

process.on('SIGINT', function()
{
	process.exit();
});
