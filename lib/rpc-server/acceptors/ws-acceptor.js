const EventEmitter = require('events').EventEmitter;
const utils = require('../../util/Utils');
const sio = require('socket.io');
const logger = require('pomelo-logger').getLogger('pomelo-rpc', __filename);
const Tracer = require('../../util/tracer');

class Acceptor extends EventEmitter
{
	constructor(opts, cb)
    {
		super();
		this.bufferMsg = opts.bufferMsg;
		this.interval = opts.interval;  // flush interval in ms
		this.rpcDebugLog = opts.rpcDebugLog;
		this.rpcLogger = opts.rpcLogger;
		this.whitelist = opts.whitelist;
		this._interval = null;          // interval object
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

		this.server = sio.listen(port);

		this.server.set('log level', 0);

		this.server.server.on('error', (err) =>
        {
			logger.error('rpc server is error: %j', err.stack);
			this.emit('error', err);
		});

		this.server.sockets.on('connection', (socket) =>
        {
			this.sockets[socket.id] = socket;

			this.emit('connection', {
				id : socket.id,
				ip : socket.handshake.address.address});

			socket.on('message', (pkg) =>
            {
				try
                {
					if (pkg instanceof Array)
                    {
						AcceptorUtility.ProcessMsgs(socket, this, pkg);
					}
					else
                    {
						AcceptorUtility.ProcessMsg(socket, this, pkg);
					}
				}
				catch (e)
                {
                    // socke.io would broken if uncaugth the exception
					logger.error('rpc server process message error: %j', e.stack);
				}
			});

			socket.on('disconnect', (reason) =>
            {
				delete this.sockets[socket.id];
				delete this.msgQueues[socket.id];
			});
		});

		this.on('connection', (obj) => {AcceptorUtility.IPFilter(this, obj);});

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
		try {this.server.server.close();}
		catch (err) {logger.error('rpc server close error: %j', err.stack);}
		this.emit('closed');
	}
}

class AcceptorUtility
{
	static Create(opts, cb)
    {
		return new Acceptor(opts || {}, cb);
	}

	static IPFilter(acceptor, obj)
    {
		if (typeof acceptor.whitelist === 'function')
        {
			acceptor.whitelist((err, tmpList) =>
            {
				if (err)
                {
					logger.error('%j.(RPC whitelist).', err);
					return;
				}
				if (!Array.isArray(tmpList))
                {
					logger.error('%j is not an array.(RPC whitelist).', tmpList);
					return;
				}
				if (Boolean(obj) && Boolean(obj.ip) && Boolean(obj.id))
                {
					for (const i in tmpList)
                    {
						const exp = new RegExp(tmpList[i]);
						if (exp.test(obj.ip))
                        {
							return;
						}
					}
					const sock = this.sockets[obj.id];
					if (sock)
                    {
						sock.disconnect('unauthorized');
						logger.warn('%s is rejected(RPC whitelist).', obj.ip);
					}
				}
			});
		}
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
		const tracer = new Tracer(acceptor.rpcLogger, acceptor.rpcDebugLog, pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId);
		tracer.info('server', __filename, 'processMsg', 'ws-acceptor receive message and try to process message');
		acceptor.cb.call(null, tracer, pkg.msg, function()
        {
			const args = Array.from(arguments);
			const l = args.length;
			let i = 0;
			for (i = 0; i < l; i++)
            {
				if (args[i] instanceof Error)
                {
					args[i] = AcceptorUtility.CloneError(args[i]);
				}
			}
			let resp;
			if (tracer.isEnabled)
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
				socket.emit('message', resp);
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
			socket.emit('message', queue);
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
