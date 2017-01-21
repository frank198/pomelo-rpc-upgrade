const logger = require('pomelo-logger').getLogger('pomelo-rpc', 'mqtt2-acceptor'),
	EventEmitter = require('events').EventEmitter,
	Constant = require('../../util/constants'),
	Tracer = require('../../util/tracer'),
	MqttCon = require('mqtt-connection'),
	utils = require('../../util/Utils'),
	Coder = require('../../util/coder'),
	net = require('net');

let curId = 1;

class Acceptor extends EventEmitter
{
	constructor(opts, cb)
	{
		super();
		this.interval = opts.interval; // flush interval in ms
		this.bufferMsg = opts.bufferMsg;
		this.rpcLogger = opts.rpcLogger;
		this.rpcDebugLog = opts.rpcDebugLog;
		this.services = opts.services;
		this._interval = null; // interval object
		this.sockets = {};
		this.msgQueues = {};
		this.servicesMap = {};
		this.cb = cb;
	}

	listen(port)
	{
		// check status
		if (this.inited)
		{
			this.cb(new Error('already inited.'));
			return;
		}
		this.inited = true;

		this.server = new net.Server();
		this.server.listen(port);

		this.server.on('error', err =>
		{
			logger.error('rpc server is error: %j', err.stack);
			this.emit('error', err);
		});

		this.server.on('connection', stream =>
		{
			const socket = MqttCon(stream);
			socket['id'] = curId++;

			socket.on('connect', pkg =>
			{
				console.log('connected');
				AcceptorUtility.SendHandshake(socket, this);
			});

			socket.on('publish', pkg =>
			{
				pkg = Coder.decodeServer(pkg.payload, this.servicesMap);
				try
				{
					AcceptorUtility.ProcessMsg(socket, this, pkg);
				}
				catch (err)
				{
					const resp = Coder.encodeServer(pkg.id, [AcceptorUtility.CloneError(err)]);
					// doSend(socket, resp);
					logger.error('process rpc message error %s', err.stack);
				}
			});

			socket.on('pingreq', () =>
			{
				socket.pingresp();
			});

			socket.on('error', () =>
			{
				this.onSocketClose(socket);
			});

			socket.on('close', () =>
			{
				this.onSocketClose(socket);
			});

			this.sockets[socket.id] = socket;

			socket.on('disconnect', reason =>
			{
				this.onSocketClose(socket);
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
		this.server.close();
		this.emit('closed');
	}

	onSocketClose(socket)
	{
		if (!socket['closed'])
		{
			const id = socket.id;
			socket['closed'] = true;
			delete this.sockets[id];
			delete this.msgQueues[id];
		}
	}
}

class AcceptorUtility
{
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
			tracer.info('server', __filename, 'processMsg', 'mqtt-acceptor receive message and try to process message');
		}
		acceptor.cb(tracer, pkg.msg, (...args) =>
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
				resp = Coder.encodeServer(pkg.id, args);
			}
			if (acceptor.bufferMsg)
			{
				AcceptorUtility.EnQueue(socket, acceptor, resp);
			}
			else
			{
				AcceptorUtility.DoSend(socket, resp);
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
		const sockets = acceptor.sockets,
			queues = acceptor.msgQueues;
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
			AcceptorUtility.DoSend(socket, queue);
			queues[socketId] = [];
		}
	}

	static DoSend(socket, msg)
	{
		socket.publish({
			topic   : Constant['TOPIC_RPC'],
			payload : msg
			// payload: JSON.stringify(msg)
		});
	}

	static DoSendHandshake(socket, msg)
	{
		socket.publish({
			topic   : Constant['TOPIC_HANDSHAKE'],
			payload : msg
			// payload: JSON.stringify(msg)
		});
	}

	static SendHandshake(socket, acceptor)
	{
		const servicesMap = utils.GenServicesMap(acceptor.services);
		acceptor.servicesMap = servicesMap;
		AcceptorUtility.DoSendHandshake(socket, JSON.stringify(servicesMap));
	}
}

/**
 * create acceptor
 *
 * @param opts init params
 * @param cb(tracer, msg, cb) callback function that would be invoked when new message arrives
 */
module.exports.create = function(opts, cb)
{
	return new Acceptor(opts || {}, cb);
};