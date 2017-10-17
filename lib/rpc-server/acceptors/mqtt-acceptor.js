const logger = require('pomelo-logger').getLogger('pomelo-rpc', 'mqtt-acceptor');
const EventEmitter = require('events').EventEmitter;
const Tracer = require('../../util/tracer');
const MqttCon = require('mqtt-connection');
const net = require('net');

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
		this._interval = null; // interval object
		this.sockets = {};
		this.msgQueues = {};
		this.cb = cb;
	}
}

const pro = Acceptor.prototype;

pro.listen = function(port)
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
		this.sockets[socket.id] = socket;

		socket.on('connect', (pkg) =>
		{
	        logger.info('connected');
		});

		socket.on('publish', (pkg) =>
		{
			pkg = pkg.payload.toString();
			let isArray = false;
			try
			{
				pkg = JSON.parse(pkg);
				if (pkg instanceof Array)
				{
					processMsgs(socket, this, pkg);
					isArray = true;
				}
				else
				{
					processMsg(socket, this, pkg);
				}
			}
			catch (err)
			{
				if (!isArray)
				{
					doSend(socket, {
						id   : pkg.id,
						resp : [cloneError(err)]
					});
				}
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

		socket.on('disconnect', (reason) =>
		{
			this.onSocketClose(socket);
		});
	});

	if (this.bufferMsg)
	{
		this._interval = setInterval(() =>
		{
			flush(this);
		}, this.interval);
	}
};

pro.close = function()
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
};

pro.onSocketClose = function(socket)
{
	if (!socket['closed'])
	{
		const id = socket.id;
		socket['closed'] = true;
		delete this.sockets[id];
		delete this.msgQueues[id];
	}
};

const cloneError = function(origin)
{
	// copy the stack infos for Error instance json result is empty
	return {
		msg   : origin.msg,
		stack : origin.stack
	};
};

const processMsg = function(socket, acceptor, pkg)
{
	let tracer = null;
	if (this.rpcDebugLog)
	{
		tracer = new Tracer(acceptor.rpcLogger, acceptor.rpcDebugLog, pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId);
		tracer.info('server', __filename, 'processMsg', 'mqtt-acceptor receive message and try to process message');
	}
	acceptor.cb(tracer, pkg.msg, function()
	{
		// var args = Array.prototype.slice.call(arguments, 0);
		const len = arguments.length;
		const args = new Array(len);
		for (let i = 0; i < len; i++)
		{
			args[i] = arguments[i];
		}

		const errorArg = args[0]; // first callback argument can be error object, the others are message
		if (errorArg && errorArg instanceof Error)
		{
			args[0] = cloneError(errorArg);
		}

		let resp;
		if (tracer && tracer.isEnabled)
		{
			resp = {
				traceId : tracer.id,
				seqId   : tracer.seq,
				source  : tracer.source,
				id      : pkg.id,
				resp    : args
			};
		}
		else
		{
			resp = {
				id   : pkg.id,
				resp : args
			};
		}
		if (acceptor.bufferMsg)
		{
			enqueue(socket, acceptor, resp);
		}
		else
		{
			doSend(socket, resp);
		}
	});
};

const processMsgs = function(socket, acceptor, pkgs)
{
	for (let i = 0, l = pkgs.length; i < l; i++)
	{
		processMsg(socket, acceptor, pkgs[i]);
	}
};

const enqueue = function(socket, acceptor, msg)
{
	const id = socket.id;
	let queue = acceptor.msgQueues[id];
	if (!queue)
	{
		queue = acceptor.msgQueues[id] = [];
	}
	queue.push(msg);
};

const flush = function(acceptor)
{
	const sockets = acceptor.sockets,
		queues = acceptor.msgQueues;
	let	queue, socket, i = 0;
	const queueArr = Object.keys(queues);
	for (i = queueArr.length - 1; i >= 0; i--)
	{
		const socketId = queueArr[i];
		if (!socketId) continue;
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
		doSend(socket, queue);
		queues[socketId] = [];
	}
};

const doSend = function(socket, msg)
{
	socket.publish({
		topic   : 'rpc',
		payload : JSON.stringify(msg)
	});
};

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