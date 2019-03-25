'use strict';
const logger = require('pomelo-logger-upgrade').getLogger('pomelo_rpc', 'mqtt-acceptor');
const EventEmitter = require('events').EventEmitter;
const MqttCon = require('mqtt-connection');
const net = require('net');

let curId = 1;

class RpcServer extends EventEmitter
{
    constructor(opts, callBack)
    {
        super();
        this.interval = opts.interval; // flush interval in ms
        this.bufferMsg = opts.bufferMsg;
        this.logger = opts.rpcLogger || logger || console;
        this.rpcDebugLog = opts.rpcDebugLog;
        this._interval = null; // interval object
        this.sockets = {};
        this.msgQueues = {};
        this.callBack = callBack;
    }

    listen(port)
    {
        // check status
        if (this.inited)
        {
            this.callBack(new Error('already inited.'));
            return;
        }
        this.inited = true;

        this.server = new net.Server();
        this.server.listen(port);

        this.server.on('error', err =>
        {
            this.logger.error('rpc server is error: %j', err.stack);
            this.emit('error', err);
        });

        this.server.on('connection', stream =>
        {
            const socket = MqttCon(stream);
            socket['id'] = curId++;
            this.sockets[socket.id] = socket;

            socket.on('connect', () =>
            {
                this.logger.info('connected');
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
                        isArray = true;
                        for (let i = 0, l = pkg.length; i < l; i++)
                        {
                            this.processMsg(socket, pkg[i]);
                        }
                    }
                    else
                    {
                        this.processMsg(socket, pkg);
                    }
                }
                catch (err)
                {
                    if (!isArray)
                    {
                        RpcServer.DoSend(socket, {
                            id   : pkg.id,
                            resp : [RpcServer.CloneError(err)]
                        });
                    }
                    this.logger.error('process rpc message error %s', err.stack);
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

            socket.on('disconnect', () =>
            {
                this.onSocketClose(socket);
            });
        });

        if (this.bufferMsg)
        {
            this._interval = setInterval(this.flush.bind(this), this.interval);
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

    processMsg(socket, pkg)
    {
        if (this.rpcDebugLog)
        {
            this.logger.info('server', pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId, __filename, 'processMsg', 'mqtt-acceptor receive message and try to process message');
        }
        this.callBack(pkg.msg, (...args) =>
        {
            const errorArg = args[0]; // first callback argument can be error object, the others are message
            if (errorArg && errorArg instanceof Error)
            {
                args[0] = RpcServer.CloneError(errorArg);
            }
            const sendMessage = {
                id   : pkg.id,
                resp : args
            };
            if (this.bufferMsg)
                this.enqueue(socket, sendMessage);
            else
                RpcServer.DoSend(socket, sendMessage);
        });
    }

    enqueue(socket, msg)
    {
        const id = socket.id;
        let queue = this.msgQueues[id];
        if (!queue)
        {
            queue = this.msgQueues[id] = [];
        }
        queue.push(msg);
    }

    flush()
    {
        const sockets = this.sockets,
                queues = this.msgQueues;
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
            RpcServer.DoSend(socket, queue);
            queues[socketId] = [];
        }
    }

    static CloneError(origin)
    {
        // copy the stack infoList for Error instance json result is empty
        return {
            msg   : origin.msg,
            stack : origin.stack
        };
    }

    static DoSend(socket, msg)
    {
        socket.publish({
            topic   : 'rpc',
            payload : JSON.stringify(msg)
        });
    }
}

/**
 * create acceptor
 *
 * @param opts init params
 * @param cb(msg, cb) callback function that would be invoked when new message arrives
 */
module.exports.create = function(opts, cb)
{
    return new RpcServer(opts || {}, cb);
};
