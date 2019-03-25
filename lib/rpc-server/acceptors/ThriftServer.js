'use strict';

const EventEmitter = require('events').EventEmitter;
const thrift = require('thrift');
const rpcStorage = require('../../thrift/RpcStorage.js');


class Server extends EventEmitter
{
    constructor(opts, callBack)
    {
        super();
        this.interval = opts.interval; // flush interval in ms
        this.bufferMsg = opts.bufferMsg;
        this.rpcLogger = opts.rpcLogger || console;
        this.rpcDebugLog = opts.rpcDebugLog;
        this._interval = null; // interval object
        this.sockets = {};
        this.msgQueues = {};
        this.callBack = callBack;
    }

    listen(port)
    {
        if (this.inited)
        {
            this.callBack(new Error('already inited.'));
            return;
        }
        this.inited = true;
        this.server = thrift.createServer(rpcStorage, {
            retrieve: this.remote.bind(this),
        });
        this.server.listen(port);
    }

    close()
    {
        if (this.closed) return;
        this.closed = true;
        this.connected = false;
        this.server.close();
        this.emit('closed');
    }

    remote(remoteCall, callBack)
    {
        let pkg = remoteCall.payload.toString();
        let isArray = false;
        try
        {
            pkg = JSON.parse(pkg);
            if (pkg instanceof Array)
            {
                for (let i = 0, l = pkg.length; i < l; i++)
                {
                    this.processMsg(pkg[i], callBack);
                }
                isArray = true;
            }
            else
            {
                this.processMsg(pkg, callBack);
            }
        }
        catch (err)
        {
            if (!isArray)
            {
                const sendMsg = {
                    id   : pkg.id,
                    resp : [this.cloneError(err)]
                };
                callBack(
                    {
                        topic   : 'rpc',
                        payload : JSON.stringify(sendMsg)
                    }
                );
            }
            this.rpcLogger.error('process rpc message error %s', err.stack);
        }
    }

    processMsg(messageData, callBack)
    {
        if (this.rpcDebugLog)
        {
            this.rpcLogger.info('server', __filename, messageData.remote, messageData.source, messageData.msg,'processMsg', 'GRPC receive message and try to process message');
        }
        this.callBack(messageData.msg, (...args) =>
        {
            // first callback argument can be error object, the others are message
            const errorArg = args[0];
            if (errorArg && errorArg instanceof Error)
            {
                args[0] = this.cloneError(errorArg);
            }
            callBack(null,
                {
                    topic   : 'rpc',
                    payload : JSON.stringify(
                        {
                            id   : messageData.id,
                            resp : args
                        }
                    )
                }
            );
        });
    }

    cloneError(origin)
    {
        // copy the stack infos for Error instance json result is empty
        return {
            msg   : origin.msg,
            stack : origin.stack
        };
    }
}

module.exports.create = function(opts, cb)
{
    return new Server(opts || {}, cb);
};
