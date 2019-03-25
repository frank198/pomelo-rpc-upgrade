'use strict';
const EventEmitter = require('events').EventEmitter;
const constants = require('../../util/constants');
const grpc = require('grpc');
const protoLoader = require('@grpc/proto-loader');
const PROTO_PATH = require.resolve('../../../config/rpc.proto');

const CONNECT_TIMEOUT = 2000;
const packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });

class Client extends EventEmitter
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
        this.logger = opts.rpcLogger || console;
        this.rpcDebugLog = opts.rpcDebugLog || false;
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

    connect(callBack)
    {
        if (this.connected)
        {
            if (callBack) callBack(null);
            return;
        }
        const rpcRemote = grpc.loadPackageDefinition(packageDefinition).RpcRemote;
        this.client = new rpcRemote.Greeter(`${this.host}:${this.port}`, grpc.credentials.createInsecure());
        // 连接超时
        this.client.waitForReady(Date.now() + CONNECT_TIMEOUT, error =>{
            if (error)
            {
                this.logger.error(error);
                this.emit('close', this.id);
                return;
            }
            if (callBack) callBack(null);
        });
        this.connected = true;
    }


    close()
    {
        if (this.closed) return;
        this.closed = true;
        this.connected = false;
        this.client.close();
    }

    send(msg, opts, cb)
    {
        if (!this.connected)
        {
            this.logger.error(this.serverId, 'client', this.id, __filename, 'send', 'GRpc http2 not init');
            cb(new Error(`${this.serverId} GRpc http2 is not init ${this.id}`));
            return;
        }

        if (this.closed)
        {
            this.logger.error(this.serverId, 'client', this.id, __filename, 'send', 'GRpc http2 has already closed');
            cb(new Error(`${this.serverId} GRpc http2 has already closed ${this.id}`));
            return;
        }

        const id = this.curId++;
        this.requests[id] = cb;
        // 这里是计时,检测 rpc 转发是否失败
        // setCbTimeout(this, id, tracer, cb);
        this.client.Remote(
            {
                topic   : 'rpc',
                payload : JSON.stringify(
                    {
                        id  : id,
                        msg : msg
                    }
                )
            }, (err, response) => {
                if (err)
                {
                    this.logger.error(err);
                    return;
                }
                this.receiveData(response);
            });
    }

    /**
     * 接收到服务器数据
     * @param data
     */
    receiveData(data)
    {
        const message = JSON.parse(data.payload);
        if (Array.isArray(message))
        {
            for (let i = 0, l = message.length; i < l; i++)
            {
                this.processMsg(message[i]);
            }
        }
        else
        {
            this.processMsg(message);
        }
    }

    processMsg(message)
    {
        const pkgId = message.id;
        // clearCbTimeout(mailbox, pkgId);
        const cb = this.requests[pkgId];
        if (!cb)
        {
            return;
        }
        delete this.requests[pkgId];
        const sendErr = null;
        if (this.rpcDebugLog)
        {
            this.logger.info(this.opts.clientId, message.source, message.resp, message.traceId, message.seqId);
        }
        const pkgResp = message.resp;
        cb(sendErr, pkgResp);
    }
}

module.exports.create = function(server, opts)
{
    return new Client(server, opts || {});
};
