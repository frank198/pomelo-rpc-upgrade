'use strict';
const constants = require('../../util/constants');
const logger = require('pomelo-logger-upgrade').getLogger('pomelo_rpc', 'thriftClient');
const EventEmitter = require('events').EventEmitter;
const thrift = require('thrift');
const rpcStorage = require('../../thrift/RpcStorage.js'),
        rpcTypes = require('../../thrift/rpc_types.js');
const CONNECT_TIMEOUT = 2000;

class Client extends EventEmitter {
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
        this.logger = opts.rpcLogger || logger || console;
        this.rpcDebugLog = opts.rpcDebugLog;
        this.keepaliveTimer = null;
        this.lastPing = -1;
        this.lastPong = -1;
        this.connected = false;
        this.closed = false;
        this.opts = opts;
        this.serverId = opts.context.serverId;
    }

    connect(cb)
    {
        if (this.rpcDebugLog)
        {
            this.logger.info('client', __filename, 'connect', 'mqtt-mailbox try to connect');
        }
        if (this.connected)
        {
            if (this.rpcDebugLog)
            {
                this.logger.info('client', __filename, 'connect', 'mailbox has already connected');
            }
            return cb(new Error('mailbox has already connected.'));
        }
        this.connection = thrift.createConnection(this.host, this.port, {
            connect_timeout:CONNECT_TIMEOUT,
            timeout:this.timeoutValue
        });
        this.client = thrift.createClient(rpcStorage, this.connection);
        this.connection.on('error', function(err) {
            this.logger.error('rpc socket %s is error, remote server %s host: %s, port: %s', this.serverId, this.id, this.host, this.port, err);
            this.emit('close', this.id);
        });
        this.connection.on('connect', (err) =>
        {
            if (!err)
                this.connected = true;
            cb(err);
        });
        this.connection.on('disconnect', (reason) =>
        {
            this.logger.error('rpc socket %s is disconnect from remote server %s, reason: %s', this.serverId, this.id, reason);
            const requests = this.requests;
            const keys = Object.keys(requests);
            for (let i = 0, l = keys.length; i < l; i++)
            {
                const ReqCb = requests[keys[i]];
                if (ReqCb)
                    ReqCb(new Error(`${this.serverId} disconnect with remote server ${this.id}`));
            }
            this.emit('close', this.id);
        });
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
        const retrieveData = new rpcTypes.RpcProfile({
            topic: 'rpc',
            payload: JSON.stringify(
                {
                    id: id,
                    msg: msg
                }
            )
        });
        // 这里是计时,检测 rpc 转发是否失败
        // setCbTimeout(this, id, tracer, cb);
        this.client.retrieve(retrieveData, (err, response) => {
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

    close()
    {
        if (this.closed) return;
        this.closed = true;
        this.connected = false;
        this.connection.end();
        this.connection.destroy();
    }
}

module.exports.create = function(server, opts)
{
    return new Client(server, opts || {});
};
