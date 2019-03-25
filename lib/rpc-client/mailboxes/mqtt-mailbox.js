'use strict';
const logger = require('pomelo-logger-upgrade').getLogger('pomelo_rpc', 'mqttClient');
const EventEmitter = require('events').EventEmitter;
const constants = require('../../util/constants');
const MqttCon = require('mqtt-connection');
const net = require('net');

const CONNECT_TIMEOUT = 2000;

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
        const stream = net.createConnection(this.port, this.host);
        this.socket = MqttCon(stream);
        const connectTimeout = setTimeout(function()
        {
            this.logger.error('rpc client %s connect to remote server %s timeout', this.serverId, this.id);
            this.emit('close', this.id);
        }, CONNECT_TIMEOUT);

        this.socket.connect(
            {
                clientId : `MQTT_RPC_${Date.now()}`
            }, () =>
            {
                if (this.connected)
                {
                    return;
                }

                clearTimeout(connectTimeout);
                this.connected = true;
                if (this.bufferMsg)
                {
                    this._interval = setInterval(this._flush.bind(this), this.interval);
                }
                this.setupKeepAlive();
                cb();
            });

        this.socket.on('publish', (pkg) =>
        {
            pkg = pkg.payload.toString();
            try
            {
                pkg = JSON.parse(pkg);
                if (pkg instanceof Array)
                {
                    for (let i = 0, l = pkg.length; i < l; i++)
                    {
                        this._processMsg(pkg[i]);
                    }
                }
                else
                {
                    this._processMsg(pkg);
                }
            }
            catch (err)
            {
                this.logger.error('rpc client %s process remote server %s message with error: %s', this.serverId, this.id, err.stack);
            }
        });

        this.socket.on('error', (err) =>
        {
            this.logger.error('rpc socket %s is error, remote server %s host: %s, port: %s', this.serverId, this.id, this.host, this.port, err);
            this.emit('close', this.id);
        });

        this.socket.on('pingresp', () =>
        {
            this.lastPong = Date.now();
        });

        this.socket.on('disconnect', (reason) =>
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
        this.connected = false;
        if (this._interval)
        {
            clearInterval(this._interval);
            this._interval = null;
        }
        this.socket.destroy();
    }

    /**
	 * send message to remote server
	 * @param msg {object} {service:"", method:"", args:[]}
	 * @param opts {object} attach info to send method
	 * @param cb declaration decided by remote interface
	 */
    send(msg, opts, cb)
    {
        if (this.rpcDebugLog)
            this.logger.info('client', __filename, 'send', 'mqtt-mailbox try to send', msg);
        if (!this.connected)
        {
            if (this.rpcDebugLog)
                this.logger.info('client', __filename, 'send', 'mqtt-mailbox not init', msg);
            cb(new Error(`${this.serverId} mqtt-mailbox is not init ${this.id}`));
            return;
        }

        if (this.closed)
        {
            cb(new Error(`${this.serverId} mqtt-mailbox has already closed ${this.id}`));
            return;
        }

        const id = this.curId++;
        this.requests[id] = cb;
        this._setCbTimeout(id, cb);

        const sendMsg = {
            id  : id,
            msg : msg
        };
        if (this.bufferMsg)
        {
            this._enqueue(sendMsg);
        }
        else
        {
            Client.DoSend(this.socket, sendMsg);
        }
    }

    setupKeepAlive()
    {
        this.keepaliveTimer = setInterval(() =>
        {
            this.checkKeepAlive();
        }, this.keepalive);
    }

    checkKeepAlive()
    {
        if (this.closed)
        {
            return;
        }

        // console.log('checkKeepAlive lastPing %d lastPong %d ~~~', this.lastPing, this.lastPong);
        const now = Date.now();
        const KEEP_ALIVE_TIMEOUT = this.keepalive * 2;
        if (this.lastPing > 0)
        {
            if (this.lastPong < this.lastPing)
            {
                if (now - this.lastPing > KEEP_ALIVE_TIMEOUT)
                {
                    this.logger.error('mqtt rpc client %s checkKeepAlive timeout from remote server %s for %d lastPing: %s lastPong: %s', this.serverId, this.id, KEEP_ALIVE_TIMEOUT, this.lastPing, this.lastPong);
                    this.emit('close', this.id);
                    this.lastPing = -1;
                    // this.close();
                }
            }
            else
            {
                this.socket.pingreq();
                this.lastPing = Date.now();
            }
        }
        else
        {
            this.socket.pingreq();
            this.lastPing = Date.now();
        }
    }

    /**
	 *
	 * @param socket
	 * @param msg
	 */
    static DoSend(socket, msg)
    {
        socket.publish({
            topic   : 'rpc',
            payload : JSON.stringify(msg)
        });
    }

    _enqueue(msg)
    {
        this.queue.push(msg);
    }

    _flush()
    {
        if (this.closed || !this.queue.length)
        {
            return;
        }
        Client.DoSend(this.socket, this.queue);
        this.queue = [];
    }

    _processMsg(pkg)
    {
        const pkgId = pkg.id;
        this._clearCbTimeout(pkgId);
        const cb = this.requests[pkgId];
        if (!cb)
        {
            return;
        }
        delete this.requests[pkgId];
        const sendErr = null;
        if (this.rpcDebugLog)
        {
            this.logger.info(this.opts.clientId, pkg.source, pkg.resp, pkg.traceId, pkg.seqId);
        }
        const pkgResp = pkg.resp;

        cb(sendErr, pkgResp);
    }

    _setCbTimeout(id, cb)
    {
        this.timeout[id] = setTimeout(() =>
        {
            // logger.warn('rpc request is timeout, id: %s, host: %s, port: %s', id, mailbox.host, mailbox.port);
            this._clearCbTimeout(id);
            if (this.requests[id])
            {
                delete this.requests[id];
            }
            const eMsg = `rpc ${this.serverId} callback timeout ${this.timeoutValue}, remote server ${id} host: ${this.host}, port: ${this.port} `;
            this.logger.error(eMsg);
            cb(new Error(eMsg));
        }, this.timeoutValue);
    }

    _clearCbTimeout(id)
    {
        if (!this.timeout[id])
        {
            this.logger.warn('timer is not exsits, serverId: %s remote: %s, host: %s, port: %s', this.serverId, id, this.host, this.port);
            return;
        }
        clearTimeout(this.timeout[id]);
        delete this.timeout[id];
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
    return new Client(server, opts || {});
};
