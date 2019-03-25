'use strict';
const logger = require('pomelo-logger-upgrade').getLogger('pomelo_rpc', 'failprocess');
const constants = require('../util/constants');

module.exports = function(code, callBack, serverId, msg, opts)
{
    const mode = opts.failMode;
    const FAIL_MODE = constants.FAIL_MODE;
    let method = failfast;
    if (mode === FAIL_MODE.FAILOVER)
        method = failover;
    else if (mode === FAIL_MODE.failsafe)
        method = failsafe;
    method.call(this, code, null, serverId, msg, opts, callBack);
};

/**
 * Failover rpc failure process. This will try other servers with option retries.
 *
 * @param code {Number} error code number.
 * @param tracer {Object} current rpc tracer.
 * @param serverId {String} rpc remote target server id.
 * @param msg {Object} rpc message.
 * @param opts {Object} rpc client options.
 * @param cb {Function} user rpc callback.
 *
 * @api private
 */
const failover = function(code, tracer, serverId, msg, opts, cb)
{
    let servers;
    const serverType = msg.serverType;
    if (!tracer || !tracer.servers)
    {
        servers = this.serversMap[serverType];
    }
    else
    {
        servers = tracer.servers;
    }

    const index = servers.indexOf(serverId);
    if (index >= 0)
    {
        servers.splice(index, 1);
    }
    tracer && (tracer.servers = servers);

    if (!servers.length)
    {
        logger.error('[pomelo-rpc] rpc failed with all this type of servers, with serverType: %s', serverType);
        cb(new Error(`rpc failed with all this type of servers, with serverType: ${serverType}`));
        return;
    }
    this.dispatch.call(this, servers[0], msg, opts, cb);
};

/**
 * fail safe rpc failure process.
 *
 * @param code {Number} error code number.
 * @param tracer {Object} current rpc tracer.
 * @param serverId {String} rpc remote target server id.
 * @param msg {Object} rpc message.
 * @param opts {Object} rpc client options.
 * @param cb {Function} user rpc callback.
 *
 * @api private
 */
const failsafe = function(code, tracer, serverId, msg, opts, cb)
{
    const retryTimes = opts.retryTimes || constants.DEFAULT_PARAM.FAILSAFE_RETRIES;
    const retryConnectTime = opts.retryConnectTime || constants.DEFAULT_PARAM.FAILSAFE_CONNECT_TIME;

    if (!this.retryTimes)
    {
        this.retryTimes = 1;
    }
    else
    {
        this.retryTimes += 1;
    }
    switch (code)
    {
        case constants.RPC_ERROR.SERVER_NOT_STARTED:
        case constants.RPC_ERROR.NO_TRAGET_SERVER:
            cb(new Error('rpc client is not started or cannot find remote server.'));
            break;
        case constants.RPC_ERROR.FAIL_CONNECT_SERVER:
            if (this.retryTimes <= retryTimes)
            {
                setTimeout(() =>
                {
                    this.connect(serverId, cb);
                }, retryConnectTime * this.retryTimes);
            }
            else
            {
                cb(new Error(`rpc client failed to connect to remote server: ${serverId}`));
            }
            break;
        case constants.RPC_ERROR.FAIL_FIND_MAILBOX:
        case constants.RPC_ERROR.FAIL_SEND_MESSAGE:
            if (this.retryTimes <= retryTimes)
            {
                setTimeout(() =>
                {
                    this.dispatch(serverId, msg, opts, cb);
                }, retryConnectTime * this.retryTimes);
            }
            else
            {
                cb(new Error(`rpc client failed to send message to remote server: ${serverId}`));
            }
            break;
        case constants.RPC_ERROR.FILTER_ERROR:
            cb(new Error('rpc client filter encounters error.'));
            break;
        default:
            cb(new Error('rpc client unknown error.'));
    }
};

/**
 * Fail fast rpc failure process. This will ignore error in rpc client.
 *
 * @param code {Number} error code number.
 * @param tracer {Object} current rpc tracer.
 * @param serverId {String} rpc remote target server id.
 * @param msg {Object} rpc message.
 * @param opts {Object} rpc client options.
 * @param cb {Function} user rpc callback.
 *
 * @api private
 */
const failfast = function(code, tracer, serverId, msg, opts, cb)
{
    logger.error('rpc failed with error, remote server: %s, msg: %j, error code: %s', serverId, msg, code);
    cb && cb(new Error(`rpc failed with error code: ${code}`));
};
