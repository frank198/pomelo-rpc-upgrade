const domain = require('domain');
const utils = require('../util/Utils');
const constants = require('../util/constants');
const logger = require('pomelo-logger').getLogger('pomelo-rpc', __filename);

class FailureProcess
{
	static Create(code, tracer, serverId, msg, opts)
	{
		const cb = tracer && tracer.cb;
		const mode = opts.failMode;
		let method = null;
		switch (mode)
		{
		case constants.FAIL_MODE.FAILOVER:
			method = FailureProcessUtility.Failover;
			break;
		case constants.FAIL_MODE.FAILBACK:
			method = FailureProcessUtility.Failback;
			break;
		case constants.FAIL_MODE.FAILFAST:
			method = FailureProcessUtility.Failfast;
			break;
		case constants.FAIL_MODE.FAILSAFE:
		default:
			method = FailureProcessUtility.Failfast;
			break;
		}
		method.call(this, code, tracer, serverId, msg, opts, cb);
	}
}

class FailureProcessUtility
{
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
	static Failover(code, tracer, serverId, msg, opts, cb)
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
			utils.InvokeCallback(cb, new Error(`rpc failed with all this type of servers, with serverType: ${serverType}`));
			return;
		}
		this.dispatch(tracer, servers[0], msg, opts, cb);
	}

	/**
	 * Failsafe rpc failure process.
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
	static Failsafe(code, tracer, serverId, msg, opts, cb)
	{
		const retryTimes = opts.retryTimes || constants.DEFAULT_PARAM.FAILSAFE_RETRIES;
		const retryConnectTime = opts.retryConnectTime || constants.DEFAULT_PARAM.FAILSAFE_CONNECT_TIME;

		if (tracer)
		{
			if (!tracer.retryTimes)
			{
				tracer.retryTimes = 1;
			}
			else
			{
				tracer.retryTimes += 1;
			}
		}

		switch (code)
		{
		case constants.RPC_ERROR.SERVER_NOT_STARTED:
		case constants.RPC_ERROR.NO_TRAGET_SERVER:
			utils.InvokeCallback(cb, new Error('rpc client is not started or cannot find remote server.'));
			break;
		case constants.RPC_ERROR.FAIL_CONNECT_SERVER:
			if (tracer && tracer.retryTimes <= retryTimes)
			{
				setTimeout(() =>
				{
					this.connect(tracer, serverId, cb);
				}, retryConnectTime * tracer.retryTimes);
			}
			else
			{
				utils.InvokeCallback(cb, new Error(`rpc client failed to connect to remote server: ${serverId}`));
			}
			break;
		case constants.RPC_ERROR.FAIL_FIND_MAILBOX:
		case constants.RPC_ERROR.FAIL_SEND_MESSAGE:
			if (tracer && tracer.retryTimes <= retryTimes)
			{
				setTimeout(() =>
				{
					this.dispatch(tracer, serverId, msg, opts, cb);
				}, retryConnectTime * tracer.retryTimes);
			}
			else
			{
				utils.InvokeCallback(cb, new Error(`rpc client failed to send message to remote server: ${serverId}`));
			}
			break;
		case constants.RPC_ERROR.FILTER_ERROR:
			utils.InvokeCallback(cb, new Error('rpc client filter encounters error.'));
			break;
		default:
			utils.InvokeCallback(cb, new Error('rpc client unknown error.'));
		}
	}

	/**
	 * Failback rpc failure process. This will try the same server with sendInterval option and retries option.
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
	static Failback(code, tracer, serverId, msg, opts, cb)
	{
		// todo record message in background and send the message at timing
	}

	/**
	 * Failfast rpc failure process. This will ignore error in rpc client.
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
	static Failfast(code, tracer, serverId, msg, opts, cb)
	{
		logger.error('rpc failed with error, remote server: %s, msg: %j, error code: %s', serverId, msg, code);
		utils.InvokeCallback(cb, new Error(`rpc failed with error code: ${code}`));
	}
}

module.exports = FailureProcess.Create;