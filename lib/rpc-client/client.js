const Loader = require('pomelo-loader-upgrade');
const Proxy = require('../util/proxy');
const Station = require('./mailstation');
const utils = require('../util/Utils');
const router = require('./router');
const constants = require('../util/constants');
const Tracer = require('../util/tracer');
const failureProcess = require('./failureProcess');
const logger = require('pomelo-logger').getLogger('pomelo-rpc', __filename);

/**
 * Client states
 */
const STATE_INITED = 1;  // client has inited
const STATE_STARTED = 2;  // client has started
const STATE_CLOSED = 3;  // client has closed

class Client
{
	constructor(opts)
    {
		opts = opts || {};
		this._context = opts.context;
		this._routeContext = opts.routeContext;
		this.router = opts.router || router.df;
		this.routerType = opts.routerType;
		if (this._context)
        {
			opts.clientId = this._context.serverId;
		}
		this.opts = opts;
		this.proxies = {};
		this._station = ClientUtility.CreateStation(opts);
		this.state = STATE_INITED;
	}

    /**
     * Start the rpc client which would try to connect the remote servers and
     * report the result by cb.
     *
     * @param cb {Function} cb(err)
     */
	start(cb)
    {
		if (this.state > STATE_INITED)
        {
			utils.InvokeCallback(cb, new Error('rpc client has started.'));
			return;
		}

		this._station.start((err) =>
        {
			if (err)
			{
				logger.error(`[pomelo-rpc] client start fail for ${err.stack}`);
				utils.InvokeCallback(cb, err);
				return;
			}
			this._station.on('error', failureProcess.bind(this._station));
			this.state = STATE_STARTED;
			utils.InvokeCallback(cb);
		});
	}

    /**
     * Stop the rpc client.
     *
     * @param  {Boolean} force
     * @return {Void}
     */
	stop(force)
    {
		if (this.state !== STATE_STARTED)
        {
			logger.warn('[pomelo-rpc] client is not running now.');
			return;
		}
		this.state = STATE_CLOSED;
		this._station.stop(force);
	}

    /**
     * Add a new proxy to the rpc client which would overrid the proxy under the
     * same key.
     *
     * @param {Object} record proxy description record, format:
     *                        {namespace, serverType, path}
     */
	addProxy(record)
    {
		if (!record)
		{
			return;
		}
		const proxy = ClientUtility.GenerateProxy(this, record, this._context);
		if (!proxy)
        {
			return;
		}
		ClientUtility.InsertProxy(this.proxies, record.namespace, record.serverType, proxy);
	}

    /**
     * Batch version for addProxy.
     *
     * @param {Array} records list of proxy description record
     */
	addProxies(records)
    {
		if (!records || !records.length)
        {
			return;
		}
		const l = records.length;
		for (let i = 0; i < l; i++)
        {
			this.addProxy(records[i]);
		}
	}

    /**
     * Add new remote server to the rpc client.
     *
     * @param {Object} server new server information
     */
	addServer(server)
    {
		this._station.addServer(server);
	}

    /**
     * Batch version for add new remote server.
     *
     * @param {Array} servers server info list
     */
	addServers(servers)
    {
		this._station.addServers(servers);
	}

    /**
     * Remove remote server from the rpc client.
     *
     * @param  {String|Number} id server id
     */
	removeServer(id)
    {
		this._station.removeServer(id);
	}

    /**
     * Batch version for remove remote server.
     *
     * @param  {Array} ids remote server id list
     */
	removeServers(ids)
    {
		this._station.removeServers(ids);
	}

    /**
     * Replace remote servers.
     *
     * @param {Array} servers server info list
     */
	replaceServers(servers)
    {
		this._station.replaceServers(servers);
	}

    /**
     * Do the rpc invoke directly.
     *
     * @param serverId {String} remote server id
     * @param msg {Object} rpc message. Message format:
     *    {serverType: serverType, service: serviceName, method: methodName, args: arguments}
     * @param cb {Function} cb(err, ...)
     */
	rpcInvoke(serverId, msg, cb)
    {
		const tracer = new Tracer(this.opts.rpcLogger, this.opts.rpcDebugLog, this.opts.clientId, serverId, msg);
		tracer.info('client', __filename, 'rpcInvoke', 'the entrance of rpc invoke');
		if (this.state !== STATE_STARTED)
        {
			tracer.error('client', __filename, 'rpcInvoke', 'fail to do rpc invoke for client is not running');
			logger.error('[pomelo-rpc] fail to do rpc invoke for client is not running');
			cb(new Error('[pomelo-rpc] fail to do rpc invoke for client is not running'));
			return;
		}
		this._station.dispatch(tracer, serverId, msg, this.opts, cb);
	}

    /**
     * Add rpc before filter.
     *
     * @param filter {Function} rpc before filter function.
     *
     * @api public
     */
	before(filter)
    {
		this._station.before(filter);
	}

    /**
     * Add rpc after filter.
     *
     * @param filter {Function} rpc after filter function.
     *
     * @api public
     */
	after(filter)
    {
		this._station.after(filter);
	}

    /**
     * Add rpc filter.
     *
     * @param filter {Function} rpc filter function.
     *
     * @api public
     */
	filter(filter)
    {
		this._station.filter(filter);
	}

    /**
     * Set rpc filter error handler.
     *
     * @param handler {Function} rpc filter error handler function.
     *
     * @api public
     */
	setErrorHandler(handler)
    {
		this._station.handleError = handler;
	}
}

class ClientUtility
{

    /**
     * Create mail station.
     *
     * @param opts {Object} construct parameters.
     *
     * @api private
     */
	static CreateStation(opts)
    {
		return Station.create(opts);
	}

    /**
     * Generate proxies for remote servers.
     *
     * @param client {Object} current client instance.
     * @param record {Object} proxy reocrd info. {namespace, serverType, path}
     * @param context {Object} mailbox init context parameter
     *
     * @api private
     */
	static GenerateProxy(client, record, context)
    {
		if (!record)
        {
			return;
		}
		let res, name;
		const modules = Loader.load(record.path, context);
		if (modules)
        {
			res = {};
			for (name in modules)
            {
				res[name] = Proxy.create({
					service : name,
					origin  : modules[name],
					attach  : record,
					proxyCB : ClientUtility.ProxyCB.bind(null, client)
				});
			}
		}
		return res;
	}

    /**
     * Generate prxoy for function type field
     *
     * @param client {Object} current client instance.
     * @param serviceName {String} delegated service name.
     * @param methodName {String} delegated method name.
     * @param args {Object} rpc invoke arguments.
     * @param attach {Object} attach parameter pass to proxyCB.
     * @param isToSpecifiedServer {boolean} true means rpc route to specified remote server.
     *
     * @api private
     */
	static ProxyCB(client, serviceName, methodName, args, attach, isToSpecifiedServer)
    {
		if (client.state !== STATE_STARTED)
        {
			logger.error('[pomelo-rpc] fail to invoke rpc proxy for client is not running');
			return;
		}
		if (args.length < 2)
        {
			logger.error('[pomelo-rpc] invalid rpc invoke, arguments length less than 2, namespace: %j, serverType, %j, serviceName: %j, methodName: %j',
                attach.namespace, attach.serverType, serviceName, methodName);
			return;
		}
		const routeParam = args.shift();
		const cb = args.pop();
		const serverType = attach.serverType;
		const msg = {
			namespace  : attach.namespace,
			serverType : serverType,
			service    : serviceName,
			method     : methodName,
			args       : args};

		if (isToSpecifiedServer)
        {
			ClientUtility.RpcToSpecifiedServer(client, msg, serverType, routeParam, cb);
		}
		else
        {
			ClientUtility.GetRouteTarget(client, serverType, msg, routeParam, function(err, serverId)
            {
				if (err)
                {
					utils.InvokeCallback(cb, err);
				}
				else
                {
					client.rpcInvoke(serverId, msg, cb);
				}
			});
		}
	}

    /**
     * Calculate remote target server id for rpc client.
     *
     * @param client {Object} current client instance.
     * @param serverType {String} remote server type.
     * @param routeParam {Object} mailbox init context parameter.
     * @param cb {Function} return rpc remote target server id.
     *
     * @api private
     */
	static GetRouteTarget(client, serverType, msg, routeParam, cb)
    {
		if (client.routerType)
        {
			let method;
			switch (client.routerType)
            {
			case constants.SCHEDULE.ROUNDROBIN:
				method = router.rr;
				break;
			case constants.SCHEDULE.WEIGHT_ROUNDROBIN:
				method = router.wrr;
				break;
			case constants.SCHEDULE.LEAST_ACTIVE:
				method = router.la;
				break;
			case constants.SCHEDULE.CONSISTENT_HASH:
				method = router.ch;
				break;
			default:
				method = router.rd;
				break;
			}
			method(client, serverType, msg, (err, serverId) =>
            {
				utils.InvokeCallback(cb, err, serverId);
			});
		}
		else
        {
			let route, target;
			if (typeof client.router === 'function')
            {
				route = client.router;
				target = null;
			}
			else if (typeof client.router.route === 'function')
            {
				route = client.router.route;
				target = client.router;
			}
			else
            {
				logger.error('[pomelo-rpc] invalid route function.');
				return;
			}
			route.call(target, routeParam, msg, client._routeContext, (err, serverId) =>
            {
				utils.InvokeCallback(cb, err, serverId);
			});
		}
	}

    /**
     * Rpc to specified server id or servers.
     *
     * @param client {Object} current client instance.
     * @param msg {Object} rpc message.
     * @param serverType {String} remote server type.
     * @param routeParam {Object} mailbox init context parameter.
     *
     * @api private
     */
	static RpcToSpecifiedServer(client, msg, serverType, routeParam, cb)
    {
		if (typeof routeParam !== 'string')
        {
			logger.error('[pomelo-rpc] server id is not a string, server id: %j', routeParam);
			return;
		}
		if (routeParam === '*')
        {
			const servers = client._station.servers;
			for (const serverId in servers)
            {
				const server = servers[serverId];
				if (server.serverType === serverType)
                {
					client.rpcInvoke(serverId, msg, cb);
				}
			}
			return;
		}
		client.rpcInvoke(routeParam, msg, cb);
	}
    /**
     * Add proxy into array.
     *
     * @param proxies {Object} rpc proxies
     * @param namespace {String} rpc namespace sys/user
     * @param serverType {String} rpc remote server type
     * @param proxy {Object} rpc proxy
     *
     * @api private
     */
	static InsertProxy(proxies, namespace, serverType, proxy)
    {
		proxies[namespace] = proxies[namespace] || {};
		if (proxies[namespace][serverType])
        {
			for (const attr in proxy)
            {
				proxies[namespace][serverType][attr] = proxy[attr];
			}
		}
		else proxies[namespace][serverType] = proxy;
	}
}

/**
 * RPC client factory method.
 *
 * @param  {Object} opts client init parameter.
 *                       opts.context: mail box init parameter,
 *                       opts.router: (optional) rpc message route function, route(routeParam, msg, cb),
 *                       opts.mailBoxFactory: (optional) mail box factory instance.
 * @return {Object}      client instance.
 */
module.exports.create = function(opts)
{
	return new Client(opts);
};

module.exports.WSMailbox = require('./mailboxes/ws-mailbox');
