const Loader = require('pomelo-loader-upgrade');
const Gateway = require('./gateway');

class ServerUtility
{
	/**
	 * Create rpc server.
	 *
	 * @param opts {Object} init parameters
	 *    opts.port {Number|String}: rpc server listen port
	 *    opts.paths {Array}: remote service code paths, [{namespace, path}, ...]
	 *    opts.acceptorFactory {Object}: acceptorFactory.create(opts, cb)
	 */
	/**
	 * Create rpc server.
	 *
	 * @param  {Object} opts construct parameters
	 *                       opts.port {Number|String} rpc server listen port
	 *                       opts.paths {Array} remote service code paths, [{namespace, path}, ...]
	 *                       opts.context {Object} context for remote service
	 *                       opts.acceptorFactory {Object} (optionals)acceptorFactory.create(opts, cb)
	 * @return {Object}      rpc server instance
	 */
	static Create(opts)
	{
		if (!opts || !opts.port || opts.port < 0 || !opts.paths)
		{
			throw new Error('opts.port or opts.paths invalid.');
		}
		const services = ServerUtility.LoadRemoteServices(opts.paths, opts.context);
		opts.services = services;
		return Gateway.create(opts);
	}

	static LoadRemoteServices(paths, context)
	{
		let item, m;
		let i = 0;
		const res = {}, l = paths.length;
		for (i = 0; i < l; i++)
		{
			item = paths[i];
			m = Loader.load(item.path, context);
			if (m)
			{
				ServerUtility.CreateNamespace(item.namespace, res);
				for (const s in m)
				{
					res[item.namespace][s] = m[s];
				}
			}
		}

		return res;
	}

	static CreateNamespace(namespace, proxies)
	{
		proxies[namespace] = proxies[namespace] || {};
	}
}

module.exports.create = ServerUtility.Create;
// module.exports.WSAcceptor = require('./acceptors/ws-acceptor');
// module.exports.TcpAcceptor = require('./acceptors/tcp-acceptor');

module.exports.MqttAcceptor = require('./acceptors/mqtt-acceptor');