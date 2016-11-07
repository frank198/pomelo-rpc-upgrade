const EventEmitter = require('events').EventEmitter;
const defaultAcceptorFactory = require('./acceptor');
const Dispatcher = require('./dispatcher');
const fs = require('fs');
const Loader = require('pomelo-loader-upgrade');

class Gateway extends EventEmitter
{
	constructor(opts)
    {
		super();
		this.opts = opts || {};
		this.port = opts.port || 3050;
		this.started = false;
		this.stoped = false;
		this.acceptorFactory = opts.acceptorFactory || defaultAcceptorFactory;
		this.services = opts.services;
		const dispatcher = new Dispatcher(this.services);
		if (this.opts.reloadRemotes)
		{
			GatewayUtility.WatchServices(this, dispatcher);
		}
		this.acceptor = this.acceptorFactory.create(opts, (tracer, msg, cb) =>
        {
			dispatcher.route(tracer, msg, cb);
		});
	}

	start()
    {
		if (this.started)
        {
			throw new Error('gateway already start.');
		}
		this.started = true;
		this.acceptor.on('error', this.emit.bind(this, 'error'));
		this.acceptor.on('closed', this.emit.bind(this, 'closed'));
		this.acceptor.listen(this.port);
	}

	stop()
    {
		if (!this.started || this.stoped)
        {
			return;
		}
		this.stoped = true;
		try {this.acceptor.close();}
		catch (err) {console.error(err);}
	}
}

class GatewayUtility
{
	static Create(opts)
    {
		if (!opts || !opts.services)
        {
			throw new Error('opts and opts.services should not be empty.');
		}
		return new Gateway(opts);
	}

	static WatchServices(gateway, dispatcher)
    {
		const paths = gateway.opts.paths;
		const app = gateway.opts.context;
		for (let i = 0; i < paths.length; i++)
        {
			(function(index)
            {
				fs.watch(paths[index].path, function(event, name)
                {
					if (event === 'change')
                    {
						const res = {};
						const item = paths[index];
						const m = Loader.load(item.path, app);
						if (m)
                        {
							GatewayUtility.CreateNamespace(item.namespace, res);
							for (const s in m)
                            {
								res[item.namespace][s] = m[s];
							}
						}
						dispatcher.emit('reload', res);
					}
				});
			})(i);
		}
	}

	static CreateNamespace(namespace, proxies)
    {
		proxies[namespace] = proxies[namespace] || {};
	}
}

/**
 * create and init gateway
 *
 * @param opts {services: {rpcServices}, connector:conFactory(optional), router:routeFunction(optional)}
 */
module.exports.create = GatewayUtility.Create;
