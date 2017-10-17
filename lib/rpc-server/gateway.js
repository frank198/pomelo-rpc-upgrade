const defaultAcceptorFactory = require('./acceptor');
const EventEmitter = require('events').EventEmitter;
const Dispatcher = require('./dispatcher');
const Loader = require('pomelo-loader-upgrade');
const fs = require('fs');

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
			watchServices(this, dispatcher);
		}
		this.acceptor = this.acceptorFactory.create(opts, function(tracer, msg, cb)
		{
			dispatcher.route(tracer, msg, cb);
		});
	}
}

Gateway.prototype.stop = function()
{
	if (!this.started || this.stoped)
	{
		return;
	}
	this.stoped = true;
	try
	{
		this.acceptor.close();
	}
	catch (err) {}
};

Gateway.prototype.start = function()
{
	if (this.started)
	{
		throw new Error('gateway already start.');
	}
	this.started = true;

	const self = this;
	this.acceptor.on('error', self.emit.bind(self, 'error'));
	this.acceptor.on('closed', self.emit.bind(self, 'closed'));
	this.acceptor.listen(this.port);
};

/**
 * create and init gateway
 *
 * @param opts {services: {rpcServices}, connector:conFactory(optional), router:routeFunction(optional)}
 */
module.exports.create = function(opts)
{
	if (!opts || !opts.services)
	{
		throw new Error('opts and opts.services should not be empty.');
	}
	return new Gateway(opts);
};

const watchServices = function(gateway, dispatcher)
{
	const paths = gateway.opts.paths;
	const app = gateway.opts.context;
	for (let i = 0; i < paths.length; i++)
	{
		(function(index)
		{
			fs.watch(paths[index].path, (event, name) =>
			{
				if (event === 'change')
				{
					const res = {};
					const item = paths[index];
					const m = Loader.load(item.path, app);
					if (m)
					{
						res[item.namespace] = res[item.namespace] || {};
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
};