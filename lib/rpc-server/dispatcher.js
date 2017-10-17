const EventEmitter = require('events').EventEmitter;

class Dispatcher extends EventEmitter
{
	constructor(services)
	{
		super();
		this.on('reload', (services) =>
		{
			this.services = services;
		});
		this.services = services;
	}
}

/**
 * route the msg to appropriate service object
 * @param tracer log4js
 * @param msg msg msg package {namespace:serviceString , service:serviceString, method:methodString, args:[]}
 * @param cb(...) callback function that should be invoked as soon as the rpc finished
 */
Dispatcher.prototype.route = function(tracer, msg, cb)
{
	tracer && tracer.info('server', __filename, 'route', 'route messsage to appropriate service object');
	const namespace = this.services[msg.namespace];
	if (!namespace)
	{
		tracer && tracer.error('server', __filename, 'route', `no such namespace:${msg.namespace}`);
		cb(new Error(`no such namespace:${msg.namespace}`));
		return;
	}

	const service = namespace[msg.service];
	if (!service)
	{
		tracer && tracer.error('server', __filename, 'route', `no such service:${msg.service}`);
		cb(new Error(`no such service:${msg.service}`));
		return;
	}

	const method = service[msg.method];
	if (!method)
	{
		tracer && tracer.error('server', __filename, 'route', `no such method:${msg.method}`);
		cb(new Error(`no such method:${msg.method}`));
		return;
	}

	const args = msg.args;
	args.push(cb);
	method.apply(service, args);
};

module.exports = function(services)
{
	if (!(this instanceof Dispatcher))
		return new Dispatcher(services);
	return this;
};