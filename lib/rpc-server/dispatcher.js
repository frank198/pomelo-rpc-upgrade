var Utils = require('../util/Utils');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

class Dispatcher extends EventEmitter
{
	constructor(services)
	{
		super();
		this.on('reload', this.onReloadEvents);
		this.services = services;
	}

	onReloadEvents(services)
	{
		this.services = services;
	}

	/**
	 * route the msg to appropriate service object
	 *
	 * @param msg msg package {service:serviceString, method:methodString, args:[]}
	 * @param services services object collection, such as {service1: serviceObj1, service2: serviceObj2}
	 * @param cb(...) callback function that should be invoked as soon as the rpc finished
	 */
	route(tracer, msg, cb)
	{
		tracer.info('server', __filename, 'route', 'route messsage to appropriate service object');
		var namespace = this.services[msg.namespace];
		if(!namespace) {
			tracer.error('server', __filename, 'route', 'no such namespace:' + msg.namespace);
			Utils.invokeCallback(cb, new Error('no such namespace:' + msg.namespace));
			return;
		}

		var service = namespace[msg.service];
		if(!service) {
			tracer.error('server', __filename, 'route', 'no such service:' + msg.service);
			Utils.invokeCallback(cb, new Error('no such service:' + msg.service));
			return;
		}

		var method = service[msg.method];
		if(!method) {
			tracer.error('server', __filename, 'route', 'no such method:' + msg.method);
			Utils.invokeCallback(cb, new Error('no such method:' + msg.method));
			return;
		}

		var args = msg.args.slice(0);
		args.push(cb);
		method.apply(service, args);
	}
}


module.exports = Dispatcher;