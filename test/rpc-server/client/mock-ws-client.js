const sioClient = require('socket.io-client');
const EventEmitter = require('events').EventEmitter;
const util = require('util');
const utils = require('../../../lib/util/Utils');

class Client extends EventEmitter
{
	constructor()
	{
		super();
		this.requests = {};
		this.curId = 0;
	}
	connect(host, port, cb)
	{
		this.socket = sioClient.connect(`${host}:${port}`, {forceNew: true});

		sioClient.Socket;
		const self = this;
		this.socket.on('message', function(pkg)
		{
			const cb = self.requests[pkg.id];
			delete self.requests[pkg.id];

			if (!cb)
			{
				return;
			}

			cb(...pkg.resp);
		});

		this.socket.on('connection', function()
		{
			utils.InvokeCallback(cb);
		});
	}

	send(msg, cb)
	{
		const id = this.curId++;
		this.requests[id] = cb;
		this.socket.emit('message', {id  : id,
			msg : msg});
	}

	close()
	{
		this.socket.disconnect();
	}
}

module.exports.create = function(opts)
{
	return new Client();
};