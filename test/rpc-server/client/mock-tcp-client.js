var net = require('net');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var utils = require('../../../lib/util/Utils');
var Composer = require('../../../lib/util/Composer');


class Client extends EventEmitter
{
    constructor()
    {
        super();
        this.requests = {};
        this.curId = 0;
        this.composer = new Composer();
        this.socket = null;
    }

    connect(host, port, cb)
    {
        this.socket = net.connect({port: port, host: host}, function() {
            utils.InvokeCallback(cb);
        });
        console.log('socket: %j', !!this.socket);
        var self = this;
        this.socket.on('data', function(data) {
            self.composer.feed(data);
        });

        this.composer.on('data', function(data) {
            var pkg = JSON.parse(data.toString());
            var cb = self.requests[pkg.id];
            delete self.requests[pkg.id];

            if(!cb) {
                return;
            }

            cb.apply(null, pkg.resp);
        });
    }

    send(msg, cb)
    {
        var id = this.curId++;
        this.requests[id] = cb;
        this.socket.write(this.composer.compose(JSON.stringify({id: id, msg: msg})));
    };

    close()
    {
        this.socket.end();
    };
}

module.exports.create = function(opts) {
  return new Client();
};