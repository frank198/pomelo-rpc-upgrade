var sioClient = require('socket.io-client');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var utils = require('../../../lib/util/Utils');

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
        this.socket = sioClient.connect(host + ':' + port, {forceNew: true});

        sioClient.Socket
        var self = this;
        this.socket.on('message', function(pkg) {
            var cb = self.requests[pkg.id];
            delete self.requests[pkg.id];

            if(!cb) {
                return;
            }

            cb.apply(null, pkg.resp);
        });

        this.socket.on('connection', function() {
            utils.InvokeCallback(cb);
        });
    }

    send(msg, cb)
    {
        var id = this.curId++;
        this.requests[id] = cb;
        this.socket.emit('message', {id: id, msg: msg});
    }

    close()
    {
        this.socket.disconnect();
    }
}


module.exports.create = function(opts) {
  return new Client();
};