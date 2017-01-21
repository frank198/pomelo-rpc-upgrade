const acceptor = require('./acceptors/mqtt-acceptor');

module.exports.create = function(opts, cb)
{
	return acceptor.create(opts, cb);
};