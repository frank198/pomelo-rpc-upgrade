'use strict';
const Loader = require('pomelo-loader-upgrade');
const Gateway = require('./gateway');

const loadRemoteServices = function(paths, context)
{
    const res = {};
    let	item, m;
    for (let i = 0, l = paths.length; i < l; i++)
    {
        item = paths[i];
        m = Loader.load(item.path, context);
        if (m)
        {
            res[item.namespace] = res[item.namespace] || {};
            for (const s in m)
            {
                res[item.namespace][s] = m[s];
            }
        }
    }
    return res;
};

/**
 * Create rpc server.
 *
 * @param  {Object}      opts construct parameters
 *                       opts.port {Number|String} rpc server listen port
 *                       opts.paths {Array} remote service code paths, [{namespace, path}, ...]
 *                       opts.context {Object} context for remote service
 *                       opts.acceptorFactory {Object} (optionals)acceptorFactory.create(opts, cb)
 * @return {Object}      rpc server instance
 */
module.exports.create = function(opts)
{
    if (!opts || !opts.port || opts.port < 0 || !opts.paths)
    {
        throw new Error('opts.port or opts.paths invalid.');
    }
    opts.services = loadRemoteServices(opts.paths, opts.context);
    return Gateway.create(opts);
};
