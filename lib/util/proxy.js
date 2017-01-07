const logger = require('pomelo-logger').getLogger('pomelo-rpc', __filename);

class Proxy
{
    /**
     * Create proxy.
     *
     * @param  {Object} opts construct parameters
     *           opts.origin {Object} delegated object
     *           opts.proxyCB {Function} proxy invoke callback
     *           opts.service {String} deletgated service name
     *           opts.attach {Object} attach parameter pass to proxyCB
     * @return {Object}      proxy instance
     */
	static create(opts)
    {
		if (!opts || !opts.origin)
        {
			logger.warn('opts and opts.origin should not be empty.');
			return null;
		}

		if (!opts.proxyCB || typeof opts.proxyCB !== 'function')
        {
			logger.warn('opts.proxyCB is not a function, return the origin module directly.');
			return opts.origin;
		}

		return ProxyUtility.GenObjectProxy(opts.service, opts.origin, opts.attach, opts.proxyCB);
	}
}

class ProxyUtility
{
	static GenObjectProxy(serviceName, origin, attach, proxyCB)
    {
        // generate proxy for function field
		const res = {};
		for (const field in origin)
        {
			if (typeof origin[field] === 'function')
            {
				res[field] = ProxyUtility.GenFunctionProxy(serviceName, field, origin, attach, proxyCB);
			}
		}
		return res;
	}

    /**
     * Generate prxoy for function type field
     *
     * @param namespace {String} current namespace
     * @param serverType {String} server type string
     * @param serviceName {String} delegated service name
     * @param methodName {String} delegated method name
     * @param origin {Object} origin object
     * @param proxyCB {Functoin} proxy callback function
     * @returns function proxy
     */
	static GenFunctionProxy(serviceName, methodName, origin, attach, proxyCB)
    {
	    return (function()
	    {
		    const proxy = function(...args)
		    {
			    proxyCB.call(null, serviceName, methodName, args, attach);
		    };

		    proxy.toServer = function(...args)
		    {
			    proxyCB.call(null, serviceName, methodName, args, attach, true);
		    };

		    return proxy;
	    })();
	}
}

module.exports = Proxy;