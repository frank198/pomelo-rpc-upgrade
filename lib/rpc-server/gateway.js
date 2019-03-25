'use strict';
const EventEmitter = require('events').EventEmitter;
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
        this.acceptorFactory = opts.acceptorFactory || Gateway.GetServer(opts.rpcType);
        this.services = opts.services;
        if (this.opts.reloadRemotes)
        {
            this.watchServices();
        }
        this.acceptor = this.acceptorFactory.create(opts, Gateway.Route.bind(null, this.services));
    }

    static GetServer(rpcType)
    {
        switch (rpcType) {
            case 'grpc':
                return require('./acceptors/GrpcHttp2Server');
            case 'thrift':
                return require('./acceptors/ThriftServer');
        }
        return require('./acceptors/mqttServer');
    }

    watchServices()
    {
        const paths = this.opts.paths;
        const app = this.opts.context;
        for (let i = 0, l = paths.length; i < l; i++)
        {
            this.watchFile(paths[i], app);
        }
    }

    stop()
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
        catch (err) {
            console.error(err);
        }
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

    watchFile(item, app)
    {
        fs.watchFile(item.path, () =>
        {
            const services = {};
            const namespace = item.namespace;
            const m = Loader.load(item.path, app);
            if (m)
            {
                services[namespace] = services[namespace] || {};
                for (const s in m)
                {
                    services[namespace][s] = m[s];
                }
            }
            Object.assign(this.services, services);
        });
    }

    /**
     * route the msg to appropriate service object
     * @param services remote service list
     * @param msg msg msg package {namespace:serviceString , service:serviceString, method:methodString, args:[]}
     * @param cb(...) callback function that should be invoked as soon as the rpc finished
     */
    static Route(services, msg, cb)
    {
        const namespace = services[msg.namespace];
        if (!namespace)
        {
            cb(new Error(`no such namespace:${msg.namespace}`));
            return;
        }
        const service = namespace[msg.service];
        if (!service)
        {
            cb(new Error(`no such service:${msg.service}`));
            return;
        }
        const method = service[msg.method];
        if (!method)
        {
            cb(new Error(`no such method:${msg.method}`));
            return;
        }
        const args = msg.args;
        args.push(cb);
        method.apply(service, args);
    }
}

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
