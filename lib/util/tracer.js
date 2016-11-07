const uuid = require('node-uuid');

class Tracer
{
    constructor(logger, enabledRpcLog, source, remote, msg, id, seq)
    {
        this.logger = logger;
        this.isEnabled = enabledRpcLog;
        this.source = source;
        this.remote = remote;
        this.id = id || uuid.v1();
        this.seq = seq || 1;
        this.msg = msg;
    }

    getLogger(role, module, method, des)
    {
        return {
            traceId: this.id,
            seq: this.seq++,
            role: role,
            source: this.source,
            remote: this.remote,
            module: TracerUtility.GetModule(module),
            method: method,
            args: this.msg,
            timestamp: Date.now(),
            description:des
        };
    }

    info(role, module, method, des)
    {
        if (this.isEnabled)
        {
            this.logger.info(JSON.stringify(this.getLogger(role, module, method, des)));
        }
        return;
    }

    debug(role, module, method, des)
    {
        if(this.isEnabled)
        {
            this.logger.debug(JSON.stringify(this.getLogger(role, module, method, des)));
        }
        return;
    }

    error(role, module, method, des)
    {
        if(this.isEnabled)
        {
            this.logger.error(JSON.stringify(this.getLogger(role, module, method, des)));
        }
        return;
    }


}

class TracerUtility
{
    static GetModule(module)
    {
        var rs ='';
        var strs = module.split('/');
        var lines = strs.slice(-3);
        for(var i= 0; i<lines.length; i++) {
            rs += '/' + lines[i];
        }
        return rs;
    }
}

module.exports = Tracer;