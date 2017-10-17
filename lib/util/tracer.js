const uuidV1 = require('uuid/v1');

class Tracer
{
	constructor(logger, enabledRpcLog, source, remote, msg, id, seq)
	{
		this.isEnabled = enabledRpcLog;
		if (!enabledRpcLog)
		{
			return;
		}
		this.logger = logger;
		this.source = source;
		this.remote = remote;
		this.id = id || uuidV1();
		this.seq = seq || 1;
		this.msg = msg;
	}

	getLogger(role, module, method, des)
	{
		return {
			traceId     : this.id,
			seq         : this.seq++,
			role        : role,
			source      : this.source,
			remote      : this.remote,
			module      : getModule(module),
			method      : method,
			args        : this.msg,
			timestamp   : Date.now(),
			description : des
		};
	}

	info(role, module, method, des)
	{
		if (!this.isEnabled) return;
		this.logger.info(JSON.stringify(this.getLogger(role, module, method, des)));
	}

	debug(role, module, method, des)
	{
		if (!this.isEnabled) return;
		this.logger.debug(JSON.stringify(this.getLogger(role, module, method, des)));
	}

	error(role, module, method, des)
	{
		if (!this.isEnabled) return;
		this.logger.error(JSON.stringify(this.getLogger(role, module, method, des)));
	}
}

const getModule = function(module)
{
	let rs = '';
	const strs = module.split('/');
	const lines = strs.slice(-3);
	for (let i = 0; i < lines.length; i++)
	{
		rs += `/${lines[i]}`;
	}
	return rs;
};

module.exports = function(logger, enabledRpcLog, source, remote, msg, id, seq)
{
	if (!(this instanceof Tracer))
	{
		return new Tracer(logger, enabledRpcLog, source, remote, msg, id, seq);
	}
	return this;
};