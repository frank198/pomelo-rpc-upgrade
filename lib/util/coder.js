const logger = require('pomelo-logger').getLogger('pomelo-rpc', 'Coder');
// var OutBuffer = require('./buffer/outputBuffer');
// var InBuffer = require('./buffer/inputBuffer');
const bBuffer = require('bearcat-buffer');
const OutBuffer = bBuffer.outBuffer;
const InBuffer = bBuffer.inBuffer;

const Coder = {};

Coder.encodeClient = function(id, msg, servicesMap)
{
	// logger.debug('[encodeClient] id %s msg %j', id, msg);
	const outBuf = new OutBuffer();
	outBuf.writeUInt(id);
	const namespace = msg['namespace'];
	const serverType = msg['serverType'];
	const service = msg['service'];
	const method = msg['method'];
	const args = msg['args'] || [];
	outBuf.writeShort(servicesMap[0][namespace]);
	outBuf.writeShort(servicesMap[1][service]);
	outBuf.writeShort(servicesMap[2][method]);
	// outBuf.writeString(namespace);
	// outBuf.writeString(service);
	// outBuf.writeString(method);

	outBuf.writeObject(args);

	return outBuf.getBuffer();
};

Coder.encodeServer = function(id, args)
{
	// logger.debug('[encodeServer] id %s args %j', id, args);
	const outBuf = new OutBuffer();
	outBuf.writeUInt(id);
	outBuf.writeObject(args);
	return outBuf.getBuffer();
};

Coder.decodeServer = function(buf, servicesMap)
{
	const inBuf = new InBuffer(buf);
	const id = inBuf.readUInt();
	const namespace = servicesMap[3][inBuf.readShort()];
	const service = servicesMap[4][inBuf.readShort()];
	const method = servicesMap[5][inBuf.readShort()];
	// var namespace = inBuf.readString();
	// var service = inBuf.readString();
	// var method = inBuf.readString();

	const args = inBuf.readObject();
	// logger.debug('[decodeServer] namespace %s service %s method %s args %j', namespace, service, method, args)

	return {
		id  : id,
		msg : {
			namespace : namespace,
			// serverType: serverType,
			service   : service,
			method    : method,
			args      : args
		}
	};
};

Coder.decodeClient = function(buf)
{
	const inBuf = new InBuffer(buf);
	const id = inBuf.readUInt();
	const resp = inBuf.readObject();
	// logger.debug('[decodeClient] id %s resp %j', id, resp);
	return {
		id   : id,
		resp : resp
	};
};

module.exports = Coder;