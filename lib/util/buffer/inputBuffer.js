const logger = require('pomelo-logger').getLogger('pomelo-rpc', 'InputBuffer');
const Utils = require('../utils');

class InputBuffer
{
	constructor(buffer)
	{
		this.buf = buffer;
		this.pos = 0;
		this.count = buffer.length;
	}
}

InputBuffer.prototype.read = function()
{
	return this.readByte();
};

InputBuffer.prototype.readBoolean = function()
{
	const r = this.read();
	if (r < 0)
{
		throw new Error('EOFException');
	}

	return (r != 0);
};

InputBuffer.prototype.readByte = function()
{
	this.check(1);
	return this.buf.readUInt8(this.pos++);
};

InputBuffer.prototype.readBytes = function()
{
	const len = this.readInt();
	this.check(len);
	const r = this.buf.slice(this.pos, this.pos + len);
	this.pos += len;
	return r;
};

InputBuffer.prototype.readChar = function()
{
	return this.readByte();
};

InputBuffer.prototype.readDouble = function()
{
	this.check(8);
	const r = this.buf.readDoubleLE(this.pos);
	this.pos += 8;
	return r;
};

InputBuffer.prototype.readFloat = function()
{
	this.check(4);
	const r = this.buf.readFloatLE(this.pos);
	this.pos += 4;
	return r;
};

InputBuffer.prototype.readInt = function()
{
	this.check(4);
	const r = this.buf.readInt32LE(this.pos);
	this.pos += 4;
	return r;
};

InputBuffer.prototype.readShort = function()
{
	this.check(2);
	const r = this.buf.readInt16LE(this.pos);
	this.pos += 2;
	return r;
};

InputBuffer.prototype.readUInt = function()
{
	this.check(4);
	const r = this.buf.readUInt32LE(this.pos);
	this.pos += 4;
	return r;
};

InputBuffer.prototype.readUShort = function()
{
	this.check(2);
	const r = this.buf.readUInt16LE(this.pos);
	this.pos += 2;
	return r;
};

InputBuffer.prototype.readString = function()
{
	const len = this.readInt();
	this.check(len);
	const r = this.buf.toString('utf8', this.pos, this.pos + len);
	this.pos += len;
	return r;
};

InputBuffer.prototype.readObject = function()
{
	const type = this.readShort();
	let instance = null;
	// console.log('readObject %s', type)
	const typeMap = Utils.typeMap;

	if (typeMap['null'] == type)
	{
		// 没有逻辑
	}
	else if (typeMap['buffer'] == type)
	{
		instance = this.readBytes();
	}
	else if (typeMap['array'] == type)
	{
		instance = [];
		const len = this.readInt();
		for (let i = 0; i < len; i++)
		{
			instance.push(this.readObject());
		}
	}
	else if (typeMap['string'] == type)
	{
		instance = this.readString();
	}
	else if (typeMap['object'] == type)
	{
		const objStr = this.readString();
		instance = JSON.parse(objStr);
	}
	else if (typeMap['bean'] == type)
	{
		const id = this.readString();
		const bearcat = Utils.getBearcat();
		const bean = bearcat.getBean(id);
		if (!bean)
		{
			logger.error('readBean bean not found %s', id);
			return;
		}
		bean.readFields(this);
		instance = bean;
	}
	else if (typeMap['boolean'] == type)
	{
		instance = this.readBoolean();
	}
	else if (typeMap['float'] == type)
	{
		instance = this.readFloat();
	}
	else if (typeMap['number'] == type)
	{
		instance = this.readInt();
	}
	else
	{
		logger.error('readObject invalid read type %j', type);
	}

	return instance;
};

InputBuffer.prototype.check = function(len)
{
	if (this.pos + len > this.count)
{
		throw new Error('IndexOutOfBoundsException');
	}
};

module.exports = function(buffer)
{
	return new InputBuffer(buffer);
};