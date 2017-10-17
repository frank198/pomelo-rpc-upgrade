const logger = require('pomelo-logger').getLogger('pomelo-rpc', 'InputBuffer');

const Utils = require('../utils');
const Codec = require('../codec');

class InputBuffer
{
	constructor(buffer)
	{
		this.buf = buffer;
		this.offset = 0;
		this.size = buffer.length;
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

	return r !== 0;
};

InputBuffer.prototype.readByte = function()
{
	this.check(1);
	return this.buf.readUInt8(this.offset++, true);
};

InputBuffer.prototype.readBuffer = function()
{
	const len = this.readUInt();
	this.check(len);
	const r = this.buf.slice(this.offset, this.offset + len);
	this.offset += len;
	return r;
};

InputBuffer.prototype.readChar = function()
{
	return this.readByte();
};

InputBuffer.prototype.readVInt = function()
{
	return Codec.decodeUInt32(this.getBytes());
};

InputBuffer.prototype.readUInt = function()
{
	return this.readVInt();
};

InputBuffer.prototype.readSInt = function()
{
	return Codec.decodeSInt32(this.getBytes());
};

InputBuffer.prototype.readDouble = function()
{
	this.check(8);
	const r = this.buf.readDoubleLE(this.offset, true);
	this.offset += 8;
	return r;
};

InputBuffer.prototype.readFloat = function()
{
	this.check(4);
	const r = this.buf.readFloatLE(this.offset, true);
	this.offset += 4;
	return r;
};

InputBuffer.prototype.readInt = function()
{
	this.check(4);
	const r = this.buf.readInt32LE(this.offset, true);
	this.offset += 4;
	return r;
};

InputBuffer.prototype.readShort = function()
{
	this.check(2);
	const r = this.buf.readInt16LE(this.offset, true);
	this.offset += 2;
	return r;
};

InputBuffer.prototype.readUShort = function()
{
	this.check(2);
	const r = this.buf.readUInt16LE(this.offset, true);
	this.offset += 2;
	return r;
};

InputBuffer.prototype.readString = function()
{
	const len = this.readUInt();
	this.check(len);
	const r = this.buf.toString('utf8', this.offset, this.offset + len);
	this.offset += len;
	return r;
};

InputBuffer.prototype.getBytes = function()
{
	const bytes = [];
	let offset = this.offset;

	const buffer = this.buf;
	let b;
	do
	{
		b = buffer.readUInt8(offset, true);
		bytes.push(b);
		offset++;
	} while (b >= 128);

	this.offset = offset;

	return bytes;
};

InputBuffer.prototype.readObject = function()
{
	const type = this.readByte();
	let instance = null;
	const typeMap = Utils.typeMap;

	switch (type)
	{
		case typeMap['buffer']:
			instance = this.readBuffer();
			break;
		case typeMap['array']:
		{
			instance = [];
			const len = this.readVInt();
			for (let i = 0; i < len; i++)
			{
				instance.push(this.readObject());
			}
			break;
		}
		case typeMap['string']:
			instance = this.readString();
			break;
		case typeMap['object']:
		{
			const objStr = this.readString();
			instance = JSON.parse(objStr);
			break;
		}
		case typeMap['bean']:
		{
			const id = this.readString();
			const bean = Utils.getBearcat().getBean(id);
			if (!bean)
			{
				throw new Error(`readBean bean not found ${id}`);
			}
			bean.readFields(this);
			instance = bean;
			break;
		}
		case typeMap['boolean']:
			instance = this.readBoolean();
			break;
		case typeMap['float']:
			instance = this.readFloat();
			break;
		case typeMap['uint']:
			instance = this.readUInt();
			break;
		case typeMap['sint']:
			instance = this.readSInt();
			break;
		case typeMap['null']:
			instance = null;
			break;
		default:
			throw new Error(`readObject invalid read type ${type}`);
	}
	return instance;
};

InputBuffer.prototype.check = function(len)
{
	if (this.offset + len > this.size)
	{
		throw new Error('IndexOutOfBoundsException');
	}
};

module.exports = function(buffer)
{
	if (!(this instanceof InputBuffer))
	{
		return new InputBuffer(buffer);
	}
	return this;
};