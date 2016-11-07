
class Utils
{
	static InvokeCallback(cb, ...args)
	{
		if (typeof cb === 'function')
		{
			const arg = Array.from ? Array.from(args) : [].slice.call(args);
			cb(...arg);
		}
	}

	static ApplyCallback(cb, args)
	{
		if (typeof cb === 'function')
		{
			cb(null, args);
		}
	}

	static GetObjectClass(obj)
	{
		if (obj && obj.constructor && obj.constructor.toString())
		{
			if (obj.constructor.name)
			{
				return obj.constructor.name;
			}
			const str = obj.constructor.toString();
			let arr = [];
			if (str.charAt(0) == '[')
			{
				arr = str.match(/\[\w+\s*(\w+)\]/);
			}
			else
			{
				arr = str.match(/function\s*(\w+)/);
			}
			if (arr && arr.length == 2)
			{
				return arr[1];
			}
		}
		return null;
	}
}

module.exports = Utils;