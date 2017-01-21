
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
			cb(...args);
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

	static GenServicesMap(services)
	{
		const nMap = {}; // namespace
		const sMap = {}; // service
		const mMap = {}; // method
		const nList = [];
		const sList = [];
		const mList = [];

		let nIndex = 0;
		let sIndex = 0;
		let mIndex = 0;

		for (const namespace in services)
		{
			nList.push(namespace);
			nMap[namespace] = nIndex++;
			const s = services[namespace];

			for (const service in s)
			{
				sList.push(service);
				sMap[service] = sIndex++;
				const m = s[service];

				for (const method in m)
				{
					const func = m[method];
					if (typeof func === 'function')
					{
						mList.push(method);
						mMap[method] = mIndex++;
					}
				}
			}
		}

		return [nMap, sMap, mMap, nList, sList, mList];
	}
}

module.exports = Utils;