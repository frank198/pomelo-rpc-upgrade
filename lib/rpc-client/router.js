const crc = require('crc');
const utils = require('../util/Utils');
const ConsistentHash = require('../util/consistentHash');

class Router
{
    /**
     * Calculate route info and return an appropriate server id.
     * @param session {Object} session object for current rpc request
     * @param msg {Object} rpc message. {serverType, service, method, args, opts}
     * @param context  {Object} context of client
     * @param cb(err, serverId)
     * @constructor
     */
	static DefRoute(session, msg, context, cb)
    {
		const list = context.getServersByType(msg.serverType);
		if (!list || !list.length)
		{
			cb(new Error(`can not find server info for type:${msg.serverType}`));
			return;
		}
		const uid = session ? (session.uid || '') : '';
		const index = Math.abs(crc.crc32(uid.toString())) % list.length;
		utils.InvokeCallback(cb, null, list[index].id);
	}

    /**
     * Random algorithm for calculating server id.
     *
     * @param client {Object} rpc client.
     * @param serverType {String} rpc target serverType.
     * @param msg {Object} rpc message.
     * @param cb {Function} cb(err, serverId).
     */
	static RdRoute(client, serverType, msg, cb)
    {
		const servers = client._station.serversMap[serverType];
		if (!servers || !servers.length)
		{
			utils.InvokeCallback(cb, new Error(`rpc servers not exist with serverType: ${serverType}`));
			return;
		}
		const index = Math.floor(Math.random() * servers.length);
		utils.InvokeCallback(cb, null, servers[index]);
	}

    /**
     * Round-Robin algorithm for calculating server id.
     *
     * @param client {Object} rpc client.
     * @param serverType {String} rpc target serverType.
     * @param msg {Object} rpc message.
     * @param cb {Function} cb(err, serverId).
     */
	static RoundRobinRoute(client, serverType, msg, cb)
    {
		const servers = client._station.serversMap[serverType];
		if (!servers || !servers.length)
		{
			utils.InvokeCallback(cb, new Error(`rpc servers not exist with serverType: ${serverType}`));
			return;
		}
		let index;
		if (!client.rrParam)
		{
			client.rrParam = {};
		}
		if (client.rrParam[serverType])
		{
			index = client.rrParam[serverType];
		}
		else
        {
			index = 0;
		}
		utils.InvokeCallback(cb, null, servers[index % servers.length]);
		if (index++ === Number.MAX_VALUE)
		{
			index = 0;
		}
		client.rrParam[serverType] = index;
	}

    /**
     * Weight-Round-Robin algorithm for calculating server id.
     *
     * @param client {Object} rpc client.
     * @param serverType {String} rpc target serverType.
     * @param msg {Object} rpc message.
     * @param cb {Function} cb(err, serverId).
     */
	static WeightRoundRobinRoute(client, serverType, msg, cb)
    {
		const servers = client._station.serversMap[serverType];
		if (!servers || !servers.length)
		{
			utils.InvokeCallback(cb, new Error(`rpc servers not exist with serverType: ${serverType}`));
			return;
		}
		let index, weight;
		if (!client.wrrParam)
		{
			client.wrrParam = {};
		}
		if (client.wrrParam[serverType])
		{
			index = client.wrrParam[serverType].index;
			weight = client.wrrParam[serverType].weight;
		}
		else
        {
			index = -1;
			weight = 0;
		}
		const getMaxWeight = () =>
        {
			let maxWeight = -1;
			for (let i = 0; i < servers.length; i++)
			{
				const server = client._station.servers[servers[i]];
				if (Boolean(server.weight) && server.weight > maxWeight)
				{
					maxWeight = server.weight;
				}
			}
			return maxWeight;
		};
		index = (index + 1) % servers.length;
		if (index === 0)
        {
			weight = weight - 1;
			if (weight <= 0)
            {
				weight = getMaxWeight();
				if (weight <= 0)
                {
					utils.InvokeCallback(cb, new Error('rpc wrr route get invalid weight.'));
					return;
				}
			}
		}
		const server = client._station.servers[servers[index]];
		if (server.weight >= weight)
        {
			client.wrrParam[serverType] = {
				index  : index,
				weight : weight};
			utils.InvokeCallback(cb, null, server.id);
		}

	}

    /**
     * Least-Active algorithm for calculating server id.
     *
     * @param client {Object} rpc client.
     * @param serverType {String} rpc target serverType.
     * @param msg {Object} rpc message.
     * @param cb {Function} cb(err, serverId).
     */
	static LeastActiveRoute(client, serverType, msg, cb)
    {
		const servers = client._station.serversMap[serverType];
		if (!servers || !servers.length)
		{
			utils.InvokeCallback(cb, new Error(`rpc servers not exist with serverType: ${serverType}`));
			return;
		}
		const actives = [];
		if (!client.laParam)
		{
			client.laParam = {};
		}
		if (client.laParam[serverType])
		{
			for (let j = 0; j < servers.length; j++)
			{
				let count = client.laParam[serverType][servers[j]];
				if (!count)
				{
					client.laParam[servers[j]] = count = 0;
				}
				actives.push(count);
			}
		}
		else
        {
			client.laParam[serverType] = {};
			for (let i = 0; i < servers.length; i++)
			{
				client.laParam[serverType][servers[i]] = 0;
				actives.push(0);
			}
		}
		let rs = [];
		let minInvoke = Number.MAX_VALUE;
		for (let k = 0; k < actives.length; k++)
		{
			if (actives[k] < minInvoke)
			{
				minInvoke = actives[k];
				rs = [];
				rs.push(servers[k]);
			}
			else if (actives[k] === minInvoke)
			{
				rs.push(servers[k]);
			}
		}
		const index = Math.floor(Math.random() * rs.length);
		const serverId = rs[index];
		client.laParam[serverType][serverId] += 1;
		utils.InvokeCallback(cb, null, serverId);
	}

    /**
     * Consistent-Hash algorithm for calculating server id.
     *
     * @param client {Object} rpc client.
     * @param serverType {String} rpc target serverType.
     * @param msg {Object} rpc message.
     * @param cb {Function} cb(err, serverId).
     */
	static ConsistentHashRoute(client, serverType, msg, cb)
    {
		const servers = client._station.serversMap[serverType];
		if (!servers || !servers.length)
		{
			utils.InvokeCallback(cb, new Error(`rpc servers not exist with serverType: ${serverType}`));
			return;
		}

		let con;
		if (!client.chParam)
		{
			client.chParam = {};
		}
		if (client.chParam[serverType])
		{
			con = client.chParam[serverType].consistentHash;
		}
		else
        {
			client.opts.station = client._station;
			con = new ConsistentHash(servers, client.opts);
		}
		const hashFieldIndex = client.opts.hashFieldIndex;
		const field = msg.args[hashFieldIndex] || JSON.stringify(msg);
		utils.InvokeCallback(cb, null, con.getNode(field));
		client.chParam[serverType] = {consistentHash: con};
	}
}

module.exports = {
	rr  : Router.RoundRobinRoute,
	wrr : Router.WeightRoundRobinRoute,
	la  : Router.LeastActiveRoute,
	ch  : Router.ConsistentHashRoute,
	rd  : Router.RdRoute,
	df  : Router.DefRoute
};