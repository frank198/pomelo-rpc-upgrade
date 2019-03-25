'use strict';
const crypto = require('crypto');

class ConsistentHash {
    constructor(nodes, opts)
    {
        this.opts = opts || {};
        this.replicas = this.opts.replicas || 100;
        this.algorithm = this.opts.algorithm || 'md5';
        this.station = this.opts.station;
        this.ring = {};
        this.keys = [];
        this.nodes = [];

        for (let i = 0; i < nodes.length; i++)
        {
            this.addNode(nodes[i]);
        }

        this.station.on('addServer', this.addNode.bind(this));
        this.station.on('removeServer', this.removeNode.bind(this));
    }

    addNode(node)
    {
        this.nodes.push(node);
        for (let i = 0; i < this.replicas; i++)
        {
            const key = hash(this.algorithm, `${node.id || node}:${i}`);
            this.keys.push(key);
            this.ring[key] = node;
        }
        this.keys.sort();
    }

    removeNode(node)
    {
        const nodeIndex = this.nodes.indexOf(node);
        if (nodeIndex >= 0)
        {
            this.nodes.splice(nodeIndex, 1);
        }

        for (let j = 0; j < this.replicas; j++)
        {
            const key = hash(this.algorithm, `${node.id || node}:${j}`);
            delete this.ring[key];
            const keyIndex = this.keys.indexOf(key);
            if (keyIndex > 0)
            {
                this.keys.splice(keyIndex, 1);
            }
        }
    }

    getNode(key)
    {
        if (getKeysLength(this.ring) === 0)
        {
            return 0;
        }
        const result = hash(this.algorithm, key);
        const pos = this.getNodePosition(result);
        return this.ring[this.keys[pos]];
    }

    getNodePosition(result)
    {
        let upper = getKeysLength(this.ring) - 1;
        let lower = 0;
        let idx = 0;
        let comp = 0;

        if (upper === 0)
        {
            return 0;
        }

        // binary search
        while (lower <= upper)
        {
            idx = Math.floor((lower + upper) / 2);
            comp = compare(this.keys[idx], result);

            if (comp === 0)
            {
                return idx;
            }
            else if (comp > 0)
            {
                upper = idx - 1;
            }
            else
            {
                lower = idx + 1;
            }
        }

        if (upper < 0)
        {
            upper = getKeysLength(this.ring) - 1;
        }

        return upper;
    }
}

module.exports = function(nodes, opts)
{
    return new ConsistentHash(nodes, opts);
};

const getKeysLength = function(map)
{
    return Object.keys(map).length;
};

const hash = function(algorithm, str)
{
    return crypto.createHash(algorithm).update(str).digest('hex');
};

const compare = function(v1, v2)
{
    return v1 > v2 ? 1 : v1 < v2 ? -1 : 0;
};
