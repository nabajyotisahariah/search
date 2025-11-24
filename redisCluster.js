// Add after Elasticsearch config
const REDIS_CLUSTER_NODES = process.env.REDIS_CLUSTER_NODES || '';
const REDIS_USERNAME = process.env.REDIS_USERNAME || undefined;
const REDIS_PASSWORD = process.env.REDIS_PASSWORD || undefined;
const REDIS_TLS = /^true$/i.test(process.env.REDIS_TLS || '');
const REDIS_KEY_PREFIX = process.env.REDIS_KEY_PREFIX || 'catelog:';
const REDIS_TTL_SECONDS = Number(process.env.REDIS_TTL_SECONDS || 300);

let redis; // Redis Cluster client
// Inside the IIFE boot block, before app.listen(...)
const nodesStr = REDIS_CLUSTER_NODES;
if (nodesStr) {
  const nodes = nodesStr.split(',').map(s => {
    const [host, port] = String(s).trim().split(':');
    return { host, port: Number(port || 6379) };
  });
  const Redis = require('ioredis');
  redis = new Redis.Cluster(nodes, {
    redisOptions: {
      ...(REDIS_USERNAME ? { username: REDIS_USERNAME } : {}),
      ...(REDIS_PASSWORD ? { password: REDIS_PASSWORD } : {}),
      ...(REDIS_TLS ? { tls: {} } : {}),
    },
  });
  redis.on('error', err => console.error('Redis error:', err));
  console.log(`Redis Cluster configured with ${nodes.length} node(s)`);
} else {
  console.log('REDIS_CLUSTER_NODES not set; cache disabled');
}