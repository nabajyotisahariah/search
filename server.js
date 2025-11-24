/* eslint-disable no-console */
require('dotenv').config();
const express = require('express');
const { MongoClient, ObjectId } = require('mongodb');
const { Client: ESClient } = require('@elastic/elasticsearch');

const PORT = process.env.PORT || 3000;

// MongoDB
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const MONGODB_DB = process.env.MONGODB_DB || 'catelog';
const MONGODB_COLLECTION = process.env.MONGODB_COLLECTION || 'catelog';

// Elasticsearch
const ELASTICSEARCH_NODE = process.env.ELASTICSEARCH_NODE || 'http://localhost:9200';
const ELASTICSEARCH_INDEX = process.env.ELASTICSEARCH_INDEX || 'catelog';
const ELASTICSEARCH_USERNAME = process.env.ELASTICSEARCH_USERNAME || undefined;
const ELASTICSEARCH_PASSWORD = process.env.ELASTICSEARCH_PASSWORD || undefined;

// Redis Cluster
const REDIS_CLUSTER_NODES = process.env.REDIS_CLUSTER_NODES || '';
const REDIS_USERNAME = process.env.REDIS_USERNAME || undefined;
const REDIS_PASSWORD = process.env.REDIS_PASSWORD || undefined;
const REDIS_TLS = /^true$/i.test(process.env.REDIS_TLS || '');
const REDIS_KEY_PREFIX = process.env.REDIS_KEY_PREFIX || 'catelog:';
const REDIS_TTL_SECONDS = Number(process.env.REDIS_TTL_SECONDS || 300);

const app = express();
app.use(express.json({ limit: '1mb' }));

let mongoClient;
let mongoDb;
let catelogCollection;
let es;
let redis=require('./redisCluster'); // Redis Cluster client

function getTenant(req) {
  return (req.headers['x-tenant-id'] && String(req.headers['x-tenant-id'])) ||
         (req.query.tenant && String(req.query.tenant)) ||
         null;
}

function toEsDoc(mDoc) {
  return {
    name: mDoc.name ?? null,
    description: mDoc.description ?? null,
    alias: mDoc.alias ?? null,
    tenantId: mDoc.tenantId ?? null,
    status: mDoc.status ?? null, // published | unpublished | draft
    createOn: mDoc.createOn ? new Date(mDoc.createOn) : null,
    modifiedOn: mDoc.modifiedOn ? new Date(mDoc.modifiedOn) : null,
  };
}

async function ensureEsIndex() {
  const exists = await es.indices.exists({ index: ELASTICSEARCH_INDEX });
  if (!exists) {
    await es.indices.create({
      index: ELASTICSEARCH_INDEX,
      settings: {
        number_of_shards: 1,
        number_of_replicas: 0,
      },
      mappings: {
        properties: {
          name: { type: 'text', fields: { keyword: { type: 'keyword' } } },
          description: { type: 'text' },
          alias: { type: 'text', fields: { keyword: { type: 'keyword' } } },
          tenantId: { type: 'keyword' },
          status: { type: 'keyword' },
          createOn: { type: 'date' },
          modifiedOn: { type: 'date' },
        },
      },
    });
    console.log(`Created index "${ELASTICSEARCH_INDEX}"`);
  }
}

app.get('/health', async (req, res) => {
  res.json({ ok: true });
});

// POST /documents - index a MongoDB doc (by _id) into Elasticsearch
app.post('/documents', async (req, res) => {
  try {
    const tenantId = getTenant(req);
    if (!tenantId) return res.status(400).json({ error: 'Missing tenant (X-Tenant-ID header or ?tenant=)' });

    const id = req.body?.id;
    if (!id) return res.status(400).json({ error: 'Body must include { "id": "<mongoId>" }' });

    let oid;
    try {
      oid = new ObjectId(String(id));
    } catch {
      return res.status(400).json({ error: 'Invalid MongoDB ObjectId' });
    }

    const mDoc = await catelogCollection.findOne({ _id: oid, tenantId });
    if (!mDoc) return res.status(404).json({ error: 'Document not found in MongoDB for this tenant' });

    const esDoc = toEsDoc(mDoc);
    await es.index({
      index: ELASTICSEARCH_INDEX,
      id: String(mDoc._id),
      document: esDoc,
      refresh: 'wait_for',
    });

    // Invalidate cache for this document if present
    if (redis) {
      const cacheKey = `${REDIS_KEY_PREFIX}doc:${tenantId}:${String(mDoc._id)}`;
      try {
        await redis.del(cacheKey);
      } catch (e) {
        console.error('Redis del error (POST /documents):', e);
      }
    }

    return res.status(201).json({ id: String(mDoc._id), indexed: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to index document' });
  }
});

// GET /search?q=...&tenant=... - search by text with tenant filter
app.get('/search', async (req, res) => {
  try {
    const tenantId = getTenant(req);
    if (!tenantId) return res.status(400).json({ error: 'Missing tenant (X-Tenant-ID header or ?tenant=)' });

    const q = (req.query.q && String(req.query.q)) || '';
    const from = Number.isFinite(Number(req.query.from)) ? Number(req.query.from) : 0;
    const size = Number.isFinite(Number(req.query.size)) ? Number(req.query.size) : 10;
    const status = req.query.status ? String(req.query.status) : undefined; // optional override

    // Try cache first
    const cacheKey = `${REDIS_KEY_PREFIX}search:${tenantId}:${from}:${size}:${status || ''}:${q}`;
    if (redis) {
      try {
        const cached = await redis.get(cacheKey);
        if (cached) {
          return res.json(JSON.parse(cached));
        }
      } catch (e) {
        console.error('Redis get error (GET /search):', e);
      }
    }

    const query = {
      bool: {
        must: q
          ? [{ multi_match: { query: q, fields: ['name^3', 'alias^2', 'description'] } }]
          : [{ match_all: {} }],
        filter: [
          { term: { tenantId } },
          ...(status ? [{ term: { status } }] : []),
        ],
      },
    };

    const result = await es.search({
      index: ELASTICSEARCH_INDEX,
      from,
      size,
      query,
      sort: [{ _score: 'desc' }, { modifiedOn: 'desc' }],
    });

    const hits = (result.hits?.hits || []).map(h => ({
      id: h._id,
      score: h._score,
      ...h._source,
    }));

    const responseBody = {
      total: typeof result.hits?.total === 'object' ? result.hits.total.value : (result.hits?.total || hits.length),
      hits,
    };

    // Populate cache
    if (redis) {
      try {
        await redis.set(cacheKey, JSON.stringify(responseBody), 'EX', REDIS_TTL_SECONDS);
      } catch (e) {
        console.error('Redis set error (GET /search):', e);
      }
    }

    return res.json(responseBody);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Search failed' });
  }
});

// GET /documents/:id - retrieve document from Elasticsearch (tenant-checked)
app.get('/documents/:id', async (req, res) => {
  try {
    const tenantId = getTenant(req);
    if (!tenantId) return res.status(400).json({ error: 'Missing tenant (X-Tenant-ID header or ?tenant=)' });

    const id = String(req.params.id);

    // Try cache first
    const cacheKey = `${REDIS_KEY_PREFIX}doc:${tenantId}:${id}`;
    if (redis) {
      try {
        const cached = await redis.get(cacheKey);
        if (cached) {
          const data = JSON.parse(cached);
          return res.json(data);
        }
      } catch (e) {
        console.error('Redis get error (GET /documents/:id):', e);
      }
    }

    // Get by ID then enforce tenant
    const r = await es.get({ index: ELASTICSEARCH_INDEX, id }).catch(e => {
      if (e.meta && e.meta.statusCode === 404) return null;
      throw e;
    });
    if (!r || !r.found) return res.status(404).json({ error: 'Not found' });

    const doc = r._source || {};
    if (doc.tenantId !== tenantId) return res.status(404).json({ error: 'Not found' });

    const responseBody = { id, ...doc };

    // Populate cache
    if (redis) {
      try {
        await redis.set(cacheKey, JSON.stringify(responseBody), 'EX', REDIS_TTL_SECONDS);
      } catch (e) {
        console.error('Redis set error (GET /documents/:id):', e);
      }
    }

    return res.json(responseBody);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to retrieve document' });
  }
});

// DELETE /documents/:id - remove from Elasticsearch (tenant-checked)
app.delete('/documents/:id', async (req, res) => {
  try {
    const tenantId = getTenant(req);
    if (!tenantId) return res.status(400).json({ error: 'Missing tenant (X-Tenant-ID header or ?tenant=)' });

    const id = String(req.params.id);

    // Verify tenant ownership first
    const r = await es.get({ index: ELASTICSEARCH_INDEX, id }).catch(e => {
      if (e.meta && e.meta.statusCode === 404) return null;
      throw e;
    });
    if (!r || !r.found) return res.status(404).json({ error: 'Not found' });
    if ((r._source?.tenantId) !== tenantId) return res.status(404).json({ error: 'Not found' });

    await es.delete({ index: ELASTICSEARCH_INDEX, id, refresh: 'wait_for' });
    // Invalidate cache
    if (redis) {
      const cacheKey = `${REDIS_KEY_PREFIX}doc:${tenantId}:${id}`;
      try {
        await redis.del(cacheKey);
      } catch (e) {
        console.error('Redis del error (DELETE /documents/:id):', e);
      }
    }
    return res.status(204).send();
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to delete document' });
  }
});

// Boot
(async () => {
  try {
    // Mongo
    mongoClient = new MongoClient(MONGODB_URI, { maxPoolSize: 10 });
    await mongoClient.connect();
    mongoDb = mongoClient.db(MONGODB_DB);
    catelogCollection = mongoDb.collection(MONGODB_COLLECTION);

    // Elasticsearch
    es = new ESClient({
      node: ELASTICSEARCH_NODE,
      ...(ELASTICSEARCH_USERNAME
        ? { auth: { username: ELASTICSEARCH_USERNAME, password: ELASTICSEARCH_PASSWORD } }
        : {}),
    });
    await ensureEsIndex();

    // Redis Cluster (optional)
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

    app.listen(PORT, () => {
      console.log(`API listening on http://localhost:${PORT}`);
    });
  } catch (err) {
    console.error('Startup error:', err);
    process.exit(1);
  }
})();

process.on('SIGINT', async () => {
  try {
    await mongoClient?.close();
  } finally {
    process.exit(0);
  }
});