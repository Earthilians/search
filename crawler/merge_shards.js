// crawler/merge_shards.js
// Usage:
// node crawler/merge_shards.js ./shard_artifacts_dir site/index.json site/last_indexed.json
//
// Expects artifacts directory contains shard-output-*.json and shard-last-*.json

const fs = require('fs');
const path = require('path');

function loadJSON(p){ try { return JSON.parse(fs.readFileSync(p,'utf8')); } catch(e) { return null } }
function saveJSON(p, obj){ const tmp = p + '.tmp.' + process.pid; fs.mkdirSync(path.dirname(p), { recursive: true }); fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), 'utf8'); fs.renameSync(tmp, p); }

const artifactsDir = process.argv[2] || './shard_artifacts';
const outIndex = process.argv[3] || './site/index.json';
const outLast = process.argv[4] || './site/last_indexed.json';
const MAX_URLS_PER_HOST = 10000;

const existingIndex = loadJSON(outIndex) || [];
const existingLast = loadJSON(outLast) || {};

const files = fs.existsSync(artifactsDir) ? fs.readdirSync(artifactsDir) : [];
const indexItems = [];
const lastByHost = { ...existingLast };

for(const f of files){
  if(f.startsWith('shard-output-') && f.endsWith('.json')){
    const arr = loadJSON(path.join(artifactsDir,f)) || [];
    for(const it of arr) indexItems.push(it);
  }
  if(f.startsWith('shard-last-') && f.endsWith('.json')){
    const recs = loadJSON(path.join(artifactsDir,f)) || {};
    for(const host of Object.keys(recs)){
      const r = recs[host];
      const existing = lastByHost[host] || { lastAt: 0, urls: [] };
      const mergedAt = Math.max(existing.lastAt || 0, r.lastAt || 0);
      const urls = Array.from(new Set([...(existing.urls||[]), ...(r.urls||[])]));
      if (urls.length > MAX_URLS_PER_HOST) urls.splice(MAX_URLS_PER_HOST);
      lastByHost[host] = { lastAt: mergedAt, urls };
    }
  }
}

// merge index: existingIndex first, then new items deduped by url
const seen = new Set();
for(const e of existingIndex){ if(e && e.url) seen.add(e.url); }
const merged = [...existingIndex];
for(const it of indexItems){
  if(!it || !it.url) continue;
  if(seen.has(it.url)) continue;
  seen.add(it.url);
  merged.push(it);
}

// write
saveJSON(outIndex, merged);
saveJSON(outLast, lastByHost);

console.log('Merged:', merged.length, 'pages; hosts tracked:', Object.keys(lastByHost).length);
