// crawler/merge_shards_safe.js
// Usage:
// node crawler/merge_shards_safe.js ./shard_artifacts_dir ./site/index.ndjson ./site/last_indexed.json
//
// Input shard files: accepts either JSON array shard-output-*.json OR NDJSON shard-output-*.ndjson
// Output index: NDJSON (one JSON object per line) to avoid large in-memory arrays.
// After merging you may convert NDJSON -> JSON array offline if you need site/index.json

const fs = require('fs');
const path = require('path');
const readline = require('readline');

function loadJSON(p){ try { return JSON.parse(fs.readFileSync(p,'utf8')); } catch(e) { return null } }
function saveJSON(p, obj){ const tmp = p + '.tmp.' + process.pid; fs.mkdirSync(path.dirname(p), { recursive: true }); fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), 'utf8'); fs.renameSync(tmp, p); }

const artifactsDir = process.argv[2] || './shard_artifacts';
const outIndexNdjson = process.argv[3] || './site/index.ndjson'; // NDJSON output
const outLast = process.argv[4] || './site/last_indexed.json';
const MAX_URLS_PER_HOST = 10000;

if (!fs.existsSync(artifactsDir)) {
  console.error('Artifacts dir not found:', artifactsDir);
  process.exit(1);
}

// 1) load existing last_indexed (ok to load fully: it's a host->small-urls mapping)
const existingLast = loadJSON(outLast) || {};
const lastByHost = { ...existingLast };

// 2) build seen set from existingIndex (if existingIndex in NDJSON form)
// If existing index exists as JSON array and is small, you can convert it to NDJSON first.
// For safety we look for site/index.ndjson; if plain site/index.json exists and is small (<50MB) we'll load it.
const existingIndexJsonPath = path.join(path.dirname(outIndexNdjson), 'index.json');
const seen = new Set();

// helper to process JSON array file in streaming-friendly way if it's small
function seedSeenFromJsonArrayFile(filePath, maxSizeBytes = 50 * 1024 * 1024) {
  try {
    const stat = fs.statSync(filePath);
    if (stat.size > maxSizeBytes) {
      console.warn('[WARN] existing index.json is large (>50MB). Consider converting to NDJSON first to avoid memory usage.');
      return false;
    }
    const arr = loadJSON(filePath);
    if (!Array.isArray(arr)) return false;
    for (const it of arr) if (it && it.url) seen.add(it.url);
    return true;
  } catch (e) {
    return false;
  }
}

// if there is a NDJSON existing index, read it line by line to seed 'seen'
const existingNdjson = outIndexNdjson.replace(/\.ndjson$/, '.ndjson');
if (fs.existsSync(outIndexNdjson)) {
  // seed from the NDJSON file
  (async () => {
    const rl = readline.createInterface({ input: fs.createReadStream(outIndexNdjson, { encoding: 'utf8' }) });
    for await (const line of rl) {
      if (!line) continue;
      try {
        const obj = JSON.parse(line);
        if (obj && obj.url) seen.add(obj.url);
      } catch (e) { /* ignore malformed lines */ }
    }
    rl.close();
  })().catch(()=>{});
} else if (fs.existsSync(existingIndexJsonPath)) {
  // try to load small JSON array safely
  const ok = seedSeenFromJsonArrayFile(existingIndexJsonPath);
  if (!ok) {
    console.warn('[WARN] existing index.json too big or not parseable; continuing without seeding seen from it (duplicates may appear).');
  }
}

// 3) Open output NDJSON stream for merged index
fs.mkdirSync(path.dirname(outIndexNdjson), { recursive: true });
const outStream = fs.createWriteStream(outIndexNdjson + '.tmp', { flags: 'w', encoding: 'utf8' });

function writeNdjson(obj) {
  outStream.write(JSON.stringify(obj) + '\n');
}

// 4) iterate artifacts directory and process shard outputs and shard-last files one by one
(async function() {
  const files = fs.readdirSync(artifactsDir);
  // change ordering to process shard-output files first
  files.sort();
  for (const f of files) {
    const full = path.join(artifactsDir, f);
    if (f.startsWith('shard-output-') && f.endsWith('.json')) {
      // try to detect NDJSON vs JSON array:
      const firstChunk = fs.readFileSync(full, { encoding: 'utf8', flag: 'r' , length: 1024 }).slice(0, 1024);
      const looksNdjson = full.endsWith('.ndjson') || firstChunk.trim().startsWith('{');
      // Attempt to read line-by-line: if file is NDJSON, handle each line
      let processed = false;
      try {
        // try reading line-by-line: treat each line as JSON object
        const rl = readline.createInterface({ input: fs.createReadStream(full, { encoding: 'utf8' }) });
        for await (const line of rl) {
          if (!line) continue;
          try {
            const obj = JSON.parse(line);
            if (!obj || !obj.url) continue;
            if (seen.has(obj.url)) continue;
            seen.add(obj.url);
            writeNdjson(obj);
          } catch (e) {
            // not NDJSON line -> break and fall back
            // (we will fallback to JSON array below)
            processed = false;
            rl.close();
            throw new Error('not-ndjson');
          }
        }
        processed = true;
      } catch (e) {
        // not NDJSON or error reading as NDJSON -> fall back to parsing entire JSON file (array)
        try {
          const arr = loadJSON(full) || [];
          if (Array.isArray(arr)) {
            for (const obj of arr) {
              if (!obj || !obj.url) continue;
              if (seen.has(obj.url)) continue;
              seen.add(obj.url);
              writeNdjson(obj);
            }
            processed = true;
          }
        } catch (ee) {
          console.error('[ERR] failed to process shard output', full, ee && ee.stack ? ee.stack : ee);
        }
      }
      if (!processed) {
        console.warn('[WARN] shard output not processed (format not recognized):', full);
      } else {
        console.log('[MERGE] processed', f);
      }
    }
    // merge shard-last files into lastByHost
    if (f.startsWith('shard-last-') && f.endsWith('.json')) {
      try {
        const recs = loadJSON(full) || {};
        for (const host of Object.keys(recs)) {
          const r = recs[host] || { lastAt: 0, urls: [] };
          const existing = lastByHost[host] || { lastAt: 0, urls: [] };
          const mergedAt = Math.max(existing.lastAt || 0, r.lastAt || 0);
          const urls = Array.from(new Set([...(existing.urls||[]), ...(r.urls||[])]));
          if (urls.length > MAX_URLS_PER_HOST) urls.splice(MAX_URLS_PER_HOST);
          lastByHost[host] = { lastAt: mergedAt, urls };
        }
        console.log('[MERGE] shard-last merged', f);
      } catch (e) {
        console.error('[ERR] processing shard-last', full, e && e.stack ? e.stack : e);
      }
    }
  }

  // close and atomic-rename NDJSON output
  await new Promise((res) => outStream.end(res));
  fs.renameSync(outIndexNdjson + '.tmp', outIndexNdjson);

  // write updated last_indexed.json
  saveJSON(outLast, lastByHost);

  console.log('Merge complete. NDJSON index at:', outIndexNdjson);
  console.log('Hosts tracked:', Object.keys(lastByHost).length);
})();
