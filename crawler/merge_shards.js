// crawler/merge_shards.js
// Usage: node crawler/merge_shards.js <artifactsDir> <outIndex> <outLast>
// Example: node crawler/merge_shards.js shard_artifacts site/index.json site/last_indexed.json

const fs = require('fs');
const path = require('path');

function loadJSON(p){ try { return JSON.parse(fs.readFileSync(p,'utf8')); } catch(e){ return null; } }
function writeAtomicFile(p, dataStr) {
  const dir = path.dirname(p);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = path.join(dir, path.basename(p) + '.tmp.' + process.pid + '.' + Date.now());
  fs.writeFileSync(tmp, dataStr, 'utf8');
  try { fs.renameSync(tmp, p); } catch(e) {
    try { fs.copyFileSync(tmp, p); fs.unlinkSync(tmp); } catch(err){ throw err; }
  }
}

const artifactsDir = process.argv[2] || './shard_artifacts';
const outIndex = process.argv[3] || './site/index.json';
const outLast = process.argv[4] || './site/last_indexed.json';
const MAX_URLS_PER_HOST = 10000;

if (!fs.existsSync(artifactsDir)) {
  console.error('[FATAL] artifacts dir not found:', artifactsDir);
  process.exit(1);
}
console.log('[INFO] artifactsDir=', artifactsDir, 'outIndex=', outIndex, 'outLast=', outLast);

const existingIndex = loadJSON(outIndex) || [];
const existingLast = loadJSON(outLast) || {};
const seen = new Set();
if (Array.isArray(existingIndex)) for (const it of existingIndex) if (it && it.url) seen.add(it.url);
console.log('[INFO] seeded seen from existing index ->', seen.size);

const files = fs.readdirSync(artifactsDir).sort();
let mergedCount = 0;
const lastByHost = { ...existingLast };

// Prepare temp out stream for incremental writing
const outDir = path.dirname(outIndex);
fs.mkdirSync(outDir, { recursive: true });
const tmpOut = path.join(outDir, path.basename(outIndex) + '.tmp.' + process.pid + '.' + Date.now());
const outStream = fs.createWriteStream(tmpOut, { encoding: 'utf8' });
outStream.write('[\n');
let firstWritten = false;

function pushObj(obj) {
  if (!obj || !obj.url) return;
  if (seen.has(obj.url)) return;
  if (firstWritten) outStream.write(',\n');
  outStream.write(JSON.stringify(obj));
  firstWritten = true;
  seen.add(obj.url);
  mergedCount++;
}

for (const f of files) {
  const full = path.join(artifactsDir, f);
  try {
    const stat = fs.statSync(full);
    if (!stat.isFile()) continue;

    // handle shard-output files (.json or .ndjson)
    if (f.startsWith('shard-output-') && (f.endsWith('.json') || f.endsWith('.ndjson'))) {
      const raw = fs.readFileSync(full, 'utf8').trim();
      if (!raw) { console.log('[SKIP EMPTY]', f); continue; }

      // JSON array (.json)
      if (raw[0] === '[') {
        try {
          const arr = JSON.parse(raw);
          if (Array.isArray(arr)) for (const it of arr) pushObj(it);
          console.log('[MERGED] array', f, 'items=', Array.isArray(arr) ? arr.length : 0);
        } catch (e) {
          // fallback to NDJSON parse
          const lines = raw.split(/\r?\n/);
          let local = 0;
          for (const line of lines) {
            if (!line) continue;
            try { const it = JSON.parse(line); pushObj(it); local++; } catch(e2){ }
          }
          console.log('[MERGED] fallback-ndjson', f, 'lines=', local);
        }
        continue;
      }

      // NDJSON content (.ndjson or .json not array)
      const lines = raw.split(/\r?\n/);
      let local = 0;
      for (const line of lines) {
        if (!line) continue;
        try { const it = JSON.parse(line); pushObj(it); local++; } catch(e) {}
      }
      console.log('[MERGED] ndjson', f, 'linesAdded=', local);
      continue;
    }

    // handle shard-last files
    if (f.startsWith('shard-last-') && f.endsWith('.json')) {
      const recs = loadJSON(full) || {};
      let hostsMerged = 0;
      for (const host of Object.keys(recs)) {
        hostsMerged++;
        const r = recs[host] || { lastAt: 0, urls: [] };
        const existing = lastByHost[host] || { lastAt: 0, urls: [] };
        const mergedAt = Math.max(existing.lastAt || 0, r.lastAt || 0);
        const urls = Array.from(new Set([...(existing.urls||[]), ...(r.urls||[])]));
        if (urls.length > MAX_URLS_PER_HOST) urls.splice(MAX_URLS_PER_HOST);
        lastByHost[host] = { lastAt: mergedAt, urls };
      }
      console.log('[MERGED] last', f, 'hosts=', hostsMerged);
      continue;
    }

  } catch (e) {
    console.error('[ERR] processing artifact', f, e && e.stack ? e.stack : e);
    // continue with other files
  }
}

// finish JSON array and close stream
outStream.write('\n]\n');
outStream.end();

outStream.on('finish', () => {
  try {
    const data = fs.readFileSync(tmpOut, 'utf8');
    writeAtomicFile(outIndex, data);
    try { fs.unlinkSync(tmpOut); } catch(_) {}
    console.log('[OK] wrote', outIndex, 'mergedCount=', mergedCount);
  } catch (e) {
    console.error('[FATAL] could not finalize index.json', e && e.stack ? e.stack : e);
    process.exit(1);
  }

  try {
    writeAtomicFile(outLast, JSON.stringify(lastByHost, null, 2));
    console.log('[OK] wrote', outLast, 'hosts=', Object.keys(lastByHost).length);
  } catch (e) {
    console.error('[FATAL] could not write last_indexed.json', e && e.stack ? e.stack : e);
    process.exit(1);
  }

  console.log('Merge complete. total pages in index (approx):', seen.size);
  process.exit(0);
});

outStream.on('error', (err) => {
  console.error('[FATAL] outStream error', err && err.stack ? err.stack : err);
  process.exit(1);
});
