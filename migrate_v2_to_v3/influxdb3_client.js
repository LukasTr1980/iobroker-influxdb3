#!/usr/bin/env node
/**
 * Gedrosselter LP-Importer für InfluxDB 3.0
 *  • Batching, begrenzte Parallelität
 *  • optionale Pausen + Retry-Backoff
 *
 * Sichere Flags / ENV:
 *   --batch 1000        (BATCH_SIZE)     |  export BATCH_SIZE=1000
 *   --conc  1           (CONCURRENCY)    |  export CONCURRENCY=1
 *   --pause 1000         (THROTTLE_MS)    |  export THROTTLE_MS=1000
 */

const fs = require('fs');
const zlib = require('zlib');
const readline = require('readline');
const path = require('path');
const cliProg = require('cli-progress');
const { InfluxDBClient } = require('@influxdata/influxdb3-client');

/* ───── Runtime-Parameter (mit Defaults) ───────────────────────────────── */
function argOrEnv(flag, env, def) {
    const idx = process.argv.indexOf(`--${flag}`);
    return idx !== -1 ? +process.argv[idx + 1] : +(process.env[env] || def);
}
const BATCH_SIZE = argOrEnv('batch', 'BATCH_SIZE', 2000); // max. Zeilen pro Batch
const CONCURRENCY = argOrEnv('conc', 'CONCURRENCY', 2); // max. gleichzeitige Writes
const THROTTLE_MS = argOrEnv('pause', 'THROTTLE_MS', 1000);   // 0 = aus
const RETRIES = argOrEnv('retry', 'RETRIES', 3);     // pro Batch

/* ───── Config / Client ───────────────────────────────────────────────── */
function loadConfig() {
    const i = process.argv.indexOf('--config');
    let p = i !== -1 ? process.argv[i + 1] : process.env.CONFIG_PATH;
    if (!p) p = path.join(__dirname, '../config.json');
    return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : {};
}
const cfg = loadConfig() ?? {};
const influxCfg = cfg.influx ?? {};

const client = new InfluxDBClient({
    host: process.env.INFLUXDB3_HOST || influxCfg.host || 'http://localhost:8181',
    token: process.env.INFLUXDB3_TOKEN || influxCfg.token || '',
    database: process.env.INFLUXDB3_DATABASE || influxCfg.database || 'iobroker',
    timeout: influxCfg.timeout ?? 180_000
});

/* ───── kleine Helfer ─────────────────────────────────────────────────── */
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function writeWithRetry(lines) {
    let attempt = 0;
    while (true) {
        try { return await client.write(lines); }
        catch (e) {
            if (++attempt > RETRIES) throw e;
            const wait = 300 * attempt;                // linearer Backoff
            console.warn(`⚠️  Retry ${attempt}/${RETRIES} in ${wait} ms – ${e.message}`);
            await sleep(wait);
        }
    }
}

/* ───── Import­routine ────────────────────────────────────────────────── */
async function importFile(lpFile) {
    const totalBytes = fs.statSync(lpFile).size;
    const bar = new cliProg.SingleBar({
        format: 'Fortschritt |{bar}| {percentage}% | {value}/{total} B | Zeilen: {lines}',
        hideCursor: true
    }, cliProg.Presets.shades_classic);
    bar.start(totalBytes, 0, { lines: 0 });

    let bytesRead = 0, written = 0, buffer = [];
    const active = new Set();

    const fileStream = fs.createReadStream(lpFile);
    const input = lpFile.endsWith('.gz') ? fileStream.pipe(zlib.createGunzip()) : fileStream;

    fileStream.on('data', c => { bytesRead += c.length; bar.update(bytesRead, { lines: written }); });

    const rl = readline.createInterface({ input, crlfDelay: Infinity });

    const send = async () => {
        if (!buffer.length) return;
        const batch = buffer; buffer = [];
        const task = writeWithRetry(batch)
            .then(() => { written += batch.length; bar.update(bytesRead, { lines: written }); })
            .catch(e => { bar.stop(); console.error('❌  Unrecoverable:', e.message); process.exit(1); })
            .finally(() => active.delete(task));
        active.add(task);

        if (THROTTLE_MS) await sleep(THROTTLE_MS);
        if (active.size >= CONCURRENCY) await Promise.race(active);
    };

    for await (const line of rl) {
        if (!line.trim()) continue;
        buffer.push(line);
        if (buffer.length >= BATCH_SIZE) await send();
    }
    await send();                  // Rest
    await Promise.all(active);     // offene Writes beenden

    bar.update(totalBytes, { lines: written }); bar.stop();
    console.log(`\n✅  Fertig – ${written} Zeilen.`); await client.close?.();
}

/* ───── CLI-Entry ─────────────────────────────────────────────────────── */
(async () => {
    const lpFile = process.argv[2] || 'export.lp';
    if (!fs.existsSync(lpFile)) { console.error(`❌  Datei nicht gefunden: ${lpFile}`); process.exit(1); }
    console.log(`→ Import startet (Batch=${BATCH_SIZE}, Conc=${CONCURRENCY}, Pause=${THROTTLE_MS} ms)\n`);
    await importFile(lpFile);
})();
