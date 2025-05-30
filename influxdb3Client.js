#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const zlib = require('zlib');
const path = require('path');
const readline = require('readline');
const { InfluxDBClient } = require('@influxdata/influxdb3-client');

/* ---------- CLI-Parser ------------------------------------------------ */
function getOpts(argv) {
    const o = {
        lpFile: 'export.lp',
        host: process.env.INFLUXDB3_HOST,
        database: process.env.INFLUXDB3_DATABASE,
        token: process.env.INFLUXDB3_TOKEN,
        stream: false,
    };
    argv.slice(2).forEach(a => {
        if (a === '--stream') o.stream = true;
        else if (a.startsWith('--host=')) o.host = a.split('=')[1];
        else if (a.startsWith('--db=')) o.database = a.split('=')[1];
        else if (a.startsWith('--token=')) o.token = a.split('=')[1];
        else if (!o.fileSet) { o.lpFile = a; o.fileSet = true; }
        else console.warn(`⚠️  Extra-Argument ignoriert: ${a}`);
    });
    return o;
}

const opts = getOpts(process.argv);

/* ---------- Grundchecks ---------------------------------------------- */
if (!opts.host || !opts.token || !opts.database) {
    console.error('❌  ENV fehlend: INFLUXDB3_HOST, _TOKEN, _DATABASE (oder via --host/--db/--token).');
    process.exit(1);
}

const filePath = path.resolve(opts.lpFile);
if (!fs.existsSync(filePath)) {
    console.error(`❌  Datei nicht gefunden: ${filePath}`);
    process.exit(1);
}

/* ---------- Client-Init ---------------------------------------------- */
const client = new InfluxDBClient({
    host: opts.host,
    token: opts.token,
    database: opts.database,
});

/* ---------- Import ---------------------------------------------------- */
(async () => {
    console.log(`→ Import '${path.basename(filePath)}' nach '${opts.database}' @ ${opts.host}` +
        (opts.stream ? ' (Stream)' : ''));

    try {
        if (opts.stream) {
            const CHUNK = 1_000;
            let buffer = [];
            let written = 0;

            const inputStream = filePath.endsWith('.gz')
                ? fs.createReadStream(filePath).pipe(zlib.createGunzip())
                : fs.createReadStream(filePath);

            const rl = readline.createInterface({ input: inputStream, crlfDelay: Infinity });

            for await (const line of rl) {
                if (!line.trim()) continue;
                buffer.push(line);
                if (buffer.length === CHUNK) {
                    await client.write(buffer.join('\n'));
                    written += buffer.length;
                    buffer = [];
                }
            }
            if (buffer.length) {
                await client.write(buffer.join('\n'));
                written += buffer.length;
            }
            console.log(`✅  ${written} Zeilen importiert (Stream)`);

        } else {
            const raw = filePath.endsWith('.gz')
                ? zlib.gunzipSync(fs.readFileSync(filePath)).toString('utf8')
                : fs.readFileSync(filePath, 'utf8');

            if (!raw.trim()) {
                console.log('ℹ️  Datei ist leer – nichts zu importieren.');
                process.exit(0);
            }
            await client.write(raw);
            console.log(`✅  Import fertig (Sync) – ${raw.split('\n').filter(Boolean).length} Zeilen`);
        }

        await client.close?.();
        process.exit(0);

    } catch (err) {
        console.error('❌  Write-Fehler:', err.message || err);
        if (err.body) console.error('Influx-Antwort:', JSON.stringify(err.body));
        await client.close?.();
        process.exit(1);
    }
})();
