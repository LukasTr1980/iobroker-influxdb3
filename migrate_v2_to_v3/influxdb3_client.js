#!/usr/bin/env node

const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });

const fs = require('fs');
const zlib = require('zlib');
const readline = require('readline');
const { InfluxDBClient } = require('@influxdata/influxdb3-client');

/* ---------- CLI-Parser ------------------------------------------------ */
function getOpts(argv) {
    const o = {
        lpFile: 'export.lp',
        host: process.env.INFLUXDB3_HOST,
        database: process.env.INFLUXDB3_DATABASE,
        token: process.env.INFLUXDB3_TOKEN,
    };
    argv.slice(2).forEach(a => {
        if (a.startsWith('--host=')) o.host = a.split('=')[1];
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
    console.error('❌  Fehlende ENV: INFLUXDB3_HOST, _TOKEN oder _DATABASE (oder via --host/--db/--token).');
    process.exit(1);
}

const filePath = path.resolve(opts.lpFile);
if (!fs.existsSync(filePath)) {
    console.error(`❌  Datei nicht gefunden: ${filePath}`);
    process.exit(1);
}

/* ---------- InfluxDB-Client mit großzügigem Timeout ------------------ */
// Standard-Timeout ist manchmal zu klein, wir erlauben 3 Minuten pro Write
const client = new InfluxDBClient({
    host: opts.host,
    token: opts.token,
    database: opts.database,
    timeout: 180_000, // 3 Minuten
});

/* ---------- Hilfsfunktion für Pause ---------------------------------- */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/* ---------- Streaming‐Import mit adaptiver Chunk-Größe --------------- */
(async () => {
    console.log(`→ Adaptiver Streaming-Import '${path.basename(filePath)}' nach '${opts.database}' @ ${opts.host}`);

    // 1) Anfangs‐Chunk-Größe: extrem klein (100 Zeilen)
    let chunkSize = 100;
    let buffer = [];
    let writtenCount = 0;

    // 2) Eingabestream (entpackt, falls .gz)
    const inputStream = filePath.endsWith('.gz')
        ? fs.createReadStream(filePath).pipe(zlib.createGunzip())
        : fs.createReadStream(filePath);

    const rl = readline.createInterface({ input: inputStream, crlfDelay: Infinity });

    for await (const line of rl) {
        if (!line.trim()) continue;
        buffer.push(line);

        // 3) Sobald Puffer die aktuelle chunkSize erreicht, versuchen wir zu schreiben
        if (buffer.length === chunkSize) {
            const payload = buffer.join('\n');

            try {
                // 4a) Versuch, das Paket in InfluxDB zu schreiben
                await client.write(payload);
                writtenCount += buffer.length;
                console.log(`   ↳ ${writtenCount} Zeilen importiert (Chunk=${chunkSize})`);
                buffer = [];
                // 4b) Kurz pausieren, damit InfluxDB das WAL verarbeiten kann
                await sleep(200);

            } catch (err) {
                const errMsg = (err.body || err.message || '').toString();

                // 5) Reagiere nur auf „max request size exceeded“
                if (errMsg.includes('max request size') && chunkSize > 1) {
                    // Chunk war zu groß → halbieren und neu versuchen
                    const newSize = Math.floor(chunkSize / 2) || 1;
                    console.warn(
                        `⚠️  Chunk-Größe ${chunkSize} zu groß. Reduziere auf ${newSize}…`
                    );
                    chunkSize = newSize;
                    // Den gesamten Buffer behalten wir, damit wir in der nächsten Iteration
                    // erneut versuchen können, mit der kleineren Größe zu schreiben.
                    // Wir müssen sofort in die nächste Schleifen‐Runde gehen, ohne buffer zu leeren.
                    continue;

                } else {
                    // 6) Anderer Fehler (z.B. Timeout, Auth‐Fehler)
                    console.error('❌  Write-Fehler:', errMsg || err);
                    await client.close?.();
                    process.exit(1);
                }
            }
        }
    }

    // 7) Rest-Puffer (falls weniger als chunkSize Zeilen übrig)
    if (buffer.length > 0) {
        const payload = buffer.join('\n');
        try {
            await client.write(payload);
            writtenCount += buffer.length;
            console.log(`   ↳ ${writtenCount} Zeilen importiert (Rest-Puffer)`);
        } catch (err) {
            console.error('❌  Write-Fehler im Rest-Puffer:', (err.body || err.message) || err);
            await client.close?.();
            process.exit(1);
        }
    }

    console.log(`✅  Import abgeschlossen: insgesamt ${writtenCount} Zeilen geschrieben.`);
    await client.close?.();
    process.exit(0);

})().catch(e => {
    console.error('❌  Unerwarteter Fehler:', e);
    client.close?.().finally(() => process.exit(1));
});
