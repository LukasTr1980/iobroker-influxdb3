#!/usr/bin/env node
require('dotenv').config();           // ← .env laden

const fs = require('fs');
const path = require('path');
const { InfluxDB3Client } = require('@influxdata/influxdb3-client');

/* ----------  Mini-CLI-Parser  ------------------------------------------ */
function getOpts(argv) {
    const opts = {
        lpFile: 'export.lp',           // default
        host: process.env.INFLUXDB3_HOST,
        database: process.env.INFLUXDB3_DATABASE,
        token: process.env.INFLUXDB3_TOKEN,
        stream: false,
    };

    argv.slice(2).forEach(arg => {
        if (arg === '--stream') opts.stream = true;
        else if (arg.startsWith('--host=')) opts.host = arg.split('=')[1];
        else if (arg.startsWith('--db=')) opts.database = arg.split('=')[1];
        else if (arg.startsWith('--token=')) opts.token = arg.split('=')[1];
        else if (!opts._fileSet) { opts.lpFile = arg; opts._fileSet = true; }
        else console.warn(`⚠️  Extra Argument ignoriert: ${arg}`);
    });
    return opts;
}

const opts = getOpts(process.argv);
/* ----------------------------------------------------------------------- */

/* ----------  Grund-Checks  -------------------------------------------- */
if (!opts.host || !opts.token || !opts.database) {
    console.error(
        '❌  Fehlende Influx-ENV-Variablen!\n' +
        '    Benötigt: INFLUXDB3_HOST, INFLUXDB3_TOKEN, INFLUXDB3_DATABASE\n' +
        '    (oder per --host= --token= --db= übergeben)'
    );
    process.exit(1);
}

const filePath = path.resolve(opts.lpFile);
if (!fs.existsSync(filePath)) {
    console.error(`❌  Datei nicht gefunden: ${filePath}`);
    process.exit(1);
}

/* ----------  Client-Init  --------------------------------------------- */
const client = new InfluxDB3Client({
    host: opts.host,
    token: opts.token,
    database: opts.database,
});

/* ----------  Import-Routine  ------------------------------------------ */
(async () => {
    console.log(
        `Importiere '${path.basename(filePath)}' → DB '${opts.database}' @ ${opts.host}` +
        (opts.stream ? ' (Streaming-Modus)' : '')
    );

    try {
        if (opts.stream) {
            /* -------- Variante B – Stream & Batch -------------------------- */
            const CHUNK = 1000;              // Zeilen pro Batch-Write
            let buffer = [];
            let written = 0;

            const lineReader = require('readline').createInterface({
                input: fs.createReadStream(filePath, { encoding: 'utf8' }),
                crlfDelay: Infinity,
            });

            for await (const line of lineReader) {
                if (!line.trim()) continue;      // skip leer
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
            console.log(`✅  ${written} Zeilen importiert (Streaming)`);

        } else {
            /* -------- Variante A – Sync Read -------------------------------- */
            const lp = fs.readFileSync(filePath, 'utf8');
            if (!lp.trim()) {
                console.log('ℹ️  Datei ist leer – nichts zu importieren.');
                process.exit(0);
            }
            await client.write(lp);
            console.log('✅  Import fertig (Sync) –', lp.split('\n').filter(Boolean).length, 'Zeilen');
        }

        process.exit(0);

    } catch (err) {
        console.error('❌  Write-Fehler:', err.message || err);
        if (err.body) console.error('Influx-Response:', JSON.stringify(err.body));
        process.exit(1);
    }
})();
