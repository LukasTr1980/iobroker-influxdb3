#!/usr/bin/env node

const fs = require('fs');
const zlib = require('zlib');
const readline = require('readline');
const { InfluxDBClient } = require('@influxdata/influxdb3-client');
const path = require('path');

const client = new InfluxDBClient({
    host: process.env.INFLUXDB3_HOST,
    token: process.env.INFLUXDB3_TOKEN,
    database: process.env.INFLUXDB3_DATABASE,
    timeout: 180_000
});

async function importLineByLine(lpFile) {
    let writtenCount = 0;

    const inputStream = lpFile.endsWith('.gz')
        ? fs.createReadStream(lpFile).pipe(zlib.createGunzip())
        : fs.createReadStream(lpFile);

    const rl = readline.createInterface({ input: inputStream, crlfDelay: Infinity });

    for await (const line of rl) {
        if (!line.trim()) continue;

        try {
            // Direkt jede einzelne LP-Zeile schreiben (als Array mit einem Element).
            await client.write([line]);
            writtenCount++;
            if (writtenCount % 1000 === 0) {
                console.log(`→ ${writtenCount} Zeilen geschrieben…`);
            }
        } catch (err) {
            console.error('❌  Fehler beim Schreiben der Zeile:', (err.body || err.message) || err);
            process.exit(1);
        }
    }

    console.log(`✅  Import fertig – insgesamt ${writtenCount} Zeilen geschrieben.`);
    await client.close?.();
}

(async () => {
    const lpFile = process.argv[2] || 'export.lp';
    if (!fs.existsSync(lpFile)) {
        console.error(`❌  Datei nicht gefunden: ${lpFile}`);
        process.exit(1);
    }
    console.log(`→ Starte Einzelzeilen‐Import für ${lpFile}`);
    await importLineByLine(lpFile);
})();
