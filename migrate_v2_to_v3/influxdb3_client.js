#!/usr/bin/env node
/**
 * Importiert Line‑Protocol‑Dateien (LP / LP.GZ) in InfluxDB 3.0.
 *
 * ● Verbindungseinstellungen werden zuerst aus Umgebungsvariablen gelesen
 *   (INFLUXDB3_HOST, INFLUXDB3_TOKEN, INFLUXDB3_DATABASE).
 * ● Falls nicht gesetzt, werden die Werte aus `config.json` verwendet.
 *   Standard‑Pfad:  <SCRIPT‑VERZEICHNIS>/config.json
 *   → kann via   --config <pfad>   oder   ENV CONFIG_PATH   überschrieben werden.
 *
 * Aufrufbeispiel:
 *    node influxdb3_client.js export.lp
 *    node influxdb3_client.js export.lp --config /pfad/zu/config.json
 */

import fs from 'fs';
import zlib from 'zlib';
import readline from 'readline';
import path from 'path';
import { fileURLToPath } from 'url';
import { InfluxDBClient } from '@influxdata/influxdb3-client';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Lädt und parst eine JSON‑Konfigurationsdatei.
 * Priorität:
 *   1. CLI‑Argument  --config <pfad>
 *   2. ENV‑Variable  CONFIG_PATH
 *   3. Standard:     <SCRIPT‑DIR>/config.json
 */
function loadConfig() {
    // --config <path>
    const cliIndex = process.argv.indexOf('--config');
    let cfgPath = cliIndex !== -1 ? process.argv[cliIndex + 1] : process.env.CONFIG_PATH;
    if (!cfgPath) {
        cfgPath = path.join(__dirname, '../config.json');
    }

    if (!fs.existsSync(cfgPath)) {
        console.warn(`⚠️  Keine config.json gefunden unter ${cfgPath} – es werden ausschließlich ENV‑Variablen verwendet.`);
        return {};
    }

    try {
        return JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
    } catch (e) {
        console.error(`❌  Fehler beim Parsen der config.json (${cfgPath}):`, e.message);
        process.exit(1);
    }
}

const cfg = loadConfig();
const influxCfg = cfg.influx || {};

// Verbindung zu InfluxDB 3 herstellen (ENV > config.json > Fallback)
const client = new InfluxDBClient({
    host: process.env.INFLUXDB3_HOST || influxCfg.host || 'http://localhost:8181',
    token: process.env.INFLUXDB3_TOKEN || influxCfg.token || '',
    database: process.env.INFLUXDB3_DATABASE || influxCfg.database || 'iobroker',
    timeout: influxCfg.timeout ?? 180_000
});

async function importLineByLine(lpFile) {
    let written = 0;

    const input = lpFile.endsWith('.gz')
        ? fs.createReadStream(lpFile).pipe(zlib.createGunzip())
        : fs.createReadStream(lpFile);

    const rl = readline.createInterface({ input, crlfDelay: Infinity });

    for await (const line of rl) {
        if (!line.trim()) continue;
        try {
            await client.write([line]); // jede LP‑Zeile einzeln schreiben
            written++;
            if (written % 1000 === 0) console.log(`→ ${written} Zeilen geschrieben …`);
        } catch (err) {
            console.error('❌  Fehler beim Schreiben:', err.body || err.message || err);
            process.exit(1);
        }
    }

    console.log(`✅  Import abgeschlossen – insgesamt ${written} Zeilen.`);
    await client.close?.();
}

(async () => {
    const lpFile = process.argv[2] || 'export.lp';
    if (!fs.existsSync(lpFile)) {
        console.error(`❌  Datei nicht gefunden: ${lpFile}`);
        process.exit(1);
    }
    console.log(`→ Starte Import für ${lpFile}`);
    await importLineByLine(lpFile);
})();
