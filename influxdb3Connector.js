/**
 * influxdb3_connector.js
 * ----------------------
 * Dieses Skript liest mehrere Datenpunkte aus ioBroker aus und schreibt sie in eine InfluxDB 3.x.
 * Die zu erfassenden Datenpunkte werden aus der config.json geladen.
 * Es unterstützt Direktschreibungen bei Wertänderungen, stündliche Schreibungen sowie eine Datei-basierte Fehler-Queue.
 *
 * Voraussetzungen:
 * - @influxdata/influxdb3-client installiert
 * - ioBroker-Umgebung mit Zugriff auf on(), getStateAsync() etc.
 * - config.json im CONFIG_PATH mit Feldern:
 *     influx: { host, token, database }
 *     datapoints: [
 *       { id: string, measurement: string, tagSource?: string },
 *       ...
 *     ]
 *
 * Funktionsübersicht:
 * - Konfiguration laden und validieren (inkl. detaillierter Prüfung jedes Datenpunkts)
 * - InfluxDB-Client initialisieren
 * - Verzeichnis und Fehler-Queue verwalten
 * - Direktes Schreiben bei Wertänderungen für jeden Datenpunkt
 * - Stündliches Schreiben des letzten Werts für jeden Datenpunkt
 * - Initial-Log beim Start
 */

const { InfluxDBClient } = require('@influxdata/influxdb3-client');
const fs = require('fs');
const path = require('path');

// Pfad zur Konfigurationsdatei
const CONFIG_PATH = '/opt/iobroker/influxdb3_connector/config.json';

// --- Konfiguration laden ---
let cfg;
try {
    cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
} catch (e) {
    console.error('Konnte config.json nicht laden:', e.message);
    process.exit(1);
}

// Validierung der Influx-Einstellungen
if (!cfg.influx || typeof cfg.influx.host !== 'string' || typeof cfg.influx.token !== 'string' || typeof cfg.influx.database !== 'string') {
    console.error('Influx-Konfiguration unvollständig oder fehlerhaft in config.json');
    process.exit(1);
}

// Validierung und Detailprüfung der datapoints
if (!Array.isArray(cfg.datapoints) || cfg.datapoints.length === 0) {
    console.error('Keine Datenpunkte in config.json definiert');
    process.exit(1);
}
for (const dp of cfg.datapoints) {
    if (!dp.id || typeof dp.id !== 'string') {
        console.error(`Ungültiger oder fehlender 'id' für einen Datenpunkt: ${JSON.stringify(dp)}`);
        process.exit(1);
    }
    if (!dp.measurement || typeof dp.measurement !== 'string') {
        console.error(`Ungültiges oder fehlendes 'measurement' für Datenpunkt '${dp.id}': ${JSON.stringify(dp)}`);
        process.exit(1);
    }
    if (dp.tagSource !== undefined && typeof dp.tagSource !== 'string') {
        console.error(`Ungültiger 'tagSource' für Datenpunkt '${dp.id}': Muss ein String sein.`);
        process.exit(1);
    }
}

// --- InfluxDB-Client ---
const client = new InfluxDBClient({
    host: cfg.influx.host,
    token: cfg.influx.token,
    database: cfg.influx.database,
});

// --- Fehler-Queue ---
const QUEUE_DIR = path.dirname(CONFIG_PATH);
const QUEUE_FILE = path.join(QUEUE_DIR, 'influxdb3_queue.json');
let queue = [];

// Verzeichnis und Queue-Datei sicherstellen
try {
    if (!fs.existsSync(QUEUE_DIR)) fs.mkdirSync(QUEUE_DIR, { recursive: true });
    if (!fs.existsSync(QUEUE_FILE)) fs.writeFileSync(QUEUE_FILE, '[]', 'utf8');
} catch (e) {
    console.error('Fehler beim Initialisieren der Queue-Datei:', e.message);
    process.exit(1);
}

// Queue laden
try {
    const raw = fs.readFileSync(QUEUE_FILE, 'utf8');
    queue = raw ? JSON.parse(raw) : [];
} catch (e) {
    console.error('Konnte Queue laden/parsen:', e.message);
    queue = [];
}

function saveQueue() {
    try {
        fs.writeFileSync(QUEUE_FILE, JSON.stringify(queue), 'utf8');
    } catch (e) {
        console.error('Fehler beim Speichern der Queue:', e.message);
        // Optional: process.exit(1);
    }
}

function enqueueValue(dp, value, source, ts) {
    queue.push({ dp, value, source, ts });
    saveQueue();
}

let isFlushing = false;
async function flushQueue() {
    if (isFlushing || queue.length === 0) return;
    isFlushing = true;
    console.log(`InfluxDB3: Schreibe ${queue.length} Queue-Einträge...`);
    const remaining = [];

    for (const { dp, value, source, ts } of queue) {
        const { measurement, tagSource } = dp;
        const tag = tagSource || 'iobroker';
        const line = `${measurement},quelle=${tag},trigger=${source} wert=${value} ${ts}`;
        try {
            await client.write(line);
        } catch (err) {
            console.error(`Write-Error für ${measurement}:`, err.message);
            remaining.push({ dp, value, source, ts });
        }
    }

    queue = remaining;
    saveQueue();
    console.log(`Queue verarbeitet. Verbleibend: ${queue.length}`);
    isFlushing = false;
}

// Initial und periodisch (60s)
flushQueue().catch(err => console.error('Initiales flushQueue fehlgeschlagen:', err.message));
setInterval(flushQueue, 60 * 1000);

// --- Tracking und Logging ---
// Map mit Schlüssel dp.id und Wert zuletzt gemessener Wert
const lastValues = new Map();

async function writeToInflux(dp, value, source, ts = Date.now() * 1e6) {
    const { id, measurement, tagSource } = dp;
    const tag = tagSource || 'iobroker';

    if (value == null || isNaN(value)) {
        console.warn(`Ungültiger Wert (${value}) für ${measurement}. Übersprungen.`);
        return;
    }

    const line = `${measurement},quelle=${tag},trigger=${source} wert=${value} ${ts}`;
    try {
        await client.write(line);
        console.log(`Geschrieben ${measurement} (${source}):`, value);
    } catch (err) {
        console.error(`Write-Error ${measurement} (${source}):`, err.message);
        enqueueValue(dp, value, source, ts);
    }
}

// Setup: Listener für Änderungen und Initial-Laden
for (const dp of cfg.datapoints) {
    on({ id: dp.id, change: 'ne', ack: true }, async obj => {
        const val = parseFloat(obj.state.val);
        lastValues.set(dp.id, val);
        const ts = obj.state.lc
            ? Math.floor(new Date(obj.state.lc).getTime() * 1e6)
            : Date.now() * 1e6;
        await writeToInflux(dp, val, 'change', ts);
    });

    // Initial laden
    (async () => {
        try {
            const state = await getStateAsync(dp.id);
            if (state?.val != null) {
                const v = parseFloat(state.val);
                lastValues.set(dp.id, v);
                console.log(`Initial geladen ${dp.measurement}:`, v);
            }
        } catch (e) {
            console.warn(`Initial-Lesen fehlgeschlagen für ${dp.measurement}:`, e.message);
        }
    })();
}

// Stündliches Schreiben prüfen
let lastHourUTC = new Date().getUTCHours();
setInterval(async () => {
    const now = new Date();
    if (now.getUTCMinutes() === 0 && now.getUTCHours() !== lastHourUTC) {
        lastHourUTC = now.getUTCHours();
        for (const dp of cfg.datapoints) {
            let val = lastValues.get(dp.id);
            if (val == null) {
                try {
                    const st = await getStateAsync(dp.id);
                    val = st?.val != null ? parseFloat(st.val) : undefined;
                    lastValues.set(dp.id, val);
                } catch (e) {
                    console.warn(`Stündlich: Lesen fehlgeschlagen ${dp.measurement}:`, e.message);
                }
            }
            if (val != null) {
                await writeToInflux(dp, val, 'hourly');
            }
        }
    }
}, 60 * 1000);

console.log('InfluxDB3 Connector gestartet. Überwacht:', cfg.datapoints.map(d => d.id).join(', '));
