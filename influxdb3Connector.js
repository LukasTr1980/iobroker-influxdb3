const { InfluxDBClient } = require('@influxdata/influxdb3-client');
const fs = require('fs');

const CONFIG_PATH = '/opt/iobroker/influxdb3_connector/config.json';

let cfg;
try {
    cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
} catch (e) {
    console.error('Konnte config.json nicht laden:', e.message);
    process.exit(1);
}

// --- Konfiguration ---
const INFLUX_HOST = cfg.influx.host;
const INFLUX_TOKEN = cfg.influx.token;
const INFLUX_DATABASE = cfg.influx.database;

// Datenpunkt- und Measurement-Konstanten
const DATAPOINT_ID = 'javascript.0.Wetterstation.Aussentemperatur';
const MEASUREMENT_NAME = 'Aussentemperatur';
const TAG_SOURCE = 'iobroker';

// --- InfluxDB-Client ---
const client = new InfluxDBClient({
    host: INFLUX_HOST,
    token: INFLUX_TOKEN,
    database: INFLUX_DATABASE
});

// --- Fehler-Queue ---
const QUEUE_FILE = '/opt/iobroker/influxdb3_connector/influxdb3_queue.json';
let queue = [];

// Sicherstellen, dass die Queue-Datei existiert (wenn nicht, neu anlegen)
try {
    if (!fs.existsSync(QUEUE_FILE)) {
        fs.writeFileSync(QUEUE_FILE, '[]', 'utf8');
        console.log('Queue-Datei angelegt:', QUEUE_FILE);
    }
} catch (e) {
    console.error('Fehler beim Anlegen der Queue-Datei:', e.message);
}

// Lade bestehende Queue
try {
    const raw = fs.readFileSync(QUEUE_FILE, 'utf8');
    queue = raw ? JSON.parse(raw) : [];
} catch (e) {
    console.error('Konnte Queue-Datei nicht laden/parsen:', e.message);
    queue = [];
}

function saveQueue() {
    try {
        fs.writeFileSync(QUEUE_FILE, JSON.stringify(queue), 'utf8');
    } catch (e) {
        console.error('Fehler beim Speichern der Queue:', e.message);
    }
}

function enqueueValue(value, source, ts) {
    queue.push({ value, source, ts });
    saveQueue();
}

let isFlushing = false;
async function flushQueue() {
    if (isFlushing || queue.length === 0) return;
    isFlushing = true;
    console.log(`InfluxDB3: Versuche ${queue.length} Einträge aus der Queue zu schreiben...`);
    const remaining = [];
    for (const item of queue) {
        const { value, source, ts } = item;
        const line = `${MEASUREMENT_NAME},quelle=${TAG_SOURCE} wert=${value} ${ts}`;
        try {
            await client.write(line);
        } catch (err) {
            console.error('Fehler beim Schreiben aus Queue, behalte Eintrag mit originalem Timestamp:', err.message);
            remaining.push(item);
        }
    }
    queue = remaining;
    saveQueue();
    console.log(`InfluxDB3: Queue-Verarbeitung beendet. ${queue.length} Einträge verbleiben.`);
    isFlushing = false;
}

// Initial und periodisch retry
flushQueue().catch(err => console.error('Initiales flushQueue fehlgeschlagen:', err.message));
setInterval(flushQueue, 60 * 1000);

// --- Wert-Tracking ---
let lastValue;

/**
 * Schreibt einen Wert in InfluxDB und enqueuet bei Fehler
 * @param {number} value  gemessener Wert
 * @param {string} source Quelle für Tag
 * @param {number} [ts]   Nanosekunden-Timestamp
 */
async function writeToInflux(value, source, ts = Date.now() * 1e6) {
    if (value === undefined || value === null || isNaN(value)) {
        console.warn(`Ungültiger Wert (${value}) von '${source}'. Schreiben übersprungen.`);
        return;
    }
    const line = `${MEASUREMENT_NAME},quelle=${TAG_SOURCE} wert=${value} ${ts}`;
    try {
        await client.write(line);
        console.log(`InfluxDB3 v3: Wert geschrieben (${source}):`, value);
    } catch (err) {
        console.error(`InfluxDB3 v3: Write error (${source}), enqueue:`, err.message);
        enqueueValue(value, source, ts);
    }
}

// 1) Echte Wertänderung abonnieren
on({ id: DATAPOINT_ID, change: 'ne', ack: true }, async obj => {
    const value = parseFloat(obj.state.val);
    lastValue = value;
    const ts = obj.state.lc ? Math.floor(new Date(obj.state.lc).getTime() * 1e6) : Date.now() * 1e6;
    await writeToInflux(value, 'change', ts);
});

// 2) Stundenweises Schreiben: prüfen jede Minute, ob Stunde gewechselt
let lastHourWritten = new Date().getHours();
setInterval(async () => {
    const now = new Date();
    if (now.getMinutes() === 0 && now.getHours() !== lastHourWritten) {
        lastHourWritten = now.getHours();
        if (lastValue === undefined) {
            try {
                const state = await getStateAsync(DATAPOINT_ID);
                lastValue = state && state.val != null ? parseFloat(state.val) : undefined;
            } catch {/* ignore */ }
        }
        if (lastValue != null) {
            const ts = Date.now() * 1e6;
            await writeToInflux(lastValue, 'hourly', ts);
        }
    }
}, 60 * 1000);

// 3) Initial-Log
(async () => {
    console.log('InfluxDB3 Skript gestartet. Überwacht:', DATAPOINT_ID);
    if (lastValue === undefined) {
        try {
            const state = await getStateAsync(DATAPOINT_ID);
            if (state && state.val != null) {
                lastValue = parseFloat(state.val);
                console.log(`Initialer Wert geladen:`, lastValue);
            }
        } catch (e) {
            console.warn(`Konnte initialen Wert nicht laden:`, e.message);
        }
    }
})();
