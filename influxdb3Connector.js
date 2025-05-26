/**
 * influxdb3_connector.js
 * ----------------------
 * Dieses Skript liest Temperaturwerte aus ioBroker aus und schreibt sie in eine InfluxDB 3.x.
 * Es unterstützt Direktschreibungen bei Wertänderungen, stündliche Schreibungen sowie eine Fehler-Queue zum Wiederholen
 * fehlgeschlagener Schreibversuche.
 *
 * Voraussetzungen:
 * - @influxdata/influxdb3-client installiert
 * - ioBroker-Umgebung mit Zugriff auf Konstanten wie on() und getStateAsync()
 * - config.json im CONFIG_PATH mit Feldern: influx.host, influx.token, influx.database
 *
 * Funktionsübersicht:
 * - Konfiguration laden und validieren
 * - InfluxDB-Client initialisieren
 * - Fehler-Queue verwalten (Datei-basiert)
 * - „Flush“ der Queue beim Start und alle 60 Sekunden
 * - Direktes Schreiben bei Wertänderungen
 * - Stündliches Schreiben einer aktuellen Messung
 * - Initial-Log beim Start
 */

const { InfluxDBClient } = require('@influxdata/influxdb3-client');
const fs = require('fs');

// Pfad zur Konfigurationsdatei
const CONFIG_PATH = '/opt/iobroker/influxdb3_connector/config.json';

let cfg;
try {
    // Konfigurationsdaten aus JSON-Datei einlesen
    cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
} catch (e) {
    console.error('Konnte config.json nicht laden:', e.message);
    process.exit(1);
}

// --- Konfiguration der InfluxDB-Verbindung ---
const INFLUX_HOST = cfg.influx.host;
const INFLUX_TOKEN = cfg.influx.token;
const INFLUX_DATABASE = cfg.influx.database;

// Konstanten für Datapoint-ID, Measurement-Name und Tag-Quelle
const DATAPOINT_ID = 'javascript.0.Wetterstation.Aussentemperatur';
const MEASUREMENT_NAME = 'Aussentemperatur';
const TAG_SOURCE = 'iobroker';

// --- InfluxDB-Client initialisieren ---
const client = new InfluxDBClient({
    host: INFLUX_HOST,
    token: INFLUX_TOKEN,
    database: INFLUX_DATABASE
});

// --- Fehler-Queue: Pfad und Initialisierung ---
const QUEUE_FILE = '/opt/iobroker/influxdb3_connector/influxdb3_queue.json';
let queue = [];

// Stelle sicher, dass die Queue-Datei existiert, sonst anlegen
try {
    if (!fs.existsSync(QUEUE_FILE)) {
        fs.writeFileSync(QUEUE_FILE, '[]', 'utf8');
        console.log('Queue-Datei angelegt:', QUEUE_FILE);
    }
} catch (e) {
    console.error('Fehler beim Anlegen der Queue-Datei:', e.message);
}

// Lade vorhandene Queue aus Datei
try {
    const raw = fs.readFileSync(QUEUE_FILE, 'utf8');
    queue = raw ? JSON.parse(raw) : [];
} catch (e) {
    console.error('Konnte Queue-Datei nicht laden/parsen:', e.message);
    queue = [];
}

/**
 * Speichert die aktuelle Queue in die Datei
 */
function saveQueue() {
    try {
        fs.writeFileSync(QUEUE_FILE, JSON.stringify(queue), 'utf8');
    } catch (e) {
        console.error('Fehler beim Speichern der Queue:', e.message);
    }
}

/**
 * Fügt einen fehlgeschlagenen Schreibversuch in die Queue ein und speichert sie
 * @param {number} value  Der zu schreibende Messwert
 * @param {string} source Kennzeichnung der Quelle (z.B. 'change' oder 'hourly')
 * @param {number} ts    Nanosekunden-Timestamp für den Eintrag
 */
function enqueueValue(value, source, ts) {
    queue.push({ value, source, ts });
    saveQueue();
}

let isFlushing = false;
/**
 * Versucht, alle Einträge in der Queue in InfluxDB zu schreiben.
 * Bei Fehlern bleiben Einträge erhalten und werden später erneut versucht.
 */
async function flushQueue() {
    if (isFlushing || queue.length === 0) return;
    isFlushing = true;
    console.log(`InfluxDB3: Versuche ${queue.length} Einträge aus der Queue zu schreiben...`);
    const remaining = [];
    for (const item of queue) {
        const { value, source, ts } = item;
        // Line Protocol: measurement,tag=wert field=value timestamp
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

// Initialer Versuch und danach alle 60 Sekunden
flushQueue().catch(err => console.error('Initiales flushQueue fehlgeschlagen:', err.message));
setInterval(flushQueue, 60 * 1000);

// --- Wert-Tracking ---
let lastValue;

/**
 * Schreibt einen Wert direkt in InfluxDB.
 * Bei Fehlern wird der Eintrag in die Queue übernommen.
 * @param {number} value  Gemessener Wert
 * @param {string} source Quelle für Tag (z.B. 'change', 'hourly')
 * @param {number} [ts]   Nanosekunden-Timestamp (Default: aktueller Zeitstempel)
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

// 1) Direktes Schreiben: abonnieren bei Zustandänderungen
on({ id: DATAPOINT_ID, change: 'ne', ack: true }, async obj => {
    const value = parseFloat(obj.state.val);
    lastValue = value;
    // Erzeuge Timestamp aus last change (lc) oder aktuellem Zeitpunkt
    const ts = obj.state.lc ? Math.floor(new Date(obj.state.lc).getTime() * 1e6) : Date.now() * 1e6;
    await writeToInflux(value, 'change', ts);
});

// 2) Stündliches Schreiben: jede Minute prüfen, ob die Stunde gewechselt hat
let lastHourWritten = new Date().getHours();
setInterval(async () => {
    const now = new Date();
    if (now.getMinutes() === 0 && now.getHours() !== lastHourWritten) {
        lastHourWritten = now.getHours();
        // Falls noch kein Wert vorliegt, versuche ihn aus ioBroker zu laden
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

// 3) Initial-Log: lade letzten Wert beim Start
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
