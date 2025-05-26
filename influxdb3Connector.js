"use strict";
/**
 * influxdb3_connector.js (async/batch + smart‑hourly + fixes)
 * ----------------------------------------------------------
 *  • Vollständig asynchron (fs/promises) – kein blockierendes I/O.
 *  • Fehler‑Queue mit Exponential‑Back‑off (60 s → 10 min) und Batch‑Writes (max 500 Lines).
 *  • „Mindestens‑ein‑Write‑pro‑Stunde“‑Guard pro Datenpunkt.
 *  • Typ‑Robustheit & Escaping für das Influx Line‑Protocol.
 *  • Graceful Shutdown – Queue wird vor Exit abgearbeitet.
 *  • **NEU**
 *      – Korrektes Timestamp‑Handling (`lc` als ms‑Zahl oder ISO‑String).
 *      – Client‑Initialisierung erst nach erfolgreichem Config‑Load.
 *      – Robustes Laden der Queue‑Datei (korruptes JSON ⇒ leere Queue).
 */

const { InfluxDBClient } = require("@influxdata/influxdb3-client");
const fs = require("fs").promises;
const path = require("path");

// --------------------------------------------------
// Konfiguration laden (async IIFE) und danach main()
// --------------------------------------------------
const CONFIG_PATH = "/opt/iobroker/influxdb3_connector/config.json";
let cfg;
let client; // wird erst in main() initialisiert

(async () => {
    try {
        cfg = JSON.parse(await fs.readFile(CONFIG_PATH, "utf8"));
    } catch (e) {
        console.error("Konnte config.json nicht laden:", e.message);
        process.exit(1);
    }

    // --- Validierung -------------------------------------------------------
    function die(msg) {
        console.error(msg);
        process.exit(1);
    }

    if (
        !cfg.influx ||
        typeof cfg.influx.host !== "string" ||
        typeof cfg.influx.token !== "string" ||
        typeof cfg.influx.database !== "string"
    ) die("Influx-Konfiguration unvollständig oder fehlerhaft in config.json");

    if (!Array.isArray(cfg.datapoints) || cfg.datapoints.length === 0)
        die("Keine Datenpunkte in config.json definiert");

    for (const dp of cfg.datapoints) {
        if (!dp.id || typeof dp.id !== "string")
            die(`Ungültiger oder fehlender 'id': ${JSON.stringify(dp)}`);
        if (!dp.measurement || typeof dp.measurement !== "string")
            die(`Ungültiges oder fehlendes 'measurement' für ${dp.id}`);
        if (dp.tagSource !== undefined && typeof dp.tagSource !== "string")
            die(`'tagSource' für ${dp.id} muss String sein.`);
        // NEU ➟ minDelta ist optional, muss aber ≥ 0 sein
        if (dp.minDelta !== undefined &&
            (typeof dp.minDelta !== "number" || dp.minDelta < 0))
            die(`'minDelta' für ${dp.id} muss eine Zahl ≥ 0 sein.`);
    }

    // Erst NACH erfolgreicher Validierung starten
    await main();
})();

// --------------------------------------------------
// Globals & Helper
// --------------------------------------------------
const QUEUE_DIR = path.dirname(CONFIG_PATH);
const QUEUE_FILE = path.join(QUEUE_DIR, "influxdb3_queue.json");
let queue = [];

const MAX_BATCH = 500;
const BASE_FLUSH_MS = 60_000;
const MAX_FLUSH_MS = 600_000; // 10 Minuten
let flushDelay = BASE_FLUSH_MS;

// Maps: letzter Wert & letzter erfolgreicher Write (ms)
const lastValues = new Map();
const lastWritten = new Map();
const writtenValues = new Map(); // **neu**: zuletzt erfolgreich geschriebener numerischer Wert

// Escape für Line‑Protocol
const escapeLP = (s) => s.replace(/[ ,=]/g, "\\$&");

// --------------------------------------------------
// Queue‑Datei Handling
// --------------------------------------------------
async function exists(p) {
    try {
        await fs.access(p);
        return true;
    } catch {
        return false;
    }
}

async function initQueueFile() {
    try {
        await fs.mkdir(QUEUE_DIR, { recursive: true });
        if (!(await exists(QUEUE_FILE))) {
            await fs.writeFile(QUEUE_FILE, "[]", "utf8");
            queue = [];
        } else {
            const raw = await fs.readFile(QUEUE_FILE, "utf8");
            try {
                queue = raw ? JSON.parse(raw) : [];
                if (!Array.isArray(queue)) queue = [];
            } catch (e) {
                console.error("Queue-Datei korrupt – starte mit leerer Queue:", e.message);
                queue = [];
            }
        }
    } catch (e) {
        console.error("Fehler beim Initialisieren/Laden der Queue-Datei:", e.message);
        queue = [];
    }
}

async function saveQueue() {
    try {
        await fs.writeFile(QUEUE_FILE, JSON.stringify(queue), "utf8");
    } catch (e) {
        console.error("Fehler beim Speichern der Queue:", e.message);
    }
}

function enqueueValue(dp, value, source, ts) {
    queue.push({ dp, value, source, ts });
    saveQueue();
}

// --------------------------------------------------
// Flush‑Routine (Batch + Back‑off)
// --------------------------------------------------
async function flushQueue() {
    if (queue.length === 0) {
        flushDelay = BASE_FLUSH_MS;
        return scheduleNextFlush();
    }

    const batch = queue.splice(0, MAX_BATCH);
    const lines = batch.map(({ dp, value, source, ts }) => {
        const meas = escapeLP(dp.measurement);
        const tag = escapeLP(dp.tagSource || "iobroker");
        const trig = escapeLP(source);
        return `${meas},quelle=${tag},trigger=${trig} wert=${value} ${ts}`;
    });

    try {
        await client.write(lines.join("\n"));
        const now = Date.now();
        for (const { dp, value } of batch) {
            lastWritten.set(dp.id, now);
            writtenValues.set(dp.id, value);
        }
        flushDelay = BASE_FLUSH_MS;
    } catch (err) {
        console.error("Write-Error (Batch):", err.message);
        queue.unshift(...batch);
        flushDelay = Math.min(flushDelay * 2, MAX_FLUSH_MS);
    }

    await saveQueue();
    scheduleNextFlush();
}

function scheduleNextFlush() {
    setTimeout(flushQueue, flushDelay);
}

// --------------------------------------------------
// Schreiben einzelner Werte (mit Typ‑Check & Escaping)
// --------------------------------------------------
async function writeToInflux(dp, rawVal, source, ts = Date.now() * 1e6) {
    const num = Number(rawVal);
    if (!Number.isFinite(num)) {
        console.warn(`Ungültiger Wert (${rawVal}) für ${dp.measurement}`);
        return;
    }
    const meas = escapeLP(dp.measurement);
    const tag = escapeLP(dp.tagSource || "iobroker");
    const trig = escapeLP(source);
    const line = `${meas},quelle=${tag},trigger=${trig} wert=${num} ${ts}`;

    try {
        await client.write(line);
        const now = Date.now();
        lastWritten.set(dp.id, now);
        writtenValues.set(dp.id, num);
    } catch (err) {
        console.error(`Write-Error ${dp.measurement} (${source}):`, err.message);
        enqueueValue(dp, num, source, ts);
    }
}

// --------------------------------------------------
// Haupt‑Entry‑Point
// --------------------------------------------------
async function main() {
    // Client erst jetzt initialisieren → cfg ist garantiert verfügbar
    client = new InfluxDBClient({
        host: cfg.influx.host,
        token: cfg.influx.token,
        database: cfg.influx.database,
    });

    await initQueueFile();
    scheduleNextFlush();

    // Listener pro Datenpunkt
    for (const dp of cfg.datapoints) {
        on({ id: dp.id, change: "ne", ack: true }, async (obj) => {
            const val = obj?.state?.val;
            const lc = obj?.state?.lc;
            let tsNs;
            if (typeof lc === "number" && Number.isFinite(lc)) {
                tsNs = lc * 1e6; // ms → ns
            } else if (typeof lc === "string") {
                tsNs = Date.parse(lc) * 1e6; // ISO‑String
            } else {
                tsNs = Date.now() * 1e6;
            }
            lastValues.set(dp.id, val);

            // ➟ **minDelta-Prüfung** (falls konfiguriert)
            if (dp.minDelta !== undefined) {
                const currentNum = Number(val);
                const last = writtenValues.get(dp.id);
                if (Number.isFinite(currentNum) &&
                    last !== undefined &&
                    Math.abs(currentNum - last) < dp.minDelta) {
                    console.log(`minDelta skip ${dp.id}: |${currentNum} - ${last}| < ${dp.minDelta}`);
                    return;
                }
            }
            await writeToInflux(dp, val, "change", tsNs);
        });

        // Initialwert laden
        (async () => {
            try {
                const st = await getStateAsync(dp.id);
                if (st?.val !== undefined) {
                    lastValues.set(dp.id, st.val);
                    console.log(`Initial geladen ${dp.measurement}:`, st.val);
                }
            } catch (e) {
                console.warn(`Initial-Lesen fehlgeschlagen für ${dp.id}:`, e.message);
            }
        })();
    }

    // Smart Hourly Guard
    const ONE_HOUR = 60 * 60 * 1000;
    setInterval(async () => {
        const now = Date.now();
        for (const dp of cfg.datapoints) {
            const last = lastWritten.get(dp.id) || 0;
            if (now - last >= ONE_HOUR) {
                let val = lastValues.get(dp.id);
                if (val === undefined) {
                    try {
                        const st = await getStateAsync(dp.id);
                        val = st?.val;
                        lastValues.set(dp.id, val);
                    } catch (e) {
                        console.warn(`Hourly‑Guard Lesen fehlgeschlagen ${dp.id}:`, e.message);
                    }
                }
                if (val !== undefined) await writeToInflux(dp, val, "hourly-guard");
            }
        }
    }, 60_000);

    console.log("InfluxDB3 Connector gestartet. Überwacht:", cfg.datapoints.map((d) => d.id).join(", "));
}

// --------------------------------------------------
// Graceful Shutdown
// --------------------------------------------------
for (const sig of ["SIGINT", "SIGTERM"]) {
    process.on(sig, async () => {
        console.log(`\n${sig} empfangen → Flush & Exit …`);
        try {
            await flushQueue();
        } finally {
            process.exit(0);
        }
    });
}
