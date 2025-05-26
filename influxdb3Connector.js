"use strict";
/**
 * influxdb3_connector.js (async/batch version)
 * ------------------------------------------------
 * - Asynchrone FS‑Operationen (fs/promises)
 * - Dynamischer Back‑off beim Queue‑Flush
 * - Batch‑Writes an InfluxDB 3.x
 * - Typ‑ und Escaping‑Sicherheit
 */

const { InfluxDBClient } = require("@influxdata/influxdb3-client");
const fs = require("fs").promises;
const path = require("path");

// Pfad zur Konfigurationsdatei
const CONFIG_PATH = "/opt/iobroker/influxdb3_connector/config.json";

// --- Hilfsfunktionen ------------------------------------------------------
const esc = (s) => String(s).replace(/[ ,=]/g, "\\$&");

const fileExists = async (file) => {
    try {
        await fs.access(file);
        return true;
    } catch {
        return false;
    }
};

// --- Haupt‑IIFE -----------------------------------------------------------
(async () => {
    // ---------------- Konfiguration laden (nicht‑blockierend) --------------
    let cfg;
    try {
        const raw = await fs.readFile(CONFIG_PATH, "utf8");
        cfg = JSON.parse(raw);
    } catch (e) {
        console.error("Konnte config.json nicht laden:", e.message);
        process.exit(1);
    }

    // ---------------- Validierung -----------------------------------------
    if (!cfg.influx || typeof cfg.influx.host !== "string" || typeof cfg.influx.token !== "string" || typeof cfg.influx.database !== "string") {
        console.error("Influx‑Konfiguration unvollständig oder fehlerhaft in config.json");
        process.exit(1);
    }
    if (!Array.isArray(cfg.datapoints) || cfg.datapoints.length === 0) {
        console.error("Keine Datenpunkte in config.json definiert");
        process.exit(1);
    }
    for (const dp of cfg.datapoints) {
        if (!dp.id || typeof dp.id !== "string") {
            console.error(`Ungültiger oder fehlender 'id' für Datenpunkt: ${JSON.stringify(dp)}`);
            process.exit(1);
        }
        if (!dp.measurement || typeof dp.measurement !== "string") {
            console.error(`Ungültiges oder fehlendes 'measurement' für Datenpunkt '${dp.id}': ${JSON.stringify(dp)}`);
            process.exit(1);
        }
        if (dp.tagSource !== undefined && typeof dp.tagSource !== "string") {
            console.error(`Ungültiger 'tagSource' für Datenpunkt '${dp.id}': Muss ein String sein.`);
            process.exit(1);
        }
    }

    // ---------------- InfluxDB‑Client --------------------------------------
    const client = new InfluxDBClient({
        host: cfg.influx.host,
        token: cfg.influx.token,
        database: cfg.influx.database,
    });

    // ---------------- Fehler‑Queue (Datei‑basiert) -------------------------
    const QUEUE_DIR = path.dirname(CONFIG_PATH);
    const QUEUE_FILE = path.join(QUEUE_DIR, "influxdb3_queue.json");
    let queue = [];

    // Verzeichnis & Datei sicherstellen (asynchron)
    try {
        await fs.mkdir(QUEUE_DIR, { recursive: true });
        if (!(await fileExists(QUEUE_FILE))) {
            await fs.writeFile(QUEUE_FILE, "[]", "utf8");
        }
    } catch (e) {
        console.error("Fehler beim Initialisieren der Queue‑Datei:", e.message);
        process.exit(1);
    }

    // Queue laden
    try {
        const rawQ = await fs.readFile(QUEUE_FILE, "utf8");
        queue = rawQ ? JSON.parse(rawQ) : [];
    } catch (e) {
        console.error("Konnte Queue laden/parsen:", e.message);
        queue = [];
    }

    const saveQueue = async () => {
        try {
            await fs.writeFile(QUEUE_FILE, JSON.stringify(queue), "utf8");
        } catch (e) {
            console.error("Fehler beim Speichern der Queue:", e.message);
        }
    };

    const enqueueValue = (dp, value, source, ts) => {
        queue.push({ dp, value, source, ts });
        void saveQueue();
    };

    // ---------------- Queue‑Flush mit Back‑off & Batch ----------------------
    let isFlushing = false;
    const FLUSH_BASE_INTERVAL = 60_000; // 60 s
    const FLUSH_MAX_INTERVAL = 10 * 60_000; // 10 min
    let currentFlushInterval = FLUSH_BASE_INTERVAL;
    const MAX_BATCH = 500; // max Punkte pro Write‑Batch

    const flushQueue = async () => {
        if (isFlushing || queue.length === 0) return;
        isFlushing = true;

        let processed = 0;
        let hadError = false;

        while (queue.length > 0) {
            const slice = queue.splice(0, MAX_BATCH);
            const lines = slice.map(({ dp, value, source, ts }) => {
                const { measurement, tagSource } = dp;
                const tag = tagSource || "iobroker";
                return `${esc(measurement)},quelle=${esc(tag)},trigger=${esc(source)} wert=${value} ${ts}`;
            });

            try {
                await client.write(lines.join("\n"));
                processed += slice.length;
            } catch (err) {
                console.error("Write‑Batch‑Error:", err.message);
                // Batch zurück an Queue‑Anfang
                queue = slice.concat(queue);
                hadError = true;
                break; // weiteren Batches erst beim nächsten Flush versuchen
            }
        }

        await saveQueue();
        console.log(`Flush fertig. Erfolgreich: ${processed}, verbleibend: ${queue.length}`);

        // Back‑off‑Update
        currentFlushInterval = hadError && queue.length > 0
            ? Math.min(currentFlushInterval * 2, FLUSH_MAX_INTERVAL)
            : FLUSH_BASE_INTERVAL;

        isFlushing = false;
    };

    // Rekursives Scheduling mit dynamischem Intervall
    const scheduleFlush = () => {
        setTimeout(async () => {
            await flushQueue();
            scheduleFlush();
        }, currentFlushInterval);
    };

    // Initial
    void flushQueue();
    scheduleFlush();

    // ---------------- Wert‑Write‑Funktion ----------------------------------
    const writeToInflux = async (dp, value, source, ts = Date.now() * 1e6) => {
        const num = Number(value);
        if (!Number.isFinite(num)) {
            console.warn(`Ungültiger Wert (${value}) für ${dp.measurement}. Übersprungen.`);
            return;
        }

        const tag = dp.tagSource || "iobroker";
        const line = `${esc(dp.measurement)},quelle=${esc(tag)},trigger=${esc(source)} wert=${num} ${ts}`;

        try {
            await client.write(line);
            console.log(`Geschrieben ${dp.measurement} (${source}):`, num);
        } catch (err) {
            console.error(`Write‑Error ${dp.measurement} (${source}):`, err.message);
            enqueueValue(dp, num, source, ts);
        }
    };

    // ---------------- Tracking & Listener ----------------------------------
    const lastValues = new Map();

    for (const dp of cfg.datapoints) {
        // Listener für Wertänderungen
        on({ id: dp.id, change: "ne", ack: true }, async (obj) => {
            const val = obj?.state?.val;
            const ts = obj?.state?.lc
                ? Math.floor(new Date(obj.state.lc).getTime() * 1e6)
                : Date.now() * 1e6;
            lastValues.set(dp.id, val);
            await writeToInflux(dp, val, "change", ts);
        });

        // Initialwert laden
        (async () => {
            try {
                const state = await getStateAsync(dp.id);
                if (state?.val != null) {
                    lastValues.set(dp.id, state.val);
                    console.log(`Initial geladen ${dp.measurement}:`, state.val);
                }
            } catch (e) {
                console.warn(`Initial‑Lesen fehlgeschlagen für ${dp.measurement}:`, e.message);
            }
        })();
    }

    // ---------------- Stündliche Writes ------------------------------------
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
                        val = st?.val;
                        lastValues.set(dp.id, val);
                    } catch (e) {
                        console.warn(`Stündlich: Lesen fehlgeschlagen ${dp.measurement}:`, e.message);
                    }
                }
                if (val != null) {
                    await writeToInflux(dp, val, "hourly");
                }
            }
        }
    }, 60_000);

    // ---------------- Graceful Shutdown ------------------------------------
    const graceful = async (sig) => {
        console.log(`${sig} empfangen – Queue wird noch geschrieben …`);
        await flushQueue();
        process.exit(0);
    };
    process.on("SIGINT", () => graceful("SIGINT"));
    process.on("SIGTERM", () => graceful("SIGTERM"));

    console.log("InfluxDB3 Connector gestartet. Überwacht:", cfg.datapoints.map((d) => d.id).join(", "));
})();
