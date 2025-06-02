#!/usr/bin/env node

/**
 * convert.js
 * -----------------------------------------------
 * Liest eine LP-Datei (z.B. raw.lp), filtert nur value=-Zeilen
 * und wandelt sie in InfluxDB-3-kompatibles Format um, indem
 * pro IoBroker-ID (id) das Ziel-Measurement und alle Tags aus
 * config.json genommen werden.
 *
 * Usage:
 *   node convert.js [input.lp] [output.lp]
 *   - input.lp  (optional, Default: raw.lp)
 *   - output.lp (optional, Default: export.lp)
 *
 * Bevor du dieses Skript verwendest, lege bitte die Datei
 * config.json im gleichen Verzeichnis ab (wie oben beschrieben).
 */

const fs = require('fs');
const path = require('path');

// ----------------------------------------------------------------------------
// 1) Konfigurationsdatei einlesen
// ----------------------------------------------------------------------------
let config;
try {
    const configText = fs.readFileSync(path.resolve(__dirname, '../config.json'), 'utf8');
    config = JSON.parse(configText);
} catch (err) {
    console.error('❌  Fehler beim Einlesen von config.json:', err.message);
    process.exit(1);
}

// Prüfe auf duplikate in config.datapoints.measurement
const allMeasurements = config.datapoints.map(dp => dp.measurement);
const duplicates = allMeasurements.filter((m, i) => allMeasurements.indexOf(m) !== i);
if (duplicates.length) {
    console.error("❌  Duplikate bei 'measurement' in config.json gefunden:", [...new Set(duplicates)]);
    process.exit(1)
}

// ----------------------------------------------------------------------------
// 2) Aus config.datapoints ein Lookup-Objekt dpMap erstellen
//    Key   = IoBroker-ID (z.B. "javascript.0.Wetterstation.Aussentemperatur")
//    Value = Objekt mit allen Tags und measurement:
//            { measurement: "...", sensor_id: "...", tagSource: "...", location: "...", processing: "...", … }
// ----------------------------------------------------------------------------
const dpMap = {};
for (const dp of config.datapoints) {
    if (!dp.id || !dp.measurement) {
        console.warn(`⚠️  Ignoriere Datapoint ohne id oder measurement: ${JSON.stringify(dp)}`);
        continue;
    }
    // Kopiere alle Schlüssel außer "id" in ein neues Objekt
    const { id, ...rest } = dp;
    dpMap[id] = rest;
}

// ----------------------------------------------------------------------------
// 3) CLI-Parser: Eingabe-/Ausgabedatei (Default: raw.lp → export.lp)
// ----------------------------------------------------------------------------
const argv = process.argv.slice(2);
const inFile = argv[0] || 'raw.lp';
const outFile = argv[1] || 'export.lp';

const inPath = path.resolve(inFile);
const outPath = path.resolve(outFile);

// Existenz-Check für input-Datei
if (!fs.existsSync(inPath)) {
    console.error(`❌  Eingabedatei nicht gefunden: ${inPath}`);
    process.exit(1);
}

// ----------------------------------------------------------------------------
// 4) transformLine: Eine Funktion, die eine einzelne LP-Zeile verarbeitet.
// ----------------------------------------------------------------------------
function transformLine(line) {
    line = line.trim();
    if (!line) return null;  // Leerzeile überspringen

    // a) Timestamp abtrennen (alles rechts vom letzten Leerzeichen)
    const lastSpace = line.lastIndexOf(' ');
    if (lastSpace === -1) return null;  // Ungültige LP-Zeile
    const ts = line.slice(lastSpace + 1);   // der Nanosekunden-Timestamp
    const prefix = line.slice(0, lastSpace);    // z.B. "javascript.0.Wetterstation.Aussentemperatur,quelle=… value=0.5"

    // b) prefix in zwei Teile aufsplitten:
    //    prefixMain (Messungsname + alteTags) und fieldPart (z.B. "value=0.5")
    const firstSpace = prefix.indexOf(' ');
    if (firstSpace === -1) return null;  // kein Feldteil
    const prefixMain = prefix.slice(0, firstSpace);  // z.B. "javascript.0.Wetterstation.Aussentemperatur,quelle=…,trigger=…"
    const fieldPart = prefix.slice(firstSpace + 1);  // z.B. "value=0.5"

    // c) Nur Zeilen mit "value=" akzeptieren
    if (!fieldPart.startsWith('value=')) return null;

    // d) OriginalMeasurement (alles vor dem ersten Komma) extrahieren
    const origMeas = prefixMain.split(',')[0];  // z.B. "javascript.0.Wetterstation.Aussentemperatur"

    // e) Schau in dpMap nach, ob es eine Konfiguration für origMeas gibt
    const dpConfig = dpMap[origMeas];
    if (!dpConfig) {
        // Wenn kein Mapping existiert, verwirf die Zeile
        return null;
    }

    // f) Neue Measurement-Name aus dpConfig
    const newMeas = dpConfig.measurement;

    // g) Tags aus dpConfig: sensor_id, tagSource, location, processing, evtl. minDelta (ignoriert hier)
    //    Wir bauen ein Array von "key=value"-Strings, entsprechend allen Schlüsseln in dpConfig außer "measurement".
    const tagPairs = [];
    for (const key of Object.keys(dpConfig)) {
        if (key === 'measurement') continue; // nicht als Tag
        const val = dpConfig[key];
        // Wenn ein Tag-Wert undefiniert oder leer ist, überspringen wir ihn
        if (val === undefined || val === null || val === '') continue;
        // Alle Keys in der Konfig heißen so, wie wir die Tags wünschen:
        // z.B. "sensor_id", "tagSource", "location", "processing" oder auch "minDelta" (wird hier nicht verwendet)
        if (key === 'minDelta') continue;
        tagPairs.push(`${key}=${val}`);
    }

    // h) Füge die zwei festen Global-Tags an (source=influxdbv2 und trigger=manual_import)
    tagPairs.push(`source=influxdbv2`);
    tagPairs.push(`trigger=manual_import`);

    // i) Tag-String zusammensetzen (Reihenfolge: so, wie keys in dpConfig stehen, danach source+trigger)
    const tagsString = tagPairs.join(',');

    // j) Feldteil unverändert übernehmen (fieldPart z. B. "value=0.5")
    const fieldFixed = fieldPart;

    // k) Neue LP-Zeile zusammenbauen:
    //     <newMeas>,<tagsString> <fieldFixed> <ts>
    return `${newMeas},${tagsString} ${fieldFixed} ${ts}`;
}

// ----------------------------------------------------------------------------
// 5) Datei zeilenweise einlesen, transformieren und nur gültige Zeilen behalten
// ----------------------------------------------------------------------------
let rawLines;
try {
    rawLines = fs.readFileSync(inPath, 'utf8').split('\n');
} catch (err) {
    console.error('❌  Fehler beim Lesen der Eingabedatei:', err.message);
    process.exit(1);
}

const transformed = rawLines
    .map(transformLine)    // jede Zeile transformieren oder null retournieren
    .filter(Boolean);      // null-Ergebnisse entfernen

// ----------------------------------------------------------------------------
// 6) Ausgabe-Datei schreiben und Zähler ausgeben
// ----------------------------------------------------------------------------
const count = transformed.length;
const output = transformed.join('\n') + '\n';
fs.writeFileSync(outPath, output, 'utf8');
console.log(`✅  ${count} value-Zeilen umgewandelt → ${outPath}`);
