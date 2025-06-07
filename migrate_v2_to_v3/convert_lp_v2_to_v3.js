#!/usr/bin/env node

/**
 * convert.js
 * -----------------------------------------------
 * Liest eine LP-Datei (z.B. raw.lp) zeilenweise (streaming),
 * filtert nur value=-Zeilen und wandelt sie in InfluxDB-3-kompatibles Format um,
 * indem pro IoBroker-ID (dp.id) das Ziel-Measurement und alle Tags
 * aus config.json herangezogen werden.
 *
 * Usage:
 *   node convert.js [input.lp] [output.lp]
 *   - input.lp  (optional, Default: raw.lp)
 *   - output.lp (optional, Default: export.lp)
 *
 * Die Datei config.json muss im selben Verzeichnis liegen wie dieses Skript.
 */

const fs = require('fs');
const path = require('path');
const readline = require('readline');

// ----------------------------------------------------------------------------
// Utility: Zählt die Zeilen einer Datei (\n-Separator)
// ----------------------------------------------------------------------------
function countLines(filePath) {
    return new Promise((resolve, reject) => {
        let lines = 0;
        const stream = fs.createReadStream(filePath);
        stream.on('data', chunk => {
            for (let i = 0; i < chunk.length; i++) {
                if (chunk[i] === 10) lines++; // 10 => '\n'
            }
        });
        stream.on('error', reject);
        stream.on('end', () => resolve(lines));
    });
}

// ----------------------------------------------------------------------------
// 1) Utility: Funktionen zum Escapen (Line Protocol Rules)
//    – Measurement-Namen: Leerzeichen, Komma, Gleichheitszeichen mit Backslash escapen.
//    – Tag-Werte: Leerzeichen → \ , Komma → \,  Gleichheitszeichen → \=
// ----------------------------------------------------------------------------
const Escape = {
    lpMeasurement: (str) => str.replace(/([ ,=])/g, '\\$1'),
    lpTagValue: (str) => str.replace(/([ ,=])/g, '\\$1')
};

// ----------------------------------------------------------------------------
// 2) Konfigurationsdatei einlesen
// ----------------------------------------------------------------------------
let config;
try {
    const configPath = path.resolve(__dirname, '../config.json');
    const configText = fs.readFileSync(configPath, 'utf8');
    config = JSON.parse(configText);
} catch (err) {
    console.error('❌ Fehler beim Einlesen von config.json:', err.message);
    process.exit(1);
}

// ----------------------------------------------------------------------------
// 3) Auf Duplikate in config.datapoints.measurement prüfen
// ----------------------------------------------------------------------------
const allMeasurements = config.datapoints.map(dp => dp.measurement);
const duplicates = allMeasurements.filter((m, i) => allMeasurements.indexOf(m) !== i);
if (duplicates.length) {
    console.error(
        "❌ Duplikate bei 'measurement' in config.json gefunden:",
        [...new Set(duplicates)]
    );
    process.exit(1);
}

// ----------------------------------------------------------------------------
// 4) Aus config.datapoints ein Lookup-Objekt dpMap erstellen
//    Key   = IoBroker-ID (dp.id, z.B. "javascript.0.Wetterstation.Aussentemperatur")
//    Value = Objekt mit:
//              { measurement, tagsArray }
//
//    Dabei bereiten wir pro dpConfig ein Array `tagsArray` vor, das schon
//    alle "key=value"-Strings enthält (ohne source/trigger, diese kommen später).
// ----------------------------------------------------------------------------
const dpMap = {};
for (const dp of config.datapoints) {
    if (!dp.id || !dp.measurement) {
        console.warn(`⚠️ Ignoriere Datapoint ohne id oder measurement: ${JSON.stringify(dp)}`);
        continue;
    }

    // 4a) Measurement-Name escapen
    const escapedMeas = Escape.lpMeasurement(dp.measurement);

    // 4b) Tags sammeln (Schlüssel außer "id", "measurement", "minDelta" und "source")
    const tagsArray = [];
    for (const key of Object.keys(dp)) {
        if (key === 'id' || key === 'measurement' || key === 'minDelta' || key === 'source') continue;
        const val = dp[key];
        if (val === undefined || val === null || val === '') continue;
        // key sollte keine Sonderzeichen enthalten, aber wir escapen den Wert:
        const escapedVal = Escape.lpTagValue(String(val));
        tagsArray.push(`${key}=${escapedVal}`);
    }

    dpMap[dp.id] = {
        measurement: escapedMeas, // bereits escaped
        tagsArray                 // Array von "key=value"-Strings
    };
}

// ----------------------------------------------------------------------------
// 5) CLI-Parser: Eingabe-/Ausgabedatei (Default: raw.lp → export.lp)
// ----------------------------------------------------------------------------
const argv = process.argv.slice(2);
const inFile = argv[0] || 'raw.lp';
const outFile = argv[1] || 'export.lp';

const inPath = path.resolve(inFile);
const outPath = path.resolve(outFile);

// Existenz-Check für input-Datei
if (!fs.existsSync(inPath)) {
    console.error(`❌ Eingabedatei nicht gefunden: ${inPath}`);
    process.exit(1);
}

// Dateigröße des Eingabefiles in MB ausgeben
const inFileSizeMB = (fs.statSync(inPath).size / (1024 * 1024)).toFixed(2);
console.log(`ℹ️  ${inPath} hat eine Größe von ${inFileSizeMB} MB`);

// ----------------------------------------------------------------------------
// 6) transformLine: Verarbeitet eine einzelne LP-Zeile (String → String|null)
// ----------------------------------------------------------------------------
function transformLine(line) {
    line = line.trim();
    if (!line) return null; // Leerzeile überspringen

    // a) Timestamp abtrennen (alles rechts vom letzten Leerzeichen)
    const lastSpace = line.lastIndexOf(' ');
    if (lastSpace === -1) return null; // ungültige LP-Zeile
    const ts = line.slice(lastSpace + 1);  // Nanosekunden-Timestamp
    const prefix = line.slice(0, lastSpace);   // z.B. "id,alteTags value=0.5"

    // b) prefix in prefixMain (Messungsname + alteTags) und fieldPart (z.B. "value=0.5") splitten
    const firstSpace = prefix.indexOf(' ');
    if (firstSpace === -1) return null; // kein Feldteil
    const prefixMain = prefix.slice(0, firstSpace);
    const fieldPart = prefix.slice(firstSpace + 1);

    // c) Nur Zeilen mit "value=" akzeptieren
    if (!fieldPart.startsWith('value=')) return null;

    // d) Original-Measurement (alles vor dem ersten Komma) extrahieren
    const origMeas = prefixMain.split(',')[0];

    // e) In dpMap nachschauen
    const dpConfig = dpMap[origMeas];
    if (!dpConfig) return null; // kein Mapping → verworfen

    // f) Neuer Measurement-Name (bereits escaped) und vorgefertigte Tag-Liste
    const newMeas = dpConfig.measurement;
    const tagsList = [...dpConfig.tagsArray];

    // g) Füge die zwei festen Global-Tags an (source=influxdbv2, trigger=manual_import)
    tagsList.push('source=influxdbv2', 'trigger=manual_import');

    // h) Tag-String zusammensetzen
    //    Wenn tagsList leer ist, ergibt join("") eine leere Zeichenkette
    const tagsString = tagsList.join(',');

    // i) Feldteil unverändert übernehmen (z.B. "value=0.5")
    //    Für numerische Werte ist kein weiteres Escaping nötig.
    const fieldFixed = fieldPart;

    // j) Neue LP-Zeile:
    //    "<measurementEscaped>,tag1=val1,tag2=val2 value=... <timestamp>"
    //    Wenn tagsString leer ist, entfernt man das führende Komma.
    return `${newMeas}${tagsString ? ',' + tagsString : ''} ${fieldFixed} ${ts}`;
}

// ----------------------------------------------------------------------------
// 7) Streaming: Zeilenweise Einlesen mit readline, Ergebnis direkt in einen Write-Stream schreiben
// ----------------------------------------------------------------------------
(async () => {
    const totalLines = await countLines(inPath);
    console.log(`ℹ️  ${totalLines} Zeilen in ${inPath} gefunden.`);

    const reader = readline.createInterface({
        input: fs.createReadStream(inPath, { encoding: 'utf8' }),
        crlfDelay: Infinity
    });
    const writer = fs.createWriteStream(outPath, { encoding: 'utf8' });

    let count = 0;
    for await (const line of reader) {
        const transformed = transformLine(line);
        if (transformed) {
            writer.write(transformed + '\n');
            count++;
        }
    }

    writer.end(() => {
        console.log(`✅ ${count} value-Zeilen umgewandelt → ${outPath}`);
        const outFileSizeMB = (fs.statSync(outPath).size / (1024 * 1024)).toFixed(2);
        console.log(`ℹ️  ${outPath} hat eine Größe von ${outFileSizeMB} MB`);
    });
})();
