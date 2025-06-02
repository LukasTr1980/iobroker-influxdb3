// convert_only_values.js
// ------------------------------------------------------------------
// Benutzung: node convert_only_values.js [input.lp] [output.lp]
// ------------------------------------------------------------------

const fs = require('fs');
const path = require('path');

// ----------------- Hier anpassen: neuer Messungsname und Tags -----------------
const measurementName = 'aussen_temperatur';   // neuer InfluxDB-3-Messungsname

const SOURCE = 'influxdbv2';
const SENSOR_ID = 'curconsa_station';
const LOCATION = 'holzhuette_dach';
const PROCESSING = 'raw';
// -----------------------------------------------------------------------------

// Eingabe-/Ausgabedatei (Standard: raw.lp → export.lp)
const inFile = process.argv[2] || 'raw.lp';
const outFile = process.argv[3] || 'export.lp';

/**
 * transformLine(line)
 * - Trennt zuerst den Timestamp ab (am letzten Leerzeichen).
 * - Trennt dann den Feldteil (fieldPart) ab, überprüft, ob er mit "value=" beginnt.
 * - Lässt "value=" unverändert.
 * - Baut eine neue LP-Zeile:
 *     <measurementName>,<tags> <fieldPart> <ts>
 */
function transformLine(line) {
    line = line.trim();
    if (!line) return null;  // Leerzeile überspringen

    // 1) Timestamp abtrennen (alles rechts vom letzten Leerzeichen)
    const lastSpace = line.lastIndexOf(' ');
    if (lastSpace === -1) return null;           // keine gültige LP-Zeile
    const ts = line.slice(lastSpace + 1);     // Timestamp
    const prefix = line.slice(0, lastSpace);      // z.B. "javascript.0.Wetterstation.Aussentemperatur value=0.5"

    // 2) Feldteil (alles nach dem ersten Leerzeichen im prefix) abtrennen
    const firstSpace = prefix.indexOf(' ');
    if (firstSpace === -1) return null;           // kein Feldteil vorhanden
    const fieldPart = prefix.slice(firstSpace + 1);

    // 3) Nur wenn das Feld mit "value=" beginnt, weiterverarbeiten
    if (!fieldPart.startsWith('value=')) return null;

    // 4) "value=<Zahl>" unverändert lassen
    const fieldFixed = fieldPart;

    // 5) Feste Tags in der gewünschten Reihenfolge aneinanderhängen
    const tags = [
        `source=${SOURCE}`,
        `sensor_id=${SENSOR_ID}`,
        `location=${LOCATION}`,
        `processing=${PROCESSING}`,
        `trigger=manual_import`
    ].join(',');

    // 6) Neue InfluxDB-3-Zeile zurückgeben
    //    Format: <measurementName>,<tags> <fieldFixed> <ts>
    return `${measurementName},${tags} ${fieldFixed} ${ts}`;
}

/* --------------------------------------------------------------------
   Datei einlesen und jede Zeile transformieren
---------------------------------------------------------------------*/
const lines = fs.readFileSync(inFile, 'utf8').split('\n');
const transformed = lines.map(transformLine).filter(Boolean);

/* --------------------------------------------------------------------
   Zähler ausgeben und Ergebnis in die Ausgabedatei schreiben
---------------------------------------------------------------------*/
const count = transformed.length;
const lpOutput = transformed.join('\n') + '\n';
fs.writeFileSync(outFile, lpOutput, 'utf8');

console.log(`✅  Nur ${count} value-Zeilen umgewandelt → ${path.resolve(outFile)}`);
