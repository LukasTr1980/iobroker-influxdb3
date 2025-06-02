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
 * - Trennt dann den Messungsnamen (prefix vor dem ersten Leerzeichen) vom Feldteil (fieldPart).
 * - Wenn fieldPart nicht mit "value=" beginnt, wird null zurückgegeben (Zeile verworfen).
 * - Ersetzt "value=" durch "wert=" und baut eine neue LP-Zeile
 *   <measurementName>,<alleTags> wert=<Zahl> <Timestamp>
 */
function transformLine(line) {
    line = line.trim();
    if (!line) return null;  // Leerzeile überspringen

    // 1) Timestamp abtrennen (alles rechts vom letzten Leerzeichen)
    const lastSpace = line.lastIndexOf(' ');
    if (lastSpace === -1) return null;  // keine gültige LP-Zeile
    const ts = line.slice(lastSpace + 1); // Timestamp
    const prefix = line.slice(0, lastSpace);  // z. B. "javascript.0.Wetterstation.Aussentemperatur value=0.5"

    // 2) Messungs-ID (alles vor dem ersten Leerzeichen) und Feldteil (alles danach) abtrennen
    const firstSpace = prefix.indexOf(' ');
    if (firstSpace === -1) return null;      // kein Feldteil vorhanden
    const fieldPart = prefix.slice(firstSpace + 1);

    // 3) Nur wenn das Feld mit "value=" beginnt, weiterverarbeiten
    if (!fieldPart.startsWith('value=')) return null;

    // 4) "value=<Zahl>" → "value=<Zahl>"
    const fieldFixed = fieldPart.replace(/^value=/, 'value=');

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
   Datei einlesen, Zeilen transformieren, nur gültige Zeilen behalten,
   und in die Ausgabedatei schreiben.
---------------------------------------------------------------------*/
const lpOutput = fs
    .readFileSync(inFile, 'utf8')
    .split('\n')
    .map(transformLine)
    .filter(Boolean)         // nur nicht-null-Zeilen behalten
    .join('\n') + '\n';

fs.writeFileSync(outFile, lpOutput, 'utf8');
console.log(`✅  Nur value-Zeilen umgewandelt → ${path.resolve(outFile)}`);
