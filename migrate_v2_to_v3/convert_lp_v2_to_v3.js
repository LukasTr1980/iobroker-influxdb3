// convert_lp.js
// Aufruf: node convert_lp.js [raw.lp] [export.lp]

const fs = require('fs');
const path = require('path');

const inFile = process.argv[2] || 'raw.lp';
const outFile = process.argv[3] || 'export.lp';

function transformLine(line) {
    line = line.trim();
    if (!line) return null;                              // Leerzeile

    /* --- prefix (measurement + fields)  +  timestamp ------------------- */
    const lastSp = line.lastIndexOf(' ');
    if (lastSp === -1) return null;                      // Ungültig
    const ts = line.slice(lastSp + 1);
    const prefix = line.slice(0, lastSp);

    /* --- measurement  +  fieldPart ------------------------------------- */
    const firstSp = prefix.indexOf(' ');
    if (firstSp === -1) return null;                     // kein Feld
    const measFull = prefix.slice(0, firstSp);
    const fieldPart = prefix.slice(firstSp + 1);

    /* --- **Nur** Zeilen mit value= behalten ---------------------------- */
    if (!fieldPart.startsWith('value=')) return null;    // alles andere verwerfen

    /* --- measurement kürzen ------------------------------------------- */
    const meas = measFull.split('.').pop();              // Aussentemperatur

    /* --- Feldnamen umbenennen + Line zurück --------------------------- */
    const fieldFixed = fieldPart.replace(/^value=/, 'wert=');
    return `${meas},quelle=influxdbv2,trigger=manual_import ${fieldFixed} ${ts}`;
}

/* -------------------------------------------------------------------- */
const lp = fs.readFileSync(inFile, 'utf8')
    .split('\n')
    .map(transformLine)
    .filter(Boolean)              // nur gültige Zeilen
    .join('\n') + '\n';           // trailing newline

fs.writeFileSync(outFile, lp, 'utf8');
console.log(`✅  Nur value-Zeilen behalten → ${path.resolve(outFile)}`);
