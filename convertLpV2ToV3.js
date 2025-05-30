// convert_lp.js
// Aufruf:   node convert_lp.js [raw.lp] [export.lp]

const fs = require('fs');
const path = require('path');

const inFile = process.argv[2] || 'raw.lp';
const outFile = process.argv[3] || 'export.lp';

function transformLine(line) {
    line = line.trim();
    if (!line) return null;                   // Leere Zeile skippen

    /*  ------  Zerlegen in prefix + timestamp  ------  */
    const lastSp = line.lastIndexOf(' ');
    if (lastSp === -1) return null;           // Ungültig
    const ts = line.slice(lastSp + 1);             // 1746056…
    const prefix = line.slice(0, lastSp);              // meas field=val

    /*  ------  Zerlegen in measurement + fields  ------  */
    const firstSp = prefix.indexOf(' ');
    if (firstSp === -1) return null;          // kein Feldteil
    const measFull = prefix.slice(0, firstSp);          // javascript.0.Wetterstation.Aussentemperatur
    const fieldPart = prefix.slice(firstSp + 1);         // value=10.27

    /*  ------  Measurement kürzen  ------  */
    const meas = measFull.split('.').pop();   // → Aussentemperatur

    /*  ------  Feldnamen umbenennen  ------  */
    const fieldFixed = fieldPart.replace(/^value=/, 'wert=');

    /*  ------  Tags ergänzen + line zurückgeben  ------  */
    return `${meas},quelle=influxdbv2,trigger=manual_import ${fieldFixed} ${ts}`;
}

/* -----------  Datei lesen, Zeilen transformieren  ----------- */
const raw = fs.readFileSync(inFile, 'utf8')
    .split('\n')
    .map(transformLine)
    .filter(Boolean)              // ungültige/null-Zeilen raus
    .join('\n') + '\n';           // trailing newline

fs.writeFileSync(outFile, raw, 'utf8');
console.log(`✅  LP umgeschrieben → ${path.resolve(outFile)}`);
