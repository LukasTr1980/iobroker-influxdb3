const fs = require('fs');
const { parse } = require('csv-parse/sync');

const [, , inFile = 'influxDB2.csv', outFile = 'export.lp', dynFlag] = process.argv;
const useDynamicField = dynFlag === '--dynamic-field';

let raw;

try {
    raw = fs.readFileSync(inFile, 'utf8');
} catch (err) {
    console.error(`❌  Konnte ${inFile} nicht lesen: ${err.message}`);
    process.exit(1);
}

const lines = raw
    .split('\n')
    .map(l => l.trim())
    .filter(l => l && !l.startsWith('#') && l.split(',').length > 1);

if (lines.length < 2) {
    console.error('❌  Datei enthält nach dem Filtern zu wenig Daten.');
    process.exit(1);
}

let rows;
try {
    rows = parse(lines.join('\n'), { columns: true, skip_empty_lines: true });
} catch (err) {
    console.error(`❌  Fehler beim CSV-Parsen: ${err.message}`);
    process.exit(1);
}

const out = [];
rows.forEach(r => {
    if (!r._measurement || !r._value || !r._time) return;

    const ts = Date.parse(r._time);
    if (Number.isNaN(ts)) return;

    const fieldName = useDynamicField ? r._field || 'wert' : 'wert';
    out.push(
        `${r._measurement},quelle=influxdbv2,trigger=manual_import ` +
        `${fieldName}=${r._value} ${ts * 1e6}`
    );
});

if (out.length === 0) {
    console.warn('⚠️  Keine gültigen Datenzeilen erzeugt – nichts geschrieben.');
    process.exit(0);
}

try {
    fs.writeFileSync(outFile, out.join('\n'), 'utf8');
    console.log(`✅  ${out.length} Zeilen → ${outFile}`);
} catch (err) {
    console.error(`❌  Fehler beim Schreiben von ${outFile}: ${err.message}`);
    process.exit(1);
}