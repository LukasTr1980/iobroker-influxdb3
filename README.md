# InfluxDB 3 Connector für ioBroker

Ein JavaScript‑Connector für **InfluxDB 3.0** sowie ein Konverter, der Datenbanken von **InfluxDB 2.x** auf das **3.x**‑Format migriert.

## Inhaltsverzeichnis

* [Projektübersicht](#projektübersicht)
* [Voraussetzungen](#voraussetzungen)
* [Installation](#installation)
* [Verwendung](#verwendung)
* [Migration 2.x → 3.x](#migration-2x-→-3x)

---

## Projektübersicht

Dieser Connector schreibt ioBroker‑Daten (Zustände, Logs, Metriken) direkt in InfluxDB 3.0.
Das Repository enthält außerdem ein Skript zur Migration bestehender InfluxDB‑2.x‑Datenbanken.

* **Connector**: Echtzeit‑Write von ioBroker ➜ InfluxDB 3.0
* **Features**:
  * Zustände, Logs, Metriken
  * Konfiguration über JSON
  * Unterstützung für InfluxDB 3.x
  * Schreibt Werte nur bei Änderungen
  * Schreibt Werte nur wenn minDelta überschritten wird, konfigurierbar in der config.json
  * Schreibt Werte immer einmal pro Stunde, egal ob Änderungen oder nicht (nützlich für Grafana)
* **Konverter**: Vollständige Daten‑ & Metadatenübernahme von 2.x‑Buckets ➜ 3.x‑Buckets

---

## Voraussetzungen

* ioBroker‑Installation (Node.js ≥ 20)
* Laufender InfluxDB 3‑Server (lokal oder remote)
* `sudo`‑Rechte auf dem Host
* Node.js & npm

---

## Installation

```bash
# 1. Verzeichnis anlegen
sudo mkdir -p /opt/iobroker/influxdb3_connector

# 2. Wechseln
cd /opt/iobroker/influxdb3_connector

# 3. Repository klonen
sudo git clone https://github.com/LukasTr1980/iobroker-influxdb3.git .
#  - oder: Dateien manuell hierher kopieren

# 4. Abhängigkeiten installieren
In iobroker -> Instanzen -> Javascript‑Adapter -> Einstellungen zusätzliche NPM-Module installieren: @influxdata/influxdb3-client

# 5. Konfig kopieren/erstellen
ACHTUNG: Die Konfigurationsdatei `config.sample.json` öffnen und genau anpassen!

# 6. Anschließend Konfigurationsdatei umbenennen
mv config.sample.json ./config.json
```

---

## Verwendung

Den Code in den Javascript‑Adapter von ioBroker einfügen und starten.

---

## OPTIONAL UND NUR MIT BACKUP VON INFLUXDB 2.x!
**Aktuell eingeschränkt und experimentell**
## Migration 2.x → 3.x

Measurements von InfluxDB 2.x exportieren mittels dieses Strings:

```
influxd inspect export-lp --bucket-id <BUCKERT-ID> --engine-path /var/lib/influxdb2/engine --measurement measurement1,measurement2 --output-path /root/raw.lp
```

2. Migration starten:

```bash
node convert_lp_v2_to_v3.js
```

Exportiert in eine neue Datei `export.lp`, die dann in InfluxDB 3.x importiert werden kann.

3. Import in InfluxDB 3.x:

```bash
node influxdb3_client.js
```

4. Nach Abschluss Konsistenz prüfen (Influx UI/CLI).