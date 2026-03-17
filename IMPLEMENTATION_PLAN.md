# Qlik MCP Server - WRITE Tools Implementation Plan

## 🎯 Ziel
Erweitere den qlik-mcp-server um WRITE-Funktionalität, um Measures, Variablen, Dimensionen und Objekte (Sheets/Charts) in QlikSense zu erstellen und zu bearbeiten.

## 📋 Phase 1: Grundlegende WRITE-Operationen

### 1.1 Measure Tools
- [ ] `create_measure` - Neues Master Measure erstellen
- [ ] `update_measure` - Bestehendes Measure ändern
- [ ] `delete_measure` - Measure löschen

### 1.2 Variable Tools
- [ ] `create_variable` - Neue Variable erstellen
- [ ] `update_variable` - Bestehende Variable ändern
- [ ] `delete_variable` - Variable löschen

### 1.3 Dimension Tools
- [ ] `create_dimension` - Neue Dimension erstellen
- [ ] `update_dimension` - Bestehende Dimension ändern
- [ ] `delete_dimension` - Dimension löschen

## 📋 Phase 2: Dashboard-Erstellung

### 2.1 Sheet Tools
- [ ] `create_sheet` - Neues Sheet erstellen
- [ ] `update_sheet` - Sheet-Eigenschaften ändern
- [ ] `delete_sheet` - Sheet löschen

### 2.2 Visualization Tools
- [ ] `create_visualization` - Chart/Table erstellen
- [ ] `add_object_to_sheet` - Objekt zu Sheet hinzufügen
- [ ] `delete_object` - Objekt löschen

## 📋 Phase 3: Script & App Management

- [ ] `set_script` - Load Script ändern
- [ ] `reload_app` - App neu laden
- [ ] `save_app` - App speichern

---

## 🔧 Implementierung

### Dateien zu ändern:
1. `src/qlik_client.py` - WebSocket Client erweitern
2. `src/tools.py` - Neue Tool-Definitionen
3. `src/server.py` - Tool-Registration

### API-Muster (aus Doku):

```python
# Measure erstellen
_send_request("CreateMeasure", app_handle, {
    "qProp": {
        "qInfo": {"qType": "measure"},
        "qMeasure": {
            "qLabel": "Name",
            "qDef": "Expression",
            "qGrouping": "N"
        }
    }
})

# Variable erstellen
_send_request("CreateVariableEx", app_handle, {
    "qProp": {
        "qInfo": {"qType": "variable"},
        "qName": "vName",
        "qDefinition": "Value"
    }
})

# Objekt erstellen (Sheet, Chart, etc.)
_send_request("CreateObject", app_handle, {
    "qProp": {
        "qInfo": {"qType": "sheet"},
        "qMetaDef": {"title": "Dashboard"}
    }
})
```

---

## ✅ Start: Phase 1.1 - create_measure

Beginne mit Implementierung von `create_measure` in `qlik_client.py` und `tools.py`.
