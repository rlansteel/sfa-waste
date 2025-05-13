
---

# 🌍 Proyecto de Análisis y Visualización de Datos de Gestión de Residuos

Este proyecto implementa un workflow automatizado para **procesar, analizar y visualizar** datos sobre la gestión de residuos a nivel municipal y nacional.
Combina:

* 📊 Datos estructurados (CSV, JSON)
* 📘 Codebooks
* 🌐 Búsquedas web (Tavily)
* 🧠 Análisis por LLM (Anthropic Claude)
* ✅ Evaluación de la calidad de datos

Los resultados se presentan a través de una **interfaz web interactiva** y reportes HTML individuales.

---

## 🚦 Estado Actual del Proyecto (Post-Sprint de Calidad de Datos)

El proyecto ha evolucionado más allá de la simple generación de reportes:

* Cuenta con un **pipeline robusto** que procesa múltiples fuentes.
* Evalúa y **cuantifica la calidad de los datos** almacenados.
* La **UI refleja** estos puntajes con indicadores visuales y opciones de ordenamiento.

---

## 🏗️ Arquitectura y Componentes Clave

### 🧩 Orquestador (`main.py`)

* Script principal que coordina la ejecución de los SFAs.
* Genera reportes HTML por municipio.

### 🔄 SFA 1 - Procesador de CSV (`sfa_csv_processor.py v7.1`)

* Lee CSV guiado por archivos de configuración JSON.
* Usa **Claude** para mapear columnas a conceptos.
* Limpieza de datos, validación, imputación y cálculo de composición.
* Genera archivos JSON estructurados.

### 🌐 SFA 2 - Web Scraper (`sfa_tavily_scraper.py`)

* Búsqueda web vía API de Tavily por municipio.
* Guarda resultados en JSON, llamados por `main.py`.

### 📝 SFA 3 - Generador de Reportes HTML (`sfa_report_generator.py v2`)

* Usa plantilla `report_template.html`.
* Combina datos procesados + hallazgos web.
* Claude genera análisis estructurado en inglés con citas.

### 🗃️ Cargador de Base de Datos (`sfa_json_to_db.py v3.5`)

* Crea y puebla tablas en `waste_data.db`:

  * `countries`, `cities`, `country_measurements`, `city_measurements`
* Agrega columnas:

  * `data_quality_score`, `score_details_json`
* Restricción `UNIQUE (municipality, iso3c)` en `cities`.

### 🗺️ Enriquecedor de Geodatos (`sfa_enrich_geodata.py v3`)

* Geolocalización vía API de Nominatim (OpenStreetMap) para países y ciudades.

### 📊 Analizador de Calidad de Datos (`sfa_data_quality_analyzer.py v1`)

* NUEVO componente
* Calcula puntajes (0–100) para cada país y ciudad.
* Actualiza `waste_data.db` con `data_quality_score` y `score_details_json`.
* Genera rankings en JSON en `quality_analysis/`.

### 🧪 Preparador de Datos para UI (`prepare_ui_data.py v6.1`)

* Lee base de datos y genera archivos:

  * `index.json`
  * Archivos por país y ciudad
* Incluye `measurements_data` y `data_quality_score`.
* Archivos guardados en `ui_data_v6/`.

### 🌐 Interfaz de Usuario (UI)

* Archivos HTML: `index.html`, `country_view.html`, `city_view.html`
* Visualización con:

  * Mapas interactivos (Leaflet)
  * Tablas enriquecidas
  * Indicadores visuales de calidad
  * Filtros por puntaje

---

## ✅ Funcionalidad Validada

* Procesamiento completo desde CSV y codebooks a base de datos.
* Enriquecimiento de geodatos.
* ✅ NUEVO: Cálculo de calidad de datos.
* ✅ NUEVO: Rankings por calidad.
* Generación de archivos optimizados para UI.
* Visualización interactiva con puntajes de calidad.
* Reportes HTML individuales con análisis LLM y citas.
* Logging robusto (`workflow.log`) y manejo de errores.
* Configuración flexible vía archivos JSON y argumentos CLI.

---

## 🚀 Próximos Pasos (Sprints Futuros)

### 🤖 SFA 1 Inteligente (Adaptabilidad)

* Mejorar `sfa_csv_processor.py` para detectar y mapear columnas automáticamente usando LLMs.

### 📈 Mejoras en la Visualización de Calidad

* Mostrar `score_details_json` en modal o sección expandible.
* Visualizaciones gráficas del desglose del puntaje.

### 🔗 Integración de Funcionalidades Existentes

#### SFA 4 - Visualización

* Integrar `eda_analysis.py` y `sfa_visualization_generator.py` en los reportes HTML.

#### SFA 5 - Comparación Avanzada

* Integrar `sfa_comparison_generator.py` y permitir comparaciones dinámicas entre municipios y países.

---

## ⚙️ Configuración y Ejecución

### 🧬 Clonar/Descargar el Proyecto

```bash
git clone <repo_url>
cd <proyecto>
```

### 🐍 Crear Entorno Virtual

```bash
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .\.venv\Scripts\activate       # Windows
```

### 📦 Instalar Dependencias

```bash
pip install -r requirements.txt
```

Incluye: `pandas`, `rich`, `ftfy`, `anthropic`, `python-dotenv`, `unidecode`, `numpy`, `requests`, `matplotlib`, `seaborn`, etc.

### 🔐 Crear `.env` con Claves API

```dotenv
TAVILY_API_KEY=tvly-xxxxxxxxxx
ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxx
```

---

## 🛠️ Ejecución de Scripts

### 📥 Procesador CSV (SFA 1)

```bash
python sfa_csv_processor.py --input-csv data.csv --output-json salida.json --config-file config.json
```

### 🗃️ Cargador a Base de Datos

```bash
python sfa_json_to_db.py --city-json ciudades.json --country-json paises.json --db-file output/waste_data.db --drop-tables
```

### 📊 Analizador de Calidad de Datos

```bash
python sfa_data_quality_analyzer.py --db-file output/waste_data.db --output-dir quality_analysis/
```

### 🧪 Preparador de Datos UI

```bash
python prepare_ui_data.py --db-file output/waste_data.db --output-dir ui_data_v6/
```

### 🌍 Orquestador de Reportes HTML

```bash
python main.py --input-csv datos_municipios.csv --output-dir html_reports/
```

### 🌐 Visualizar UI

* Abrir `index.html` en el navegador.
* Si se requieren permisos de fetch:

```bash
python -m http.server
```

---

## 📚 Este README

Proporciona una **visión clara del estado actual** del sistema y su proyección futura.
El enfoque modular, el uso de LLMs y la evaluación de calidad posicionan al proyecto como una herramienta potente para la **gestión y análisis de datos ambientales**.

---

