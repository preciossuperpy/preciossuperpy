# SUPERMERCADOS_PRECIOS – Ingesta diaria + Tablero

Este repo contiene:
- **GitHub Action** programado para correr **cada día**: lee todos los CSVs de una carpeta de Google Drive y los agrega a una hoja de Google Sheets (histórico acumulado).
- **Página web** (en `docs/`) que **embebe** un tablero dinámico publicado como **Google Apps Script Web App**.
- Código de ejemplo de **Apps Script** (en `appscript/`) para que publiques tu propio dashboard.

---

## 1) Variables y secretos del repositorio

Ve a **Settings → Secrets and variables → Actions** y crea:

### Secrets
- `SERVICE_ACCOUNT_JSON`: **contenido completo** del JSON del Service Account (no el archivo).

### Variables
- `DRIVE_FOLDER_ID`: `1Ot0DsSvA9isa30-wxspIO_TbJPrD3b_c`
- `SPREADSHEET_URL`: `https://docs.google.com/spreadsheets/d/1plZ1LzHu2W2TrbV7wXPueWsO2g4dFRyUdpxXIUE5ns8`
- (opcional) cambia `DATA_WS_NAME` y `LOG_WS_NAME` editando el workflow si quieres otros nombres.

> Asegúrate de **compartir** la carpeta de Drive y el Spreadsheet con el **correo** del Service Account.

---

## 2) Ingesta diaria (GitHub Actions)

- Archivo: `.github/workflows/ingesta.yml`
- Corre todos los días a las **13:30 UTC** (≈ **09:30 America/Asuncion**; ajusta el `cron` si cambia DST).
- También puedes ejecutarlo manualmente desde **Actions → Run workflow**.

El job instala dependencias, ejecuta `pipeline_ingesta.py` y actualiza las worksheets:
- Datos: `precios_supermercados`
- Log: `ingestas_archivos`

---

## 3) Ejecutar en local

```bash
python -m venv .venv && source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt

# Usando archivo:
set GOOGLE_APPLICATION_CREDENTIALS=./service_account.json  # Windows
export GOOGLE_APPLICATION_CREDENTIALS=./service_account.json  # Linux/Mac

# o con secret inline:
set SERVICE_ACCOUNT_JSON={...}
export SERVICE_ACCOUNT_JSON='{...}'

set DRIVE_FOLDER_ID=1Ot0DsSvA9isa30-wxspIO_TbJPrD3b_c
set SPREADSHEET_URL=https://docs.google.com/spreadsheets/d/1plZ1LzHu2W2TrbV7wXPueWsO2g4dFRyUdpxXIUE5ns8

python pipeline_ingesta.py
```

---

## 4) Tablero (Google Apps Script)

En `appscript/` tienes:
- `Code.gs` (backend): lee la hoja `precios_supermercados` y expone datos al frontend.
- `index.html` (frontend): dibuja gráficas con **Google Charts**.

### Pasos
1. Abre https://script.google.com y crea un proyecto.
2. Crea archivos `Code.gs` e `index.html` con el contenido del folder `appscript/`.
3. Edita `Code.gs` y pon tu `SHEET_ID` (el ID del spreadsheet).
4. **Deploy → New deployment → Web app**. En `Who has access` elige tu preferencia (público o dominio).
5. Copia la **URL de la Web App**.

---

## 5) Web estática (GitHub Pages) que **embebe** el tablero

- Edita `docs/index.html` y reemplaza `APPS_SCRIPT_WEBAPP_URL` por la URL del paso anterior.
- Este repo incluye el workflow `.github/workflows/gh-pages.yml` para publicar `docs/` automáticamente a **GitHub Pages** cuando hagas push a `main`.
- Alternativa: en **Settings → Pages**, elige **Build and deployment → GitHub Actions** (o Source: `docs/`).

La página usará un `<iframe>` responsivo para mostrar tu tablero.

---

## 6) Estructura

```
.
├─ pipeline_ingesta.py
├─ requirements.txt
├─ docs/
│  └─ index.html           # web estática que embebe tu Apps Script
├─ appscript/
│  ├─ Code.gs              # servidor Apps Script
│  └─ index.html           # cliente Apps Script (dashboard)
└─ .github/
   └─ workflows/
      ├─ ingesta.yml       # cron diario
      └─ gh-pages.yml      # despliegue GitHub Pages
```

¡Listo! Solo configura los secretos/variables y tendrás ingesta diaria + tablero embebido.
