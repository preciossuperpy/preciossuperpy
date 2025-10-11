// Google Apps Script: Web App para tablero dinámico
// 1) Abrir script.google.com, crear proyecto, pegar estos archivos (Code.gs e index.html)
// 2) Establecer SHEET_ID abajo, desplegar como Web App (acceso: Anyone with the link o tu dominio)

const SHEET_ID = 'REEMPLAZA_CON_TU_SHEET_ID'; // p.ej. 1plZ1LzHu2W2TrbV7wXPueWsO2g4dFRyUdpxXIUE5ns8
const WS_NAME  = 'precios_supermercados';

function doGet() {
  return HtmlService.createTemplateFromFile('index').evaluate()
    .setTitle('Precios Super – Tablero')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function getData(limit) {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  const ws = ss.getSheetByName(WS_NAME);
  const range = ws.getDataRange();
  const values = range.getDisplayValues();
  const headers = values.shift();
  // busca columnas básicas
  const colIndex = Object.fromEntries(headers.map((h,i)=>[h,i]));
  const out = [];
  for (let i=0; i<values.length; i++) {
    const row = values[i];
    out.push({
      Supermercado: row[colIndex['Supermercado']] || '',
      Producto: row[colIndex['Producto']] || '',
      Precio: parseFloat((row[colIndex['Precio']]||'').toString().replace(',','.')) || null,
      Grupo: row[colIndex['Grupo']] || '',
      Subgrupo: row[colIndex['Subgrupo']] || '',
      FechaConsulta: row[colIndex['FechaConsulta']] || ''
    });
    if (limit && out.length >= limit) break;
  }
  return out;
}
