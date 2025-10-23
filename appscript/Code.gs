/** ===================== CONFIG ===================== **/
const SHEET_ID = '1plZ1LzHu2W2TrbV7wXPueWsO2g4dFRyUdpxXIUE5ns8';
const WS_NAME  = 'precios_supermercados';

/**
 * EXCLUSIONES: términos o formatos que NO deben considerarse “canasta básica”
 * por cada Grupo. Mantengo reglas prudentes y sin patrones truncados.
 */
const EXCLUSIONES = {
  'Verdulería': /en\s+(?:lata|conserva|vinagre)|encurtid[oa]|congelad[oa]|deshidratad[oa]|polvo|chips?|snack/i,
  'Lácteos': /\b(?:en\s+polvo|rallad[oa]|fundid[oa]|concentrad[oa]|evaporad[oa]|saborizad[oa]|light|diet)\b/i,
  'Panadería': /\b(?:tostad[oa]|integral|sin\s+gluten|hornead[oa])\b/i,
  'Huevos': /\b(?:de\s+chocolate|kinde?r|decorad[oa]|relleno|en\s+polvo)\b/i,
  'Carnicería': /\b(?:jamonad[oa]|salame|mortadela|embutid[oa]|ahumad[oa]|empanad[oa])\b/i,
};

/** ===================== HTML ===================== **/
function doGet() {
  return HtmlService.createHtmlOutputFromFile('index')
    .setTitle('Tablero de Precios - Canasta Básica')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/** Utilidad: fecha a ISO (YYYY-MM-DD) */
const toISO = (d) => {
  if (!d) return '';
  if (Object.prototype.toString.call(d) === '[object Date]' && !isNaN(d)) {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
  const parsed = new Date(d);
  return isNaN(parsed) ? '' : toISO(parsed);
};

/** ===================== DATA API ===================== **/
function getData(filters = {}) {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  const ws = ss.getSheetByName(WS_NAME);
  if (!ws) {
    return { data: [], uniqueValues: { supermercados: [], grupos: [], subgrupos: [], productos: [] } };
  }

  const values = ws.getDataRange().getValues();
  if (!values.length) {
    return { data: [], uniqueValues: { supermercados: [], grupos: [], subgrupos: [], productos: [] } };
  }

  const headers = values.shift().map(String);
  const col = Object.fromEntries(headers.map((h, i) => [h, i]));

  // Filtros normalizados
  const filtroSuper = (filters.supermercado || '').trim();
  const filtroGrupo = (filters.grupo || '').trim();
  const filtroSub   = (filters.subgrupo || '').trim();
  const filtroProd  = (filters.producto || '').trim().toLowerCase();
  const fechaIniISO = (filters.fechaInicio || '').trim();
  const fechaFinISO = (filters.fechaFin || '').trim();
  const soloCB      = !!filters.soloCanastaBasica;

  const uniques = {
    supermercados: new Set(),
    grupos: new Set(),
    subgrupos: new Set(),
    productos: new Set(),
  };

  const out = [];

  for (const row of values) {
    const supermercado = String(row[col['Supermercado']] || '').trim();
    const producto     = String(row[col['Producto']] || '').trim();
    const grupo        = String(row[col['Grupo']] || '').trim();
    const subgrupo     = String(row[col['Subgrupo']] || '').trim();

    // Precio robusto
    const precioCell = row[col['Precio']];
    let precio = null;
    if (typeof precioCell === 'number') {
      precio = precioCell;
    } else if (typeof precioCell === 'string') {
      const p = parseFloat(precioCell.replace(',', '.'));
      precio = isNaN(p) ? null : p;
    }

    // Fecha robusta → ISO YYYY-MM-DD
    const fechaCell = row[col['FechaConsulta']];
    const fechaISO  = toISO(fechaCell);
    if (!fechaISO) continue;

    // Filtros
    if (filtroSuper && supermercado !== filtroSuper) continue;
    if (filtroGrupo && grupo !== filtroGrupo) continue;
    if (filtroSub   && subgrupo !== filtroSub) continue;
    if (filtroProd  && !producto.toLowerCase().includes(filtroProd)) continue;
    if (fechaIniISO && fechaISO < fechaIniISO) continue;
    if (fechaFinISO && fechaISO > fechaFinISO) continue;

    if (soloCB) {
      const exclusion = EXCLUSIONES[grupo];
      if (exclusion && exclusion.test(producto)) continue;
    }

    // Uniques
    if (supermercado) uniques.supermercados.add(supermercado);
    if (grupo)        uniques.grupos.add(grupo);
    if (subgrupo)     uniques.subgrupos.add(subgrupo);
    if (producto)     uniques.productos.add(producto);

    out.push({
      Supermercado: supermercado,
      Grupo: grupo,
      Subgrupo: subgrupo,
      Producto: producto,
      Precio: (typeof precio === 'number' && isFinite(precio)) ? precio : null,
      FechaConsulta: fechaISO,
    });
  }

  return {
    data: out,
    uniqueValues: {
      supermercados: Array.from(uniques.supermercados).sort(),
      grupos:        Array.from(uniques.grupos).sort(),
      subgrupos:     Array.from(uniques.subgrupos).sort(),
      productos:     Array.from(uniques.productos).sort(),
    },
  };
}

function getTimeSeriesData(filters = {}) {
  const payload = getData(filters);
  const data = payload.data || [];
  if (!data.length) return { timeSeries: {}, categories: [], dates: [] };

  const categoryField = (filters.timeSeriesCategory === 'Subgrupo')
    ? 'Subgrupo'
    : (filters.timeSeriesCategory === 'Producto')
      ? 'Producto'
      : 'Grupo';

  // date ISO -> category -> {sum, count}
  const grouped = {};
  const dateSet = new Set();
  const catSet  = new Set();

  for (const item of data) {
    if (item.Precio == null || !item.FechaConsulta) continue;
    const d   = item.FechaConsulta;
    const cat = item[categoryField] || 'Sin categoría';
    dateSet.add(d);
    catSet.add(cat);

    if (!grouped[d]) grouped[d] = {};
    if (!grouped[d][cat]) grouped[d][cat] = { sum: 0, count: 0 };

    grouped[d][cat].sum   += item.Precio;
    grouped[d][cat].count += 1;
  }

  const dates = Array.from(dateSet).sort();
  let categories = Array.from(catSet);

  // Limitar a top-10 categorías por promedio global
  const score = {};
  for (const c of categories) {
    let s = 0, n = 0;
    for (const d of dates) {
      if (grouped[d] && grouped[d][c]) {
        s += grouped[d][c].sum;
        n += grouped[d][c].count;
      }
    }
    score[c] = n ? s / n : 0;
  }
  categories = categories.sort((a, b) => score[b] - score[a]).slice(0, 10);

  const timeSeries = {};
  for (const c of categories) timeSeries[c] = [];

  for (const d of dates) {
    for (const c of categories) {
      if (grouped[d] && grouped[d][c]) {
        timeSeries[c].push([d, grouped[d][c].sum / grouped[d][c].count]);
      } else {
        timeSeries[c].push([d, null]);
      }
    }
  }

  return { timeSeries, categories, dates };
}
