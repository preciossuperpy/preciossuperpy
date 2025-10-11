/** CONFIG **/
const SHEET_ID = '1plZ1LzHu2W2TrbV7wXPueWsO2g4dFRyUdpxXIUE5ns8';
const WS_NAME  = 'precios_supermercados';

// Exclusiones para canasta básica (coinciden con valores de la columna "Grupo")
const EXCLUSIONES = {
  'Tomate fresco': /arroz\s+con\s+tomate|en\s+tomate|salsa\s+de\s+tomate|con\s+tomate|ketchup|tomate\s+en\s+polvo|tomate\s+en\s+lata|extr(?:a|o)/gi,
  'Morrón rojo': /en\s+conserva|lata|vinagre|encurtid[oa]/gi, // corregido: no excluye por 'ñ'
  'Cebolla fresca': /en\s+polvo|salsa|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pat[eé]|crema|sopa/gi,
  'Papa fresca': /chips|frit[ao]s?|chuñ[oa]|pur[ée]|congelad[oa]|deshidratad[oa]|harina|sopa|snack/gi,
  'Zanahoria fresca': /jug(?:o)/gi,
  'Lechuga fresca': /ensalada\s+procesada|mix\s+de\s+ensaladas|congelad[oa]|deshidratad[oa]/gi,
  'Remolacha fresca': /en\s+lata|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|jug(?:o)/gi,
  'Rúcula fresca': /extracto|jugo|con\s+|sabor|pulpa|pur[ée]|salsa|lata|en\s+conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pat[eé]|mermelada|chips|snack|polvo|humo/gi,
  'Berro fresco': /extracto|jugo|con\s+|sabor|pulpa|pur[ée]|salsa|lata|en\s+conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]|pat[eé]|mermelada|chips|snack|polvo|humo/gi,
  'Banana fresca': /harina|polvo|chips|frit[ao]s?|dulce|mermelada|batido|jugo|snack|pur[ée]|congelad[oa]|deshidratad[oa]/gi,
  'Cítrico fresco': /jug(?:o)/gi,
  'Zapallo/Calabaza fresco': /ensalada|pur[ée]|conserva|crema|snack|congelad[oa]|deshidratad[oa]/gi,
  'Pepino fresco': /encurtid[oa]|conserva|en\s+vinagre|congelad[oa]|deshidratad[oa]/gi,
  'Batata/Boniato fresco': /chips|frit[ao]s?|harina|pur[ée]|mermelada|congelad[oa]|deshidratad[oa]/gi,
  'Repollo fresco': /en\s+lata|conserva|encurtid[oa]|congelad[oa]|deshidratad[oa]/gi,
  'Leche fresca': /en\s+polvo|concentrad[oa]|evaporad[oa]|descremad[oa]/gi,
  'Queso fresco': /rallad[oa]|fundid[oa]|crema|procesad[oa]|untar/gi,
  'Yogur': /azucarad[oa]|batid[oa]|gelatinizad[oa]|saborizad[oa]/gi,
  'Panificados': /tostad[oa]|integral|sin\s+gluten|hornead[oa]/gi,
  'Galletitas': /salad[oa]|dulce|rellen[oa]s?|simple|con\s+chocolate/gi,
  'Pan de hamburguesa': /con\s+semilla|bland[oa]|integral|precortad[oa]/gi,
  'Miel de abeja': /procesad[oa]|mezclad[oa]|artificial|con\s+sabor/gi,
  'Café': /instantáne?o|soluble|descafeinad[oa]|saborizad[oa]/gi,
  'Azúcar': /light|diet|morena|rubia|impalpable/gi,
  'Bebida': /alcoh[oó]lica|energ[eé]tica|isot[oó]nica|gaseos[oa]|en\s+polvo/gi,
  'Helado': /crem[oa]|saborizad[oa]|paleta|conos?/gi,
  'Carne vacuna': /jamonad[oa]|salame|mortadela|chorizo|empanad[oa]|embutid[oa]|ahumad[oa]/gi,
  'Carne de cerdo': /chuleta|lomo|tocin[oa]|patita|chorizo/gi,
  'Carne de pollo': /pechuga|alita|muslo|jamoncito/gi,
  'Picadillo': /pre?cocid[oa]|con\s+especia/gi,
  'Fideo instantáneo': /sopa\s+instantáne?a|conserva/gi,
  'Condimento / Cubo': /caldo|condimento|concentrad[oa]|polvo/gi,
  'Snacks / Mascotas': /alimento\s+para\s+(?:gato|perro)/gi
};

/** UTIL **/
const toISO = (d) => {
  if (!d) return '';
  if (Object.prototype.toString.call(d) === '[object Date]' && !isNaN(d)) {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
  // Si vino como string (por ejemplo por getDisplayValues), intentamos parsear
  const parsed = new Date(d);
  return isNaN(parsed) ? '' : toISO(parsed);
};

const parseDate = (s) => {
  if (!s) return null;
  const d = new Date(s); // espera 'YYYY-MM-DD'
  return isNaN(d) ? null : d;
};

function doGet() {
  return HtmlService.createTemplateFromFile('index').evaluate()
    .setTitle('Tablero de Precios - Canasta Básica')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

/** DATA **/
function getData(filters = {}) {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  const ws = ss.getSheetByName(WS_NAME);
  if (!ws) return { data: [], uniqueValues: { supermercados: [], grupos: [], subgrupos: [], productos: [] } };

  // Usamos getValues para obtener tipos nativos (número/fecha)
  const range = ws.getDataRange();
  const values = range.getValues();
  const headers = values.shift().map(String);

  const colIndex = Object.fromEntries(headers.map((h, i) => [h, i]));

  // Normalizamos filtros
  const filtroSuper = (filters.supermercado || '').trim();
  const filtroGrupo = (filters.grupo || '').trim();
  const filtroSub  = (filters.subgrupo || '').trim();
  const filtroProd = (filters.producto || '').trim().toLowerCase();
  const soloCB     = !!filters.soloCanastaBasica; // Boolean real
  const fIni       = parseDate(filters.fechaInicio || '');
  const fFin       = parseDate(filters.fechaFin || '');

  const uniques = {
    supermercados: new Set(),
    grupos: new Set(),
    subgrupos: new Set(),
    productos: new Set()
  };

  const out = [];

  for (let i = 0; i < values.length; i++) {
    const row = values[i];

    const supermercado = String(row[colIndex['Supermercado']] || '').trim();
    const producto     = String(row[colIndex['Producto']] || '').trim();
    const precioCell   = row[colIndex['Precio']];
    const grupo        = String(row[colIndex['Grupo']] || '').trim();
    const subgrupo     = String(row[colIndex['Subgrupo']] || '').trim();
    const fechaCell    = row[colIndex['FechaConsulta']];

    // Precio numérico robusto
    let precio = null;
    if (typeof precioCell === 'number') {
      precio = precioCell;
    } else if (typeof precioCell === 'string') {
      const maybe = parseFloat(precioCell.replace(',', '.'));
      precio = isNaN(maybe) ? null : maybe;
    }

    // Fecha como Date e ISO para respuesta/comparación
    let fechaDate = null;
    if (Object.prototype.toString.call(fechaCell) === '[object Date]' && !isNaN(fechaCell)) {
      fechaDate = fechaCell;
    } else if (typeof fechaCell === 'string') {
      const d = new Date(fechaCell);
      fechaDate = isNaN(d) ? null : d;
    }
    const fechaISO = toISO(fechaDate);

    // Filtros
    if (filtroSuper && supermercado !== filtroSuper) continue;
    if (filtroGrupo && grupo !== filtroGrupo) continue;
    if (filtroSub  && subgrupo !== filtroSub) continue;
    if (filtroProd && !producto.toLowerCase().includes(filtroProd)) continue;

    if (fIni && (!fechaDate || fechaDate < fIni)) continue;
    if (fFin && (!fechaDate || fechaDate > fFin)) continue;

    // Solo canasta básica => excluir según patrón por Grupo
    if (soloCB) {
      const exclusionPattern = EXCLUSIONES[grupo];
      if (exclusionPattern && exclusionPattern.test(producto)) {
        continue;
      }
    }

    // Uniques (sin vacíos)
    if (supermercado) uniques.supermercados.add(supermercado);
    if (grupo)        uniques.grupos.add(grupo);
    if (subgrupo)     uniques.subgrupos.add(subgrupo);
    if (producto)     uniques.productos.add(producto);

    out.push({
      Supermercado: supermercado,
      Producto: producto,
      Precio: precio,
      Grupo: grupo,
      Subgrupo: subgrupo,
      FechaConsulta: fechaISO
    });
  }

  return {
    data: out,
    uniqueValues: {
      supermercados: Array.from(uniques.supermercados).sort(),
      grupos: Array.from(uniques.grupos).sort(),
      subgrupos: Array.from(uniques.subgrupos).sort(),
      productos: Array.from(uniques.productos).sort()
    }
  };
}

function getTimeSeriesData(filters = {}) {
  const resp = getData(filters);
  const data = resp.data;

  // Elegimos dimensión para series
  const categoryField = (filters.timeSeriesCategory === 'Subgrupo')
    ? 'Subgrupo'
    : (filters.timeSeriesCategory === 'Producto')
      ? 'Producto'
      : 'Grupo';

  // date(ISO) -> category -> {sum, count}
  const grouped = {};
  const dateSet = new Set();
  const catSet  = new Set();

  data.forEach(item => {
    if (item.Precio == null || !item.FechaConsulta) return;
    const dateISO = item.FechaConsulta;
    const cat = item[categoryField] || 'Sin categoría';
    dateSet.add(dateISO);
    catSet.add(cat);

    if (!grouped[dateISO]) grouped[dateISO] = {};
    if (!grouped[dateISO][cat]) grouped[dateISO][cat] = { sum: 0, count: 0 };

    grouped[dateISO][cat].sum   += item.Precio;
    grouped[dateISO][cat].count += 1;
  });

  const dates = Array.from(dateSet).sort(); // YYYY-MM-DD ordena bien
  let categories = Array.from(catSet);

  // Limitar a top 10 categorías por promedio global para no sobrecargar el gráfico
  const catScore = {};
  categories.forEach(c => {
    let s = 0, n = 0;
    dates.forEach(d => {
      if (grouped[d] && grouped[d][c]) {
        s += grouped[d][c].sum;
        n += grouped[d][c].count;
      }
    });
    catScore[c] = n ? (s / n) : 0;
  });
  categories = categories
    .sort((a, b) => (catScore[b] - catScore[a]))
    .slice(0, 10);

  const timeSeries = {};
  categories.forEach(c => timeSeries[c] = []);

  dates.forEach(d => {
    categories.forEach(c => {
      if (grouped[d] && grouped[d][c]) {
        const avg = grouped[d][c].sum / grouped[d][c].count;
        timeSeries[c].push([d, avg]); // fecha ISO, valor
      } else {
        timeSeries[c].push([d, null]);
      }
    });
  });

  return {
    timeSeries,
    categories,
    dates // YYYY-MM-DD
  };
}
