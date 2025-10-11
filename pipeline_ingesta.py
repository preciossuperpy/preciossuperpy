# -*- coding: utf-8 -*-
"""
Scraper unificado de precios – Portable (Colab y no Colab)
Append histórico a Google Sheets con enriquecimiento de unidades
"""

from __future__ import annotations
from typing import List, Dict, Callable, Set, Optional, Tuple
import os, sys, glob, re, unicodedata, json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"J:\Mi unidad\preciossuper\scrappinsupermercados-1ce00b82eb71_b.json"


# ───────── 0) Entorno (Colab opcional) ─────────
IS_COLAB = False
try:
    from google.colab import drive, auth  # type: ignore
    IS_COLAB = True
except Exception:
    drive = None
    auth = None

if IS_COLAB:
    drive.mount('/content/drive')
    auth.authenticate_user()

# ───────── 1) Paths & Constantes ─────────
OUT_DIR = os.getenv(
    "OUT_DIR",
    "/content/drive/My Drive/preciossuper" if IS_COLAB else os.path.abspath("./data")
)
os.makedirs(OUT_DIR, exist_ok=True)

PATTERN_DAILY   = os.path.join(OUT_DIR, "*_canasta_*.csv")
# Si usas GOOGLE_APPLICATION_CREDENTIALS (ruta) no hace falta tocar CREDS_JSON
CREDS_JSON      = os.getenv("GOOGLE_APPLICATION_CREDENTIALS",
                            "/content/drive/My Drive/preciossuper/scrappinsupermercados-1ce00b82eb71.json" if IS_COLAB else "./service_account.json")

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1plZ1LzHu2W2TrbV7wXPueWsO2g4dFRyUdpxXIUE5ns8"
WORKSHEET_NAME  = "precios_supermercados"

MAX_WORKERS, REQ_TIMEOUT = 8, 10
KEY_COLS = ["Supermercado", "CategoríaURL", "Producto", "FechaConsulta"]

# ───────── 2) Dependencias Google Sheets ─────────
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

def _make_credentials():
    scopes = ["https://www.googleapis.com/auth/drive",
              "https://www.googleapis.com/auth/spreadsheets"]
    # Permitir pasar el JSON por variable de entorno SERVICE_ACCOUNT_JSON
    js = os.getenv("SERVICE_ACCOUNT_JSON")
    if js:
        info = json.loads(js)
        return Credentials.from_service_account_info(info, scopes=scopes)
    # Si no, usar archivo (GOOGLE_APPLICATION_CREDENTIALS o CREDS_JSON)
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", CREDS_JSON)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No se encontró el archivo de credenciales en '{path}'. "
            "Define GOOGLE_APPLICATION_CREDENTIALS o SERVICE_ACCOUNT_JSON."
        )
    return Credentials.from_service_account_file(path, scopes=scopes)

def _open_sheet():
    cred = _make_credentials()
    sh   = gspread.authorize(cred).open_by_url(SPREADSHEET_URL)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="60")
    df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False).dropna(how="all")
    return ws, df

def _write_sheet(ws, df):
    ws.clear()
    set_with_dataframe(ws, df, include_index=False)

# ───────── 3) Texto & Clasificación ─────────
def strip_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", txt) if unicodedata.category(c) != "Mn")

_token_re = re.compile(r"[a-záéíóúñü]+", re.I)
def tokenize(txt: str) -> List[str]:
    return [strip_accents(t.lower()) for t in _token_re.findall(txt)]

EXCLUDE_PRODUCT_WORDS = [
    "pañal","pañales","toallita","algodon","curita","gasas","jeringa",
    "termometro","ibuprofeno","paracetamol","ampolla","inyectable",
    "dental","facial","crema","locion","shampoo","perfume","maquillaje",
    "labial","rimel","colonia","esmalte","herramienta","martillo","clavo",
    "taladro","pintura","cable","juguete","bicicleta","detergente",
    "lavavajilla","alfajor","alfajores","celulitis","corporal","fusifar",
    "estrias","aciclovir"
]
EXCLUDE_SET: Set[str] = {strip_accents(w) for w in EXCLUDE_PRODUCT_WORDS}
def is_excluded(name: str) -> bool:
    return any(tok in EXCLUDE_SET for tok in tokenize(name))

CATEGORY_RULES = [
    {"name":"Carnicería","include":[r"\bcarne\b",r"\bvacuno\b",r"\bcerdo\b",r"\bpollo\b",r"\bpescado\b",
                                    r"\bmarisco\b",r"\bhamburguesa\b",r"\bmortadela\b",r"\bchuleta\b",
                                    r"\bpechuga\b",r"\bmuslo\b",r"\bmilanesa\b",r"\bsalchicha\b",
                                    r"\bchorizo\b",r"\bchurrasco\b"],
     "exclude":[r"\bconserva\b",r"\blata\b",r"\benlatado\b",r"\bsalsa\b",r"\bgalleta\b",
                r"\bpascua\b",r"\bchocolate\b",r"\bdulce\b",r"\bvegetal\b",r"\bsoja\b",r"\btexturizada\b"]},
    {"name":"Panadería","include":[r"\bpan\b",r"\bbaguette\b",r"\bfactura\b",r"\bmedialuna\b",r"\bcroissant\b",
                                   r"\bbizcocho\b",r"\bbudin\b",r"\btorta\b",r"\bgalleta\b",r"\bprepizza\b",
                                   r"\bpizza\b",r"\bchipa\b",r"\btostada\b",r"\brosquita\b",r"\bpanqueque\b",
                                   r"\bmasa\b",r"\bhojaldre\b",r"\bpanettone\b"],
     "exclude":[r"\bmolde\b",r"\bplástico\b",r"\bmetal\b",r"\bjuguete\b",r"\bpascua\b",r"\bchocolate\b",
                r"\bpintura\b",r"\bbandeja\b"]},
    {"name":"Huevos","include":[r"\bhuevo\b",r"\bhuevos\b"],
     "exclude":[r"\bpascua\b",r"\bchocolate\b",r"\bdulce\b",r"\bjuguete\b",r"\bsorpresa\b",
                r"\bdecorado\b",r"\brelleno\b",r"\bkinde?r\b"]},
    {"name":"Lácteos","include":[r"\bleche\b",r"\byogur\b",r"\bqueso\b",r"\bmanteca\b",r"\bcrema\b",
                                 r"\bmuzzarella\b",r"\bdambo\b",r"\bricotta\b",r"\bpostre\b",r"\bflan\b",
                                 r"\bdulce de leche\b",r"\bcremoso\b",r"\bquesillo\b",r"\bprovoleta\b"],
     "exclude":[r"\bfacial\b",r"\bcorporal\b",r"\bhidratante\b",r"\bjab[oó]n\b",r"\bshampoo\b",
                r"\bacondicionador\b",r"\bmaquillaje\b",r"\bdesodorante\b",r"\bprotector\b"]},
    {"name":"Verdulería","include":[r"\blechuga\b",r"\btomate\b",r"\bcebolla\b",r"\bzanahoria\b",r"\bpapa\b",
                                    r"\bbatata\b",r"\blim[oó]n\b",r"\bnaranja\b",r"\bmanzana\b",r"\bbanana\b",
                                    r"\bpera\b",r"\bdurazno\b",r"\bfrutilla\b",r"\buva\b",r"\bpimiento\b",
                                    r"\bpalta\b",r"\bajo\b",r"\bperejil\b",r"\bmandarina\b"],
     "exclude":[r"\bconserva\b",r"\benlatado\b",r"\bcongelado\b",r"\bsalsa\b"]},
]

def assign_group(name: str) -> Optional[str]:
    if not name: return None
    s = name.lower()
    for cat in CATEGORY_RULES:
        if any(re.search(ex, s) for ex in cat.get("exclude", [])):
            continue
        if any(re.search(p, s) for p in cat["include"]):
            return cat["name"]
    return None

SUBGROUP_RULES: Dict[str, List[Tuple[str, str]]] = {
    "Lácteos":[(r"\bleche\b","Leche"),(r"\byogur\b","Yogur"),(r"\bqueso\b","Queso"),
               (r"\bmanteca\b","Manteca"),(r"\bcrema\b","Crema"),(r"\bflan\b|\bpostre\b","Postre Lácteo")],
    "Carnicería":[(r"\bpollo\b|pechuga|muslo","Pollo"),
                  (r"\bvacuno\b|lomo|asado|churrasco|bola de lomo","Vacuno"),
                  (r"\bcerdo\b|chuleta|bondiola|costilla","Cerdo"),
                  (r"\bsalchicha\b|chorizo|hamburguesa|mortadela","Embutidos/Procesados"),
                  (r"\bpescado\b|merluza|tilapia|salm[oó]n","Pescado")],
    "Panadería":[(r"\bpan\b","Pan"),(r"\bgalleta\b|cookie","Galleta"),
                 (r"\bprepizza\b|\bpizza\b","Pizza/Prepizza"),(r"\bchipa\b","Chipa")],
    "Huevos":[(r"\bhuev","Huevos")],
    "Verdulería":[(r".*","Hortalizas/Frutas")]
}
def assign_subgroup(name: str, group: Optional[str]) -> Optional[str]:
    if not group or not name: return None
    rules = SUBGROUP_RULES.get(group, [])
    s = name.lower()
    for pat, lbl in rules:
        if re.search(pat, s):
            return lbl
    return None

# ───────── 4) Unidades ─────────
_pack_re = re.compile(
    r"(?:(\d+)\s*[x×]\s*)?(\d+(?:[.,]\d+)?)\s*(kg|kilo|gr|g|l|lt|litros?|ml|cc|cm3|u|unid(?:ad(?:es)?)?|und|uds)\b",
    re.I
)
def parse_units_from_text(txt: str) -> Tuple[str, str, float]:
    if not txt: return ("","",0.0)
    s = strip_accents(txt.lower())

    m = _pack_re.search(s)
    if not m:
        m2 = re.search(r"(\d+)\s*(u|unid(?:ad(?:es)?)?|und|uds)\b", s, re.I)
        if m2:
            n = float(m2.group(1))
            return (m2.group(0), "u", n)
        return ("","",0.0)

    n_pack = m.group(1)
    cant   = m.group(2)
    unit   = m.group(3).lower()

    unit = {"kilo":"kg","lt":"l","litro":"l","litros":"l","gr":"g","cm3":"ml","cc":"ml",
            "unid":"u","unidad":"u","unidades":"u","und":"u","uds":"u"}.get(unit, unit)
    try:
        val = float(cant.replace(",", "."))
    except:
        val = 0.0
    mult = float(n_pack) if n_pack else 1.0

    etiqueta, cantidad = "", 0.0
    if unit in ("g","kg"):
        if unit == "g": val /= 1000.0
        etiqueta, cantidad = "kg", mult * val
    elif unit in ("ml","l"):
        if unit == "ml": val /= 1000.0
        etiqueta, cantidad = "l", mult * val
    elif unit == "u":
        etiqueta, cantidad = "u", mult * (val if val > 0 else 1.0)

    return (m.group(0).strip(), etiqueta, round(float(cantidad), 6))

def enrich_unit_cols(df: pd.DataFrame) -> pd.DataFrame:
    if "Producto" not in df.columns: return df
    unidad_raw, etiqueta, cantidad = [], [], []
    for nombre in df["Producto"].fillna(""):
        raw, etq, qty = parse_units_from_text(nombre)
        unidad_raw.append(raw); etiqueta.append(etq); cantidad.append(qty)
    df["Unidad"] = unidad_raw
    df["unidad_corregido"] = etiqueta
    df["etiquetaunidad"] = etiqueta
    df["cantidad_unidades"] = cantidad

    df["Precio"] = pd.to_numeric(df.get("Precio"), errors="coerce")
    df["precio_unidad"] = np.where(
        (pd.to_numeric(df["cantidad_unidades"], errors="coerce").fillna(0) > 0),
        df["Precio"] / df["cantidad_unidades"].replace(0, np.nan),
        np.nan
    )
    return df

# ───────── 5) HTTP helpers ─────────
def norm_price(val) -> float:
    if isinstance(val, (int, float)): return float(val)
    txt = re.sub(r"[^\d,\.]", "", str(val)).replace(".", "").replace(",", ".")
    try: return float(txt)
    except ValueError: return 0.0

def _first_price(node: BeautifulSoup, sels: List[str] = None) -> float:
    sels = sels or ["span.price ins span.amount","span.price > span.amount",
                    "span.woocommerce-Price-amount","span.amount","bdi","[data-price]"]
    for s in sels:
        el = node.select_one(s)
        if el:
            p = norm_price(el.get_text() or el.get("data-price",""))
            if p > 0: return p
    return 0.0

def _build_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.2,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=("GET","HEAD"), raise_on_status=False)
    ad = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.headers["User-Agent"] = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/123 Safari/537.36")
    s.mount("http://", ad); s.mount("https://", ad)
    return s

# ───────── 6) Scrapers ─────────
KEYWORDS_SUPER = (
    "carn", "carne", "carnes", "vacuno", "pollo", "cerdo", "pescado",
    "pan", "panader", "galleta", "pizza", "chipa",
    "huevo", "huevos",
    "lacteo", "lacteos", "leche", "yogur", "queso", "manteca", "crema",
    "verduler", "frutas", "verduras", "hortalizas"
)

class HtmlSiteScraper:
    def __init__(self, name, base):
        self.name = name
        self.base_url = base.rstrip("/")
        self.session = _build_session()

    def category_urls(self):  raise NotImplementedError
    def parse_category(self, url): raise NotImplementedError

    def scrape(self):
        urls = self.category_urls()
        if not urls: return []
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out = []
        with ThreadPoolExecutor(MAX_WORKERS) as pool:
            futs = {pool.submit(self.parse_category, u): u for u in urls}
            for f in as_completed(futs):
                try:
                    for row in f.result():
                        row["FechaConsulta"] = fecha
                        out.append(row)
                except Exception:
                    pass
        return out

    def save_csv(self, rows):
        if not rows: return
        fn = f"{self.name}_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, fn), index=False)

class StockScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("stock","https://www.stock.com.py")
    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = set()
        for a in soup.select('a[href*="/category/"]'):
            href = a.get("href","").lower()
            if any(k in href for k in KEYWORDS_SUPER):
                urls.add(urljoin(self.base_url, a["href"]))
        return list(urls)
    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows=[]
        for p in soup.select("div.product-item"):
            nm = p.select_one("h2.product-title")
            if not nm: continue
            nombre = nm.get_text(" ", strip=True)
            if is_excluded(nombre): continue
            grupo = assign_group(nombre)
            if not grupo: continue
            precio = _first_price(p, ["span.price-label", "span.price"])
            rows.append({"Supermercado":"Stock","CategoríaURL":url,
                         "Producto":nombre.upper(),"Precio":precio,"Grupo":grupo})
        return rows

class SuperseisScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("superseis","https://www.superseis.com.py")
    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls=set()
        for a in soup.select('a[href*="/category/"]'):
            href = a.get("href","").lower()
            if any(k in href for k in KEYWORDS_SUPER):
                urls.add(urljoin(self.base_url, a["href"]))
        return list(urls)
    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows=[]
        for a in soup.select("a.product-title-link"):
            nombre = a.get_text(" ", strip=True)
            if is_excluded(nombre): continue
            grupo = assign_group(nombre)
            if not grupo: continue
            cont = a.find_parent("div", class_="product-item") or a
            precio = _first_price(cont, ["span.price-label","span.price"])
            rows.append({"Supermercado":"Superseis","CategoríaURL":url,
                         "Producto":nombre.upper(),"Precio":precio,"Grupo":grupo})
        return rows

class SalemmaScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("salemma","https://www.salemmaonline.com.py")
    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls=set()
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if any(k in href for k in KEYWORDS_SUPER):
                urls.add(urljoin(self.base_url, a["href"]))
        return list(urls)
    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows=[]
        for f in soup.select("form.productsListForm"):
            nm = f.find("input", {"name":"name"})
            nombre = (nm.get("value","") if nm else "").strip()
            if not nombre: continue
            if is_excluded(nombre): continue
            grupo = assign_group(nombre)
            if not grupo: continue
            pr = f.find("input", {"name":"price"})
            precio = norm_price(pr.get("value","") if pr else "")
            rows.append({"Supermercado":"Salemma","CategoríaURL":url,
                         "Producto":nombre.upper(),"Precio":precio,"Grupo":grupo})
        return rows

class AreteScraper(HtmlSiteScraper):
    def __init__(self): super().__init__("arete","https://www.arete.com.py")
    def category_urls(self):
        try:
            r = self.session.get(self.base_url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls=set()
        for sel in ("#departments-menu","#menu-departments-menu-1"):
            for a in soup.select(f'{sel} a[href^="catalogo/"]'):
                href = a["href"].split("?")[0].lower()
                if any(k in href for k in KEYWORDS_SUPER):
                    urls.add(urljoin(self.base_url+"/", a["href"]))
        return list(urls)
    def parse_category(self, url):
        try:
            r = self.session.get(url, timeout=REQ_TIMEOUT); r.raise_for_status()
        except Exception: return []
        soup = BeautifulSoup(r.content, "html.parser")
        rows=[]
        for p in soup.select("div.product"):
            nm = p.select_one("h2.ecommercepro-loop-product__title")
            if not nm: continue
            nombre = nm.get_text(" ", strip=True)
            if is_excluded(nombre): continue
            grupo = assign_group(nombre)
            if not grupo: continue
            precio = _first_price(p)
            rows.append({"Supermercado":"Arete","CategoríaURL":url,
                         "Producto":nombre.upper(),"Precio":precio,"Grupo":grupo})
        return rows

class JardinesScraper(AreteScraper):
    def __init__(self):
        super().__init__()
        self.name = "losjardines"
        self.base_url = "https://losjardinesonline.com.py"

# Biggie API
class BiggieScraper:
    name, API, TAKE = "biggie","https://api.app.biggie.com.py/api/articles",100
    GROUPS = ["carniceria","panaderia","huevos","lacteos"]
    session = _build_session()
    def fetch_group(self, grp):
        rows, skip = [], 0
        while True:
            try:
                js = self.session.get(self.API, params=dict(
                    take=self.TAKE, skip=skip, classificationName=grp
                ), timeout=REQ_TIMEOUT).json()
            except Exception:
                break
            for it in js.get("items", []):
                nombre = it.get("name", "")
                if is_excluded(nombre): continue
                grupo = assign_group(nombre) or grp.capitalize()
                rows.append({"Supermercado":"Biggie","CategoríaURL":grp,
                             "Producto":nombre.upper(),
                             "Precio":norm_price(it.get("price",0)),
                             "Grupo":grupo})
            skip += self.TAKE
            if skip >= js.get("count", 0): break
        return rows
    def scrape(self):
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows=[]
        for g in self.GROUPS:
            for item in self.fetch_group(g):
                item["FechaConsulta"]=fecha
                rows.append(item)
        return rows
    def save_csv(self, rows):
        if not rows: return
        fn = f"biggie_canasta_{datetime.now():%Y%m%d_%H%M%S}.csv"
        pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, fn), index=False)

# ───────── 7) Gestor de sitios ─────────
SCRAPERS: Dict[str, Callable] = {
    "stock":StockScraper, "superseis":SuperseisScraper, "salemma":SalemmaScraper,
    "arete":AreteScraper, "losjardines":JardinesScraper, "biggie":BiggieScraper
}

def _parse_args(argv=None):
    if argv is None: return list(SCRAPERS)
    if any(a in ("-h","--help") for a in argv):
        print("Uso: python script.py [sitio1 sitio2 …]"); sys.exit(0)
    sel = [a for a in argv if a in SCRAPERS]
    return sel or list(SCRAPERS)

# ───────── 8) Orquestador ─────────
def main(argv=None):
    objetivos = _parse_args(argv if argv is not None else sys.argv[1:])
    registros = []
    for k in objetivos:
        sc = SCRAPERS[k]()
        filas = sc.scrape()
        sc.save_csv(filas)
        registros.extend(filas)
        print(f"• {k:<12}: {len(filas):>5} filas")

    if not registros:
        print("Sin datos nuevos.")
        return 0

    csvs = glob.glob(PATTERN_DAILY)
    if csvs:
        df_all = pd.concat([pd.read_csv(f, dtype=str) for f in csvs], ignore_index=True, sort=False)
    else:
        df_all = pd.DataFrame(registros)

    df_all["Grupo"]  = df_all["Grupo"].map(strip_accents).fillna("")
    df_all["Precio"] = pd.to_numeric(df_all["Precio"], errors="coerce")

    # Enriquecimiento
    df_all["Subgrupo"] = [assign_subgroup(n, g) for n, g in zip(df_all.get("Producto",""), df_all.get("Grupo",""))]
    df_all = enrich_unit_cols(df_all)

    # Abrir hoja y unir con histórico previo
    ws, df_prev = _open_sheet()
    target_cols = [
        "ID","Supermercado","Producto","Precio","Unidad","Grupo","Subgrupo",
        "FechaConsulta","unidad_corregido","etiquetaunidad","cantidad_unidades","precio_unidad",
        "CategoríaURL"
    ]
    for c in target_cols:
        if c not in df_all.columns: df_all[c] = np.nan
        if c not in df_prev.columns: df_prev[c] = np.nan

    base = pd.concat([df_prev[target_cols], df_all[target_cols]], ignore_index=True, sort=False)
    base["FechaConsulta"] = pd.to_datetime(base["FechaConsulta"], errors="coerce")
    base.sort_values("FechaConsulta", inplace=True)
    base["FechaConsulta"] = base["FechaConsulta"].dt.strftime("%Y-%m-%d")
    base.drop_duplicates(KEY_COLS, keep="first", inplace=True)

    # ID secuencial
    if "ID" in base.columns:
        base.drop(columns=["ID"], inplace=True, errors="ignore")
    base.insert(0, "ID", range(1, len(base) + 1))

    # Redondeos
    base["Precio"] = pd.to_numeric(base["Precio"], errors="coerce").round(2)
    base["cantidad_unidades"] = pd.to_numeric(base["cantidad_unidades"], errors="coerce").round(3)
    base["precio_unidad"] = pd.to_numeric(base["precio_unidad"], errors="coerce").round(3)

    _write_sheet(ws, base[target_cols])
    print(f"✅ Hoja '{WORKSHEET_NAME}' actualizada: {len(base)} filas totales")
    return 0

if __name__ == "__main__":
    sys.exit(main())
