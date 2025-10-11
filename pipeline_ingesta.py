# -*- coding: utf-8 -*-
"""
Ingesta incremental de CSVs desde una carpeta de Google Drive a un Google Sheet.
- Lista y descarga todos los CSV del folder_id indicado.
- Solo ingiere archivos no procesados (bitácora en worksheet 'ingestas_archivos').
- Concatena con el histórico de la hoja 'precios_supermercados' y re-escribe la hoja.
Se configura por variables de entorno (ver README).
"""

from __future__ import annotations
import os, io, sys, json
from datetime import datetime, timezone
from typing import List, Dict

import numpy as np
import pandas as pd

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# ==================== CONFIG DESDE ENV ====================
FOLDER_ID        = os.getenv("DRIVE_FOLDER_ID", "").strip()
SPREADSHEET_URL  = os.getenv("SPREADSHEET_URL", "").strip()
DATA_WS_NAME     = os.getenv("DATA_WS_NAME", "precios_supermercados").strip()
LOG_WS_NAME      = os.getenv("LOG_WS_NAME", "ingestas_archivos").strip()

if not FOLDER_ID:
    raise SystemExit("Env DRIVE_FOLDER_ID no definido.")
if not SPREADSHEET_URL:
    raise SystemExit("Env SPREADSHEET_URL no definido.")

def make_credentials():
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    js = os.getenv("SERVICE_ACCOUNT_JSON")
    if js:
        info = json.loads(js)
        return Credentials.from_service_account_info(info, scopes=scopes)
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./service_account.json")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No se encontró el archivo de credenciales '{path}'. "
            "Define GOOGLE_APPLICATION_CREDENTIALS o SERVICE_ACCOUNT_JSON."
        )
    return Credentials.from_service_account_file(path, scopes=scopes)

def open_sheets(creds):
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SPREADSHEET_URL)
    try:
        ws_data = sh.worksheet(DATA_WS_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws_data = sh.add_worksheet(title=DATA_WS_NAME, rows="1000", cols="50")
    try:
        ws_log = sh.worksheet(LOG_WS_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws_log = sh.add_worksheet(title=LOG_WS_NAME, rows="100", cols="10")
        set_with_dataframe(ws_log, pd.DataFrame(columns=[
            "file_id","file_name","md5","modified_time","size",
            "rows_imported","imported_at"
        ]))
    return ws_data, ws_log

def get_drive(creds):
    return build("drive", "v3", credentials=creds)

def list_csv_files(drive, folder_id: str) -> List[Dict]:
    q = (
        f"'{folder_id}' in parents and trashed = false "
        f"and (mimeType='text/csv' or name contains '.csv')"
    )
    fields = "nextPageToken, files(id,name,mimeType,modifiedTime,md5Checksum,size)"
    files, page_token = [], None
    while True:
        resp = drive.files().list(q=q, fields=fields, pageSize=1000, pageToken=page_token).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def download_csv_to_df(drive, file_id: str, file_name: str) -> pd.DataFrame:
    req = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    try:
        df = pd.read_csv(fh, dtype=str, sep=None, engine="python")
    except Exception:
        fh.seek(0)
        df = pd.read_csv(fh, dtype=str)
    df["source_file_id"] = file_id
    df["source_file_name"] = file_name
    return df

def df_from_ws(ws) -> pd.DataFrame:
    df = get_as_dataframe(ws, dtype=str, header=0, evaluate_formulas=False)
    return df.dropna(how="all")

def write_ws(ws, df: pd.DataFrame):
    ws.clear()
    set_with_dataframe(ws, df, include_index=False)

KEY_COLS = ["Supermercado","CategoríaURL","Producto","FechaConsulta"]
def smart_dedup(df: pd.DataFrame) -> pd.DataFrame:
    if all(c in df.columns for c in KEY_COLS):
        return df.drop_duplicates(KEY_COLS, keep="first")
    return df.drop_duplicates(keep="first")

def main():
    creds = make_credentials()
    drive = get_drive(creds)
    ws_data, ws_log = open_sheets(creds)

    df_prev = df_from_ws(ws_data)
    df_log = df_from_ws(ws_log)
    already = set(df_log["file_id"].dropna()) if "file_id" in df_log.columns else set()

    files = list_csv_files(drive, FOLDER_ID)
    if not files:
        print("No se encontraron CSVs en la carpeta."); return 0

    new_dataframes, new_log_rows = [], []

    for f in files:
        fid, name = f.get("id"), f.get("name")
        if fid in already:
            continue
        try:
            df = download_csv_to_df(drive, fid, name)
        except Exception as e:
            print(f"⚠️ Error '{name}': {e}"); continue
        new_dataframes.append(df)
        new_log_rows.append({
            "file_id": fid,
            "file_name": name,
            "md5": f.get("md5Checksum",""),
            "modified_time": f.get("modifiedTime",""),
            "size": f.get("size",""),
            "rows_imported": len(df),
            "imported_at": datetime.now(timezone.utc).isoformat()
        })

    if not new_dataframes:
        print("No hay archivos nuevos para importar."); return 0

    df_new = pd.concat(new_dataframes, ignore_index=True, sort=False)
    # Unión de columnas
    all_cols = list(dict.fromkeys(list(df_prev.columns if len(df_prev) else []) + list(df_new.columns)))
    for c in all_cols:
        if c not in df_prev.columns: df_prev[c] = np.nan
        if c not in df_new.columns: df_new[c] = np.nan

    base = pd.concat([df_prev[all_cols], df_new[all_cols]], ignore_index=True, sort=False)
    base = smart_dedup(base)

    if "ID" in base.columns: base = base.drop(columns=["ID"])
    base.insert(0, "ID", range(1, len(base) + 1))

    # Redondeo suave si existen
    for col in ("Precio","cantidad_unidades","precio_unidad"):
        if col in base.columns:
            base[col] = pd.to_numeric(base[col], errors="coerce")
    if "Precio" in base.columns: base["Precio"] = base["Precio"].round(2)
    if "cantidad_unidades" in base.columns: base["cantidad_unidades"] = base["cantidad_unidades"].round(3)
    if "precio_unidad" in base.columns: base["precio_unidad"] = base["precio_unidad"].round(3)

    write_ws(ws_data, base)

    if new_log_rows:
        df_newlog = pd.DataFrame(new_log_rows)
        log_cols = list(dict.fromkeys(list(df_log.columns if len(df_log) else []) + list(df_newlog.columns)))
        for c in log_cols:
            if c not in df_log.columns: df_log[c] = np.nan
            if c not in df_newlog.columns: df_newlog[c] = np.nan
        df_log = pd.concat([df_log[log_cols], df_newlog[log_cols]], ignore_index=True, sort=False)
        write_ws(ws_log, df_log)

    print(f"✅ Importación completa. Filas totales en '{DATA_WS_NAME}': {len(base)}")
    print(f"🗂️ Archivos nuevos procesados: {len(new_dataframes)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
