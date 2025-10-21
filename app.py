# app.py — Go Mapper — Compilador AT&T (single-file v2.8)
# - Sniff de encabezado real en sábanas AT&T (salta filas de portada)
# - Tipo derivado con SERV + T_REG + TIPO_COM
# - Hoja adicional Datos_Limpios_669 con columna Teléfono fija

from __future__ import annotations
import os, io, re, tempfile
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Go Mapper — Compilador AT&T (single-file)", layout="wide")

# --------- PLUS CODE opcional ----------
try:
    from openlocationcode import openlocationcode as olc
    _HAS_OLC = True
except Exception:
    _HAS_OLC = False

# ==================== Utilidades base ====================

def _strip_accents(text: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", str(text)) if unicodedata.category(c) != "Mn")

def _norm_colname(name: str) -> str:
    name = _strip_accents(str(name)).strip().lower()
    name = re.sub(r"\s+", " ", name)
    name = name.replace("/", " ").replace("-", " ")
    name = name.replace("(", " ").replace(")", " ")
    name = name.replace("[", " ").replace("]", " ")
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def _parse_duration_to_seconds(val: Any) -> Optional[int]:
    if pd.isna(val): return None
    if isinstance(val, (int, float)) and not pd.isna(val):
        return int(round(float(val)))
    s = str(val).strip()
    if not s: return None
    if re.match(r"^\d{1,2}:[0-5]\d:[0-5]\d$", s):  # HH:MM:SS
        h, m, sec = s.split(":"); return int(h)*3600 + int(m)*60 + int(sec)
    if re.match(r"^[0-5]?\d:[0-5]\d$", s):        # MM:SS
        m, sec = s.split(":"); return int(m)*60 + int(sec)
    s2 = re.sub(r"[^0-9]", "", s)
    if s2.isdigit(): return int(s2)
    return None

def _excel_days_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, unit="d", origin="1899-12-30", errors="coerce")

def _to_local_naive(ts: pd.Timestamp, tz: Optional[str]) -> pd.Timestamp:
    if tz is None or pd.isna(ts):
        return ts if getattr(ts, "tzinfo", None) is None else ts.tz_localize(None)
    if getattr(ts, "tzinfo", None) is None:
        return ts.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT").tz_convert(tz).tz_localize(None)
    return ts.tz_convert(tz).tz_localize(None)

def _maybe_plus(lat: Any, lon: Any) -> Optional[str]:
    if not _HAS_OLC: return None
    try:
        latf, lonf = float(lat), float(lon)
        if not (-90 <= latf <= 90 and -180 <= lonf <= 180): return None
        return olc.encode(latf, lonf, codeLength=10)
    except Exception:
        return None

# ==================== Sniff de encabezado real ====================

HEADER_TOKENS_REQ = {"NO", "FECHA"}  # y al menos uno de DUR / DURACIÓN
HEADER_TOKENS_ANY = {"DUR", "DURACIÓN"}

def _looks_like_header(row_vals: List[str]) -> bool:
    up = {str(x).strip().upper() for x in row_vals if pd.notna(x)}
    return HEADER_TOKENS_REQ.issubset(up) and (len(HEADER_TOKENS_ANY.intersection(up)) > 0)

def _read_excel_with_header_sniff(path: str) -> pd.DataFrame:
    # 1) Cargamos sin encabezado para examinar primeras ~100 filas
    raw = pd.read_excel(path, sheet_name=0, header=None, dtype=str)
    header_row_idx = None
    max_probe = min(100, len(raw))
    for i in range(max_probe):
        if _looks_like_header(raw.iloc[i].tolist()):
            header_row_idx = i
            break
    if header_row_idx is not None:
        headers = raw.iloc[header_row_idx].tolist()
        df = raw.iloc[header_row_idx+1:].copy()
        df.columns = headers
        df = df.dropna(how="all", axis=1).dropna(how="all").reset_index(drop=True)
        return df
    # Fallback: primer fila como encabezado
    return pd.read_excel(path, sheet_name=0)

def _read_csv_with_header_sniff(path: str) -> pd.DataFrame:
    # Intento 1: leer normal
    try:
        df = pd.read_csv(path, engine="python")
        if _looks_like_header(list(df.columns)):
            return df
    except Exception:
        pass
    # Intento 2: leer sin encabezado y buscar la fila correcta
    try:
        raw = pd.read_csv(path, engine="python", header=None, dtype=str)
        header_row_idx = None
        max_probe = min(100, len(raw))
        for i in range(max_probe):
            if _looks_like_header(raw.iloc[i].tolist()):
                header_row_idx = i
                break
        if header_row_idx is not None:
            headers = raw.iloc[header_row_idx].tolist()
            df = raw.iloc[header_row_idx+1:].copy()
            df.columns = headers
            df = df.dropna(how="all", axis=1).dropna(how="all").reset_index(drop=True)
            return df
    except Exception:
        pass
    # Último intento
    return pd.read_csv(path, engine="python", encoding_errors="ignore")

def _read_any_with_sniff(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xlsm", ".xls"}:
        if ext == ".xls":
            # xlrd para xls no soporta header=None con algunos archivos; intentamos normal primero
            try:
                return _read_excel_with_header_sniff(path)
            except Exception:
                return pd.read_excel(path, engine="xlrd")
        return _read_excel_with_header_sniff(path)
    elif ext in {".csv", ".txt"}:
        return _read_csv_with_header_sniff(path)
    else:
        raise ValueError(f"Extensión no soportada: {ext}")

# ==================== Modo estricto AT&T ====================

STRICT_ATT_MAP = {
    "no": "registro_id",
    "serv": "serv",
    "t reg": "t_reg", "t_reg": "t_reg",
    "tipo com": "tipo_com", "tipo_com": "tipo_com",
    "tipo": "tipo_com",
    "num a": "numero_a", "num_a": "numero_a",
    "num a imsi": "imsi", "num_a_imsi": "imsi",
    "num a imei": "imei", "num_a_imei": "imei",
    "dest": "numero_b", "id dest": "numero_b", "id_dest": "numero_b",
    "fecha": "fecha", "hora": "hora",
    "dur": "duracion_seg",
    "id celda": "ci_eci", "id_celda": "ci_eci",
    "latitud": "latitud", "longitud": "longitud",
    "azimuth": "azimuth_deg",
    # extras conservables
    "huso": "huso", "uso dw": "uso_dw", "uso_dw": "uso_dw",
    "uso up": "uso_up", "uso_up": "uso_up",
    "causa t": "causa_t", "causa_t": "causa_t", "pais": "pais",
}

# Tokens de clasificación
_VOICE_TOK = {"voz", "llamada", "call", "moc", "mtc"}
_DATA_TOK  = {"gprs", "datos", "data", "internet", "ps", "pdp", "packet"}
_SMS_TOK   = {"sms", "mensaje", "mensajes", "2 vias", "2vias", "mms"}
_TRF_TOK   = {"transfer", "desvio", "desvío", "call forward", "cfu", "cfb", "cfnry", "cfnr", "cfnrc"}

# Dirección VOZ
_OUT_TOK = {"mo", "moc", "saliente", "orig", "out", "originating", "salida"}
_IN_TOK  = {"mt", "mtc", "entrante", "term", "in", "terminating", "entrada"}

def _norm_text(x: Any) -> str:
    if pd.isna(x): return ""
    return _norm_colname(str(x))

def derive_tipo_from_serv_treg(serv: Any, t_reg: Any, tipo_com: Any = None) -> Optional[str]:
    s = _norm_text(serv); t = _norm_text(t_reg); c = _norm_text(tipo_com)
    # Transfer
    if any(tok in s or tok in t or tok in c for tok in _TRF_TOK): return "TRANSFER"
    # Mensajes
    if any(tok in s or tok in t or tok in c for tok in _SMS_TOK): return "MENSAJES 2 VÍAS"
    # Datos
    if any(tok in s or tok in t or tok in c for tok in _DATA_TOK): return "DATOS"
    # Voz + dirección
    is_voice = any(tok in s or tok in c for tok in _VOICE_TOK) or ("call" in t) or ("moc" in t) or ("mtc" in t)
    if is_voice:
        if any(tok in t or tok in s or tok in c for tok in _OUT_TOK): return "VOZ SALIENTE"
        if any(tok in t or tok in s or tok in c for tok in _IN_TOK):  return "VOZ ENTRANTE"
        return "VOZ SALIENTE"
    # Si T_REG deja claro IN/OUT:
    if any(tok in t for tok in _OUT_TOK): return "VOZ SALIENTE"
    if any(tok in t for tok in _IN_TOK):  return "VOZ ENTRANTE"
    return None

def _dir_voz(tipo: Optional[str]) -> Optional[str]:
    if tipo == "VOZ SALIENTE": return "SALIENTE"
    if tipo == "VOZ ENTRANTE": return "ENTRANTE"
    return None

def _strict_att_normalize(raw_df: pd.DataFrame, tz: Optional[str]) -> pd.DataFrame:
    # 1) Renombrado exacto por normalización
    norm_map = {_norm_colname(c): c for c in raw_df.columns}
    rename = {}
    for norm, orig in norm_map.items():
        if norm in STRICT_ATT_MAP:
            rename[orig] = STRICT_ATT_MAP[norm]
    df = raw_df.rename(columns=rename).copy()

    # 2) Campos base
    df["Operador"] = "AT&T"
    df["Número A"] = df.get("numero_a")
    df["Número B"] = df.get("numero_b")

    # 3) Tipo derivado (SERV + T_REG + TIPO_COM)
    df["Tipo"] = df.apply(
        lambda r: derive_tipo_from_serv_treg(r.get("serv"), r.get("t_reg"), r.get("tipo_com")),
        axis=1
    )
    df["Dirección del tráfico (VOZ)"] = df["Tipo"].apply(_dir_voz)

    # 4) Datetime a partir de FECHA/HORA (texto o serial)
    fecha = df.get("fecha")
    hora  = df.get("hora")
    dt = pd.Series([pd.NaT]*len(df), dtype="datetime64[ns]")

    if fecha is not None:
        f_num = pd.to_numeric(fecha, errors="coerce")
        f_isnum = f_num.notna()
        if f_isnum.any():
            dt[f_isnum] = _excel_days_to_datetime(f_num[f_isnum])
        if (~f_isnum).any():
            dt[~f_isnum] = pd.to_datetime(fecha[~f_isnum], errors="coerce", dayfirst=True)
        if hora is not None:
            h_str = hora.astype(str).str.strip()
            hhmmss = h_str.str.match(r"^\d{1,2}:[0-5]\d(:[0-5]\d)?$")
            idx1 = hhmmss & dt.notna()
            if idx1.any():
                dt[idx1] = pd.to_datetime(dt[idx1].dt.strftime("%Y-%m-%d") + " " + h_str[idx1], errors="coerce")
            h_num = pd.to_numeric(h_str, errors="coerce")
            idx_num = h_num.notna() & dt.notna()
            if idx_num.any():
                frac = h_num.between(0, 1, inclusive="both")
                idx_frac = idx_num & frac
                idx_secs = idx_num & (~frac)
                if idx_frac.any():
                    add = pd.to_timedelta((h_num[idx_frac] * 86400).round().astype(int), unit="s")
                    dt[idx_frac] = dt[idx_frac] + add
                if idx_secs.any():
                    add = pd.to_timedelta(h_num[idx_secs].round().astype(int), unit="s")
                    dt[idx_secs] = dt[idx_secs] + add
    elif "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], errors="coerce", dayfirst=True)

    df["Datetime"] = dt.apply(lambda x: _to_local_naive(x, tz) if pd.notna(x) else x)

    # 5) Duración
    if "duracion_seg" in df.columns:
        dur = df["duracion_seg"].apply(_parse_duration_to_seconds)
        as_num = pd.to_numeric(df["duracion_seg"], errors="coerce")
        frac_mask = as_num.notna() & (as_num.between(0, 1, inclusive="both"))
        if frac_mask.any():
            dur = dur.fillna(0)
            dur.loc[frac_mask] = (as_num.loc[frac_mask] * 86400).round().astype(int)
        df["Duración (seg)"] = dur
    elif "dur" in df.columns:
        df["Duración (seg)"] = df["dur"].apply(_parse_duration_to_seconds)
    else:
        df["Duración (seg)"] = None

    # Identificadores
    df["IMEI"] = df.get("imei")
    df["IMSI"] = df.get("imsi")

    # Celda / Radio
    df["LAC_TAC"] = df.get("lac_tac")
    df["CI_ECI"] = df.get("ci_eci")
    df["Tecnología"] = df.get("tecnologia")
    df["Celda"] = df.get("celda")
    df["Azimuth_deg"] = pd.to_numeric(df.get("azimuth_deg"), errors="coerce") if "azimuth_deg" in df.columns else None

    # Geo
    df["Latitud"] = pd.to_numeric(df.get("latitud"), errors="coerce") if "latitud" in df.columns else None
    df["Longitud"] = pd.to_numeric(df.get("longitud"), errors="coerce") if "longitud" in df.columns else None

    # PLUS CODE
    if _HAS_OLC and ("Latitud" in df.columns and "Longitud" in df.columns):
        try:
            df["PLUS_CODE"] = [
                _maybe_plus(lat, lon) if pd.notna(lat) and pd.notna(lon) else None
                for lat, lon in zip(df["Latitud"], df["Longitud"])
            ]
        except Exception:
            df["PLUS_CODE"] = None
    else:
        df["PLUS_CODE"] = df.get("plus_code")
    df["PLUS_CODE_NOMBRE"] = df.get("plus_code_nombre") if "plus_code_nombre" in df.columns else df.get("direccion")

    # Registro_ID
    if "registro_id" in df.columns:
        df["Registro_ID"] = pd.to_numeric(df["registro_id"], errors="coerce").astype("Int64")
    else:
        df["Registro_ID"] = pd.Series([pd.NA]*len(df), dtype="Int64")

    # Orden final
    cols_final = [
        "Registro_ID", "Archivo_Origen", "Operador", "Tipo", "Dirección del tráfico (VOZ)",
        "Número A", "Número B", "Datetime", "Duración (seg)",
        "IMEI", "IMSI", "Tecnología",
        "LAC_TAC", "CI_ECI", "Celda", "Azimuth_deg",
        "Latitud", "Longitud", "PLUS_CODE", "PLUS_CODE_NOMBRE",
    ]
    for c in cols_final:
        if c not in df.columns: df[c] = None
    return df[cols_final]

# ==================== Pipeline completo ====================

@dataclass
class CompileResult:
    df: pd.DataFrame
    log: pd.DataFrame
    dupes: pd.DataFrame
    stats: Dict[str, pd.DataFrame]
    out_xlsx: Optional[str] = None

def compile_att_sabanas_strict(file_paths: List[str], tz: Optional[str]) -> CompileResult:
    frames, logs = [], []
    for path in file_paths:
        try:
            raw = _read_any_with_sniff(path)
            raw["Archivo_Origen"] = os.path.basename(path)
            df = _strict_att_normalize(raw, tz=tz)
            frames.append(df)
            logs.append({
                "archivo": os.path.basename(path),
                "filas": len(df),
                "columnas_origen": ", ".join(map(str, raw.columns)),
                "modo": "estricto_AT&T + header-sniff"
            })
        except Exception as e:
            logs.append({"archivo": os.path.basename(path), "error": repr(e), "modo": "estricto_AT&T + header-sniff"})
    if not frames:
        return CompileResult(
            pd.DataFrame(columns=[
                "Registro_ID","Archivo_Origen","Operador","Tipo","Dirección del tráfico (VOZ)",
                "Número A","Número B","Datetime","Duración (seg)","IMEI","IMSI","Tecnología",
                "LAC_TAC","CI_ECI","Celda","Azimuth_deg","Latitud","Longitud","PLUS_CODE","PLUS_CODE_NOMBRE"
            ]),
            pd.DataFrame(logs), pd.DataFrame(), {}, None
        )
    all_df = pd.concat(frames, ignore_index=True)

    # Dedupe DATOS/min
    dupes = pd.DataFrame()
    if not all_df.empty and "Tipo" in all_df.columns and "Datetime" in all_df.columns:
        datos = all_df[all_df["Tipo"] == "DATOS"].copy()
        otros = all_df[all_df["Tipo"] != "DATOS"].copy()
        if not datos.empty:
            datos["_min"] = pd.to_datetime(datos["Datetime"], errors="coerce").dt.floor("min")
            datos["Duración (seg)"] = pd.to_numeric(datos["Duración (seg)"], errors="coerce")
            idx = datos.sort_values("Duración (seg)", ascending=False).groupby(["Número A", "Número B", "_min"], dropna=False).head(1).index
            kept = datos.loc[idx]; removed = datos.drop(index=idx)
            dupes = removed.drop(columns=["_min"], errors="ignore").copy()
            datos = kept.drop(columns=["_min"], errors="ignore")
            all_df = pd.concat([otros, datos], ignore_index=True)

    # Registro_ID si faltó
    if "Registro_ID" not in all_df or all_df["Registro_ID"].isna().all():
        if "Registro_ID" in all_df:
            all_df.drop(columns=["Registro_ID"], inplace=True, errors="ignore")
        all_df.insert(0, "Registro_ID", range(1, len(all_df)+1))

    if "Datetime" in all_df.columns:
        all_df = all_df.sort_values("Datetime", na_position="last").reset_index(drop=True)

    # Stats simples
    stats = {}
    try:
        sal = all_df[all_df["Tipo"] == "VOZ SALIENTE"].groupby(["Número A","Número B"], dropna=False).size().reset_index(name="Conteo").sort_values("Conteo", ascending=False).head(10)
        if not sal.empty: stats["Top10_Salientes"] = sal
    except Exception: pass
    try:
        ent = all_df[all_df["Tipo"] == "VOZ ENTRANTE"].groupby(["Número A","Número B"], dropna=False).size().reset_index(name="Conteo").sort_values("Conteo", ascending=False).head(10)
        if not ent.empty: stats["Top10_Entrantes"] = ent
    except Exception: pass

    return CompileResult(all_df, pd.DataFrame(logs), dupes, stats, None)

# ==================== Hoja extra tipo “669” (con Teléfono) ====================

def build_hoja_669(df: pd.DataFrame, telefono: str) -> pd.DataFrame:
    out_cols = [
        'Teléfono','Tipo','Número A','Número B','Fecha','Hora','Duración (seg)','IMEI',
        'Latitud','Longitud','Azimuth','Latitud_raw','Longitud_raw','Azimuth_raw',
        'PLUS_CODE','PLUS_CODE_NOMBRE','Azimuth_deg','Datetime','Es_Duplicado','Cuenta_GrupoDup'
    ]
    res = pd.DataFrame(index=range(len(df)), columns=out_cols)

    tel = str(telefono).strip() if telefono else None
    res['Teléfono'] = tel

    res['Tipo'] = df.get('Tipo')
    res['Número A'] = df.get('Número A')
    res['Número B'] = df.get('Número B')
    res['Duración (seg)'] = df.get('Duración (seg)')
    res['IMEI'] = df.get('IMEI')
    res['Latitud'] = df.get('Latitud')
    res['Longitud'] = df.get('Longitud')
    res['PLUS_CODE'] = df.get('PLUS_CODE')
    res['PLUS_CODE_NOMBRE'] = df.get('PLUS_CODE_NOMBRE')
    res['Azimuth_deg'] = df.get('Azimuth_deg')
    res['Datetime'] = pd.to_datetime(df.get('Datetime'), errors='coerce')

    # Raw
    res['Latitud_raw'] = df.get('Latitud').astype(str).where(df.get('Latitud').notna(), None)
    res['Longitud_raw'] = df.get('Longitud').astype(str).where(df.get('Longitud').notna(), None)
    res['Azimuth_raw'] = df.get('Azimuth_deg').astype(str).where(df.get('Azimuth_deg').notna(), None)
    res['Azimuth'] = df.get('Azimuth_deg')

    dt = res['Datetime']
    res['Fecha'] = dt.dt.strftime('%d/%m/%Y').where(dt.notna(), None)
    res['Hora']  = dt.dt.strftime('%H:%M:%S').where(dt.notna(), None)

    # Duplicados (DATOS/min)
    res['Es_Duplicado'] = False
    res['Cuenta_GrupoDup'] = 1
    mask_datos = res['Tipo'] == 'DATOS'
    if mask_datos.any():
        datos = res.loc[mask_datos].copy()
        datos['_min'] = pd.to_datetime(datos['Datetime'], errors='coerce').dt.floor('min')
        datos['Duración (seg)'] = pd.to_numeric(datos['Duración (seg)'], errors='coerce')
        grp = datos.groupby(['Número A','Número B','_min'], dropna=False)
        cnt = grp.size().rename('Cuenta_GrupoDup')
        datos = datos.join(cnt, on=['Número A','Número B','_min'])
        keep_idx = grp['Duración (seg)'].idxmax()
        datos['Es_Duplicado'] = True
        datos.loc[keep_idx, 'Es_Duplicado'] = False
        res.loc[datos.index, 'Es_Duplicado'] = datos['Es_Duplicado']
        res.loc[datos.index, 'Cuenta_GrupoDup'] = datos['Cuenta_GrupoDup']

    return res[out_cols]

def build_excel(df: pd.DataFrame, log: pd.DataFrame, dupes: pd.DataFrame, stats: Dict[str, pd.DataFrame],
                hoja_669: Optional[pd.DataFrame] = None) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name="Datos_Limpios")
        log.to_excel(xw, index=False, sheet_name="LOG_Compilación")
        if not dupes.empty:
            dupes.to_excel(xw, index=False, sheet_name="Duplicados")
        if stats:
            for name, sdf in stats.items():
                sheet = name[:31]
                sdf.to_excel(xw, index=False, sheet_name=sheet)
        if hoja_669 is not None:
            hoja_669.to_excel(xw, index=False, sheet_name="Datos_Limpios_669")
    bio.seek(0)
    return bio.getvalue()

# ==================== UI ====================

st.title("📞 Go Mapper — Compilador AT&T (single-file)")
st.caption("Detecta el encabezado real, deriva **Tipo** con `SERV+T_REG+TIPO_COM` y puede generar la hoja **Datos_Limpios_669** con la columna **Teléfono** fija.")

st.sidebar.header("Parámetros")
tz = st.sidebar.text_input("Zona horaria", value="America/Mazatlan")
telefono_obj = st.sidebar.text_input("MSISDN objetivo (columna 'Teléfono')", value="", help="Ej: 526691634209")
export_669 = st.sidebar.checkbox("Generar hoja 'Datos_Limpios_669' (con Teléfono)", value=True)
show_preview = st.sidebar.checkbox("Mostrar preview", value=True)

files = st.file_uploader(
    "Arrastra y suelta archivos AT&T (XLS/XLSX/CSV/TXT)",
    type=["xlsx","xls","csv","txt"],
    accept_multiple_files=True,
)

left, right = st.columns(2)
go    = left.button("🧩 Compilar (modo estricto AT&T)", type="primary")
clear = right.button("🗑️ Limpiar sesión")

if clear:
    try: st.rerun()
    except Exception: st.experimental_rerun()

if go:
    if not files:
        st.warning("Primero sube al menos un archivo.")
    else:
        tmp_paths: List[str] = []
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                for f in files:
                    suffix = ("." + f.name.split(".")[-1].lower()) if "." in f.name else ""
                    p = tempfile.NamedTemporaryFile(delete=False, dir=tmpdir, suffix=suffix).name
                    with open(p, "wb") as w:
                        w.write(f.getvalue())
                    tmp_paths.append(p)

                with st.spinner("Compilando y normalizando (estricto AT&T, header-sniff)…"):
                    res = compile_att_sabanas_strict(tmp_paths, tz=tz)

                st.success(f"✅ Compilado: {len(res.df):,} filas | Archivos: {len(files)}")
                if show_preview:
                    st.subheader("Preview — Datos_Limpios")
                    st.dataframe(res.df.head(500), width="stretch")

                st.subheader("📜 LOG de compilación")
                st.dataframe(res.log, width="stretch")

                hoja_669 = None
                if export_669:
                    hoja_669 = build_hoja_669(res.df, telefono=telefono_obj)
                    st.subheader("Preview — Datos_Limpios_669 (con Teléfono)")
                    st.dataframe(hoja_669.head(500), width="stretch")

                xlsx = build_excel(res.df, res.log, res.dupes, res.stats, hoja_669=hoja_669)
                st.download_button(
                    "⬇️ Descargar Excel Compilado",
                    xlsx,
                    file_name="ATT_compilado.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        except Exception as e:
            st.error("Ocurrió un error durante la compilación.")
            st.exception(e)

st.markdown("""
---
**Notas**
- Encabezado: si la sábana trae “portada”, el motor busca la fila que contenga `NO`, `FECHA` y `DUR` y usa esa como encabezado real.
- **Tipo**: se deriva de `SERV`, `T_REG` y `TIPO_COM` → `VOZ SALIENTE / VOZ ENTRANTE / DATOS / MENSAJES 2 VÍAS / TRANSFER`.
- **Datos_Limpios_669**: fija **Teléfono** al MSISDN objetivo (ej. `526691634209`).
- FECHA/HORA: acepta serial de Excel (número) o texto (`dd/mm/aaaa` y `HH:MM(:SS)`).
- DUR: acepta segundos, fracción de día (<1) o `HH:MM:SS`.
- Dedupe *DATOS/min*: conserva el registro de mayor duración por par A/B por minuto.
""")
