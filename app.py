from __future__ import annotations
import os, io, re, tempfile
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

import numpy as np
import pandas as pd
import streamlit as st

# ========= Config =========
st.set_page_config(page_title="Go Mapper — Compilador AT&T", layout="wide")

# ========= OLC opcional =========
try:
    from openlocationcode import openlocationcode as olc
    _HAS_OLC = True
except Exception:
    _HAS_OLC = False

# ========= Utils de normalización =========
def _strip_accents(text: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")

def _norm_colname(name: str) -> str:
    name = _strip_accents(str(name)).strip().lower()
    name = re.sub(r"\s+", " ", name)
    name = name.replace("/", " ").replace("-", " ")
    name = name.replace("(", " ").replace(")", " ")
    name = name.replace("[", " ").replace("]", " ")
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def _pct(series: pd.Series, mask: pd.Series) -> float:
    try:
        den = series.notna().sum()
        if den == 0:
            return 0.0
        return float(mask.sum()) / float(den)
    except Exception:
        return 0.0

# ========= Diccionario de sinónimos (incluye headers de AT&T) =========
COLUMN_SYNONYMS: Dict[str, List[str]] = {
    # Registro (AT&T usa 'NO')
    "registro_id": ["no", "no.", "n°", "numero", "num", "nro", "id", "folio", "consecutivo", "id registro"],

    # Núcleos A/B
    "numero_a": [
        "numero a", "msisdn a", "origen", "abonado a", "a", "numero de origen",
        "num a", "telefono a", "tel a", "caller a", "telefono origen", "numero origen",
        "num_a"  # AT&T
    ],
    "numero_b": [
        "numero b", "msisdn b", "destino", "abonado b", "b", "numero de destino",
        "num b", "telefono b", "tel b", "called b", "telefono destino", "numero destino",
        "dest", "id dest", "id_dest"  # AT&T
    ],

    # Tipo / servicio / sentido
    "tipo": [
        "tipo", "tipo de registro", "tipo de evento", "servicio", "call type",
        "event type", "clase de servicio", "sentido", "direccion llamada", "entrada salida",
        "serv", "t reg", "t_reg", "tipo com", "tipo_com"  # AT&T
    ],

    # Fecha/hora
    "fecha": ["fecha", "date", "start date", "fecha inicio", "fecha llamada", "fecha de inicio"],
    "hora":  ["hora", "time", "start time", "hora inicio", "hora llamada", "hora de inicio"],
    "datetime": ["fecha hora", "fecha y hora", "datetime", "inicio", "timestamp", "fechahora", "fec hora", "fh inicio"],

    # Duración
    "duracion_seg": [
        "duracion seg", "duracion", "duracion segundos", "duration", "segundos",
        "tiempo de conexion", "tiempo", "duracion s", "tiempo (seg)", "tiempo seg",
        "duracion (s)", "duracion (segundos)", "dur"  # AT&T
    ],

    # Radio / celda
    "lac_tac": ["lac", "tac", "lac tac", "area", "lac t", "tac lac", "tac/lac", "lac/tac"],
    "ci_eci":  ["ci", "eci", "cell id", "cid", "cellid", "id de celda", "ecid", "eci/cid", "celd id", "id_celda", "id celda"],  # AT&T
    "tecnologia": ["tecnologia", "tecnologia radio", "radio", "rat", "2g 3g 4g 5g", "tecnología"],
    "celda": ["celda", "site", "sitio", "e nodeb", "enodeb", "sector"],
    "azimuth_deg": ["azimuth deg", "azimuth", "azimut", "angulo", "bearing", "azimuth"],  # AT&T usa AZIMUTH

    # Identificadores
    "imei": ["imei", "num a imei", "num_a_imei"],
    "imsi": ["imsi", "num a imsi", "num_a_imsi"],

    # Geografía
    "latitud": ["latitud", "lat", "latitude"],
    "longitud": ["longitud", "lon", "lng", "long", "longitude"],
    "direccion": ["direccion", "address", "ubicacion", "ubicacion antena", "location"],
    "plus_code": ["plus code", "olc", "code plus", "plus"],
    "plus_code_nombre": [
        "plus code nombre", "nombre lugar", "nombre ubicacion", "ubicacion geografica",
        "ubicacion geografica latitud longitud", "ubicacion texto"
    ],
}

def _build_reverse_map() -> Dict[str, str]:
    rev: Dict[str, str] = {}
    for canonical, syns in COLUMN_SYNONYMS.items():
        rev[_norm_colname(canonical)] = canonical
        for s in syns:
            rev[_norm_colname(s)] = canonical
    return rev

_REV_MAP = _build_reverse_map()

def _detect_columns(df: pd.DataFrame) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    normalized = {col: _norm_colname(col) for col in df.columns}
    for orig, norm in normalized.items():
        if norm in _REV_MAP:
            canonical = _REV_MAP[norm]
            mapping.setdefault(canonical, orig)
    return mapping

# ========= Lectura de archivos =========
def _read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xlsm"}:
        return pd.read_excel(path)  # requiere openpyxl
    elif ext == ".xls":
        return pd.read_excel(path, engine="xlrd")  # xlrd 1.2.0 para .xls antiguos
    elif ext in {".csv", ".txt"}:
        for enc in ("utf-8", "latin1"):
            for sep in (",", ";", "\t", "|"):
                try:
                    return pd.read_csv(path, sep=sep, engine="python", encoding=enc)
                except Exception:
                    continue
        return pd.read_csv(path, engine="python", encoding_errors="ignore")
    else:
        raise ValueError(f"Extensión no soportada: {ext}")

# ========= Parse de fecha/hora y duración =========
def _parse_duration_to_seconds(val: Any) -> Optional[int]:
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)) and not pd.isna(val):
        return int(round(float(val)))
    s = str(val).strip()
    if not s:
        return None
    if re.match(r"^\d{1,2}:[0-5]\d:[0-5]\d$", s):
        h, m, sec = s.split(":"); return int(h)*3600 + int(m)*60 + int(sec)
    if re.match(r"^[0-5]?\d:[0-5]\d$", s):
        m, sec = s.split(":"); return int(m)*60 + int(sec)
    s2 = re.sub(r"[^0-9]", "", s)
    if s2.isdigit(): return int(s2)
    return None

def _to_local_naive(ts: pd.Timestamp, tz: Optional[str]) -> pd.Timestamp:
    if tz is None or pd.isna(ts):
        return ts if getattr(ts, "tzinfo", None) is None else ts.tz_localize(None)
    if getattr(ts, "tzinfo", None) is None:
        return ts.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT").tz_convert(tz).tz_localize(None)
    return ts.tz_convert(tz).tz_localize(None)

def _combine_datetime(row: pd.Series, cols: Dict[str, str], tz: Optional[str]) -> Optional[pd.Timestamp]:
    # 'cols' refiere a nombres que EXISTEN en row
    if "datetime" in cols and cols["datetime"] in row.index:
        dt = pd.to_datetime(row[cols["datetime"]], errors="coerce", dayfirst=True)
        if pd.notna(dt): return _to_local_naive(dt, tz)
    f = row[cols["fecha"]] if "fecha" in cols and cols["fecha"] in row.index else None
    h = row[cols["hora"]]  if "hora"  in cols and cols["hora"]  in row.index else None
    if f is not None and h is not None:
        dt = pd.to_datetime(f"{f} {h}", errors="coerce", dayfirst=True)
        if pd.notna(dt): return _to_local_naive(dt, tz)
    if "fecha" in cols and cols["fecha"] in row.index:
        dt = pd.to_datetime(row[cols["fecha"]], errors="coerce", dayfirst=True)
        if pd.notna(dt): return _to_local_naive(dt, tz)
    return None

# ========= Normalización de Tipo y Dirección VOZ =========
_VOZ_OUT_TOKENS = {"mo", "saliente", "orig", "out", "originating", "salida"}
_VOZ_IN_TOKENS  = {"mt", "entrante", "term", "in", "terminating", "entrada"}
_MSG_TOKENS     = {"sms", "mensaje", "mensajes", "2 vias", "sms mo", "sms mt"}
_DATA_TOKENS    = {"gprs", "datos", "data", "internet"}
_TRANSF_TOKENS  = {"transfer", "desvio", "call forward", "cfu", "cfnr", "cfnry", "desvío"}

def _normalize_tipo(raw: Any) -> Optional[str]:
    if pd.isna(raw): return None
    s = _norm_colname(str(raw))
    if any(tok in s for tok in _TRANSF_TOKENS): return "TRANSFER"
    if any(tok in s for tok in _MSG_TOKENS):   return "MENSAJES 2 VÍAS"
    if any(tok in s for tok in _DATA_TOKENS):  return "DATOS"
    if any(tok in s for tok in _VOZ_OUT_TOKENS): return "VOZ SALIENTE"
    if any(tok in s for tok in _VOZ_IN_TOKENS):  return "VOZ ENTRANTE"
    if "voz" in s or "llamada" in s or "call" in s: return "VOZ SALIENTE"
    return None

def _dir_trafico_voz(tipo: Optional[str]) -> Optional[str]:
    if tipo == "VOZ SALIENTE": return "SALIENTE"
    if tipo == "VOZ ENTRANTE": return "ENTRANTE"
    return None

# ========= PLUS CODE =========
def _maybe_plus(lat: Any, lon: Any) -> Optional[str]:
    if not _HAS_OLC: return None
    try:
        latf, lonf = float(lat), float(lon)
        if not (-90 <= latf <= 90 and -180 <= lonf <= 180): return None
        return olc.encode(latf, lonf, codeLength=10)
    except Exception:
        return None

# ========= Heurísticas de respaldo =========
_MSISDN_RE = re.compile(r"^\+?\d[\d\s\-]{7,}$")
_HHMMSS_RE = re.compile(r"^\d{1,2}:[0-5]\d(:[0-5]\d)?$")

def _augment_mapping_with_guesses(df: pd.DataFrame, mapping: Dict[str, str]) -> Dict[str, str]:
    # Número A/B
    if ("numero_a" not in mapping) or ("numero_b" not in mapping):
        candidates = []
        for col in df.columns:
            s = df[col].astype(str)
            ratio = _pct(s, s.str.fullmatch(_MSISDN_RE).fillna(False))
            if ratio >= 0.5:
                candidates.append((col, ratio))
        candidates.sort(key=lambda x: x[1], reverse=True)
        if candidates:
            if "numero_a" not in mapping:
                mapping["numero_a"] = candidates[0][0]
            for col, _ in candidates[1:]:
                if col != mapping.get("numero_a"):
                    mapping.setdefault("numero_b", col)
                    break

    # Datetime combinado si no hay fecha/hora
    if "datetime" not in mapping and ("fecha" not in mapping or "hora" not in mapping):
        best_col, best_ratio = None, 0.0
        for col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            ratio = parsed.notna().mean()
            if ratio > best_ratio:
                best_col, best_ratio = col, ratio
        if best_col and best_ratio >= 0.5:
            mapping["datetime"] = best_col

    # Duración
    if "duracion_seg" not in mapping:
        for col in df.columns:
            n = _norm_colname(col)
            if any(tok in n for tok in ["duracion", "seg", "tiempo", "dur"]):
                mapping["duracion_seg"] = col
                break
        if "duracion_seg" not in mapping:
            best_col, best_ratio = None, 0.0
            for col in df.columns:
                s = df[col].astype(str)
                ratio = _pct(s, s.str.fullmatch(_HHMMSS_RE).fillna(False))
                if ratio > best_ratio:
                    best_col, best_ratio = col, ratio
            if best_col and best_ratio >= 0.5:
                mapping["duracion_seg"] = best_col

    return mapping

# ========= Estadísticas =========
def _build_stats(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    stats: Dict[str, pd.DataFrame] = {}
    if df.empty: return stats
    try:
        sal = df[df["Tipo"] == "VOZ SALIENTE"].groupby(["Número A", "Número B"], dropna=False).size().reset_index(name="Conteo").sort_values("Conteo", ascending=False).head(10)
        stats["Top10_Salientes"] = sal
    except Exception: pass
    try:
        ent = df[df["Tipo"] == "VOZ ENTRANTE"].groupby(["Número A", "Número B"], dropna=False).size().reset_index(name="Conteo").sort_values("Conteo", ascending=False).head(10)
        stats["Top10_Entrantes"] = ent
    except Exception: pass
    if "IMEI" in df.columns:
        try:
            imei = df.dropna(subset=["IMEI"]).groupby("IMEI").size().reset_index(name="Registros").sort_values("Registros", ascending=False).head(20)
            stats["Top_IMEI"] = imei
        except Exception: pass
    if {"LAC_TAC", "CI_ECI"}.issubset(df.columns):
        try:
            for t in ["DATOS", "VOZ ENTRANTE", "VOZ SALIENTE", "MENSAJES 2 VÍAS", "TRANSFER"]:
                sub = df[df["Tipo"] == t]
                if not sub.empty:
                    k = sub.groupby(["LAC_TAC", "CI_ECI"], dropna=False).size().reset_index(name="Eventos").sort_values("Eventos", ascending=False).head(10)
                    stats[f"Antenas_TOP__{t}"] = k
        except Exception: pass
    return stats

# ========= Salida Excel =========
def _write_excel(out_path: str, df: pd.DataFrame, log: pd.DataFrame, dupes: pd.DataFrame, stats: Dict[str, pd.DataFrame]):
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name="Datos_Limpios")
        log.to_excel(xw, index=False, sheet_name="LOG_Compilación")
        if not dupes.empty:
            dupes.to_excel(xw, index=False, sheet_name="Duplicados")
        if stats:
            for name, sdf in stats.items():
                sheet = name[:31]
                sdf.to_excel(xw, index=False, sheet_name=sheet)

# ========= Pipeline principal =========
@dataclass
class CompileResult:
    df: pd.DataFrame
    log: pd.DataFrame
    dupes: pd.DataFrame
    stats: Dict[str, pd.DataFrame]
    out_xlsx: Optional[str] = None

def compile_att_sabanas(
    file_paths: List[str],
    tz: Optional[str] = "America/Mazatlan",
    out_xlsx: Optional[str] = None,
) -> CompileResult:
    frames: List[pd.DataFrame] = []
    logs: List[Dict[str, Any]] = []

    for path in file_paths:
        try:
            raw = _read_any(path)
            orig_cols = list(raw.columns)

            # detectar y completar mapping
            mapping = _detect_columns(raw)
            mapping = _augment_mapping_with_guesses(raw, mapping)

            # renombrar a canónico
            rename_map = {v: k for k, v in mapping.items()}
            df = raw.rename(columns=rename_map).copy()

            # mapping canónico (EXISTENTE en df)
            canon_map = {k: k for k in mapping.keys() if k in df.columns}

            # Origen
            df["Archivo_Origen"] = os.path.basename(path)

            # Duración a segundos
            if "duracion_seg" in df.columns:
                df["duracion_seg"] = df["duracion_seg"].apply(_parse_duration_to_seconds)

            # Datetime
            if ("fecha" in df.columns) or ("datetime" in df.columns):
                df["Datetime"] = df.apply(lambda r: _combine_datetime(r, canon_map, tz), axis=1)

            # Tipo
            df["Tipo"] = df.get("tipo").apply(_normalize_tipo) if "tipo" in df.columns else None

            # Dirección VOZ
            df["Dirección del tráfico (VOZ)"] = df["Tipo"].apply(_dir_trafico_voz)

            # Registro_ID (NO)
            df["Registro_ID"] = pd.to_numeric(df.get("registro_id"), errors="coerce").astype("Int64") if "registro_id" in df.columns else pd.Series([pd.NA]*len(df), dtype="Int64")

            # A/B
            df["Número A"] = df.get("numero_a")
            df["Número B"] = df.get("numero_b")

            # Radio/celda
            df["LAC_TAC"] = df.get("lac_tac")
            df["CI_ECI"] = df.get("ci_eci")
            df["Tecnología"] = df.get("tecnologia")
            df["Celda"] = df.get("celda")
            df["Azimuth_deg"] = pd.to_numeric(df.get("azimuth_deg"), errors="coerce") if "azimuth_deg" in df.columns else None

            # IDs
            df["IMEI"] = df.get("imei")
            df["IMSI"] = df.get("imsi")

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

            # Duración (seg) salida
            df["Duración (seg)"] = df.get("duracion_seg")

            # Operador
            df["Operador"] = "AT&T"

            # Orden columnas
            cols_final = [
                "Registro_ID", "Archivo_Origen",
                "Operador", "Tipo", "Dirección del tráfico (VOZ)",
                "Número A", "Número B", "Datetime", "Duración (seg)",
                "IMEI", "IMSI", "Tecnología",
                "LAC_TAC", "CI_ECI", "Celda", "Azimuth_deg",
                "Latitud", "Longitud", "PLUS_CODE", "PLUS_CODE_NOMBRE",
            ]
            for c in cols_final:
                if c not in df.columns: df[c] = None
            df = df[cols_final]

            frames.append(df)
            logs.append({
                "archivo": os.path.basename(path),
                "filas": len(df),
                "columnas_detectadas": ", ".join(sorted(mapping.keys())),
                "columnas_origen": ", ".join(map(str, orig_cols)),
            })
        except Exception as e:
            logs.append({"archivo": os.path.basename(path), "error": str(e)})

    if not frames:
        empty = pd.DataFrame(columns=[
            "Registro_ID", "Archivo_Origen", "Operador", "Tipo", "Dirección del tráfico (VOZ)", "Número A", "Número B",
            "Datetime", "Duración (seg)", "IMEI", "IMSI", "Tecnología", "LAC_TAC",
            "CI_ECI", "Celda", "Azimuth_deg", "Latitud", "Longitud", "PLUS_CODE", "PLUS_CODE_NOMBRE"
        ])
        logdf = pd.DataFrame(logs)
        return CompileResult(empty, logdf, pd.DataFrame(), {}, out_xlsx)

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
    if "Registro_ID" in all_df.columns:
        try:
            if all_df["Registro_ID"].isna().all():
                all_df.drop(columns=["Registro_ID"], inplace=True, errors="ignore")
                all_df.insert(0, "Registro_ID", range(1, len(all_df) + 1))
        except Exception:
            all_df.drop(columns=["Registro_ID"], inplace=True, errors="ignore")
            all_df.insert(0, "Registro_ID", range(1, len(all_df) + 1))
    else:
        all_df.insert(0, "Registro_ID", range(1, len(all_df) + 1))

    # Orden por tiempo
    if "Datetime" in all_df.columns:
        all_df = all_df.sort_values("Datetime", na_position="last").reset_index(drop=True)

    logdf = pd.DataFrame(logs)
    stats = _build_stats(all_df)

    if out_xlsx:
        _write_excel(out_xlsx, all_df, logdf, dupes, stats)

    return CompileResult(all_df, logdf, dupes, stats, out_xlsx)

# ========= Helpers UI =========
CSV_SEPS = [",", ";", "\t", "|"]
CSV_ENCS = ["utf-8", "latin1"]

def _rerun():
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()

def sniff_headers_from_bytes(buf: bytes, filename: str) -> Optional[List[str]]:
    """Devuelve encabezados detectados del archivo en memoria."""
    name = filename.lower()
    try:
        if name.endswith((".xlsx", ".xls", ".xlsm")):
            xls = pd.ExcelFile(io.BytesIO(buf))
            sheet = xls.sheet_names[0]
            df = pd.read_excel(io.BytesIO(buf), sheet_name=sheet, nrows=5)
            return list(map(str, df.columns))
        elif name.endswith((".csv", ".txt")):
            for enc in CSV_ENCS:
                for sep in CSV_SEPS:
                    try:
                        df = pd.read_csv(io.BytesIO(buf), engine="python", encoding=enc, sep=sep, nrows=5)
                        return list(map(str, df.columns))
                    except Exception:
                        continue
            df = pd.read_csv(io.BytesIO(buf), engine="python", nrows=5)
            return list(map(str, df.columns))
    except Exception:
        return None
    return None

def rename_with_manual_map(src_path: str, dst_path: str, mapping: Dict[str, str]) -> None:
    """Lee el archivo, renombra columnas (raw->canónicas) y escribe CSV estándar UTF-8."""
    ext = os.path.splitext(src_path)[1].lower()
    if ext in {".xls", ".xlsx", ".xlsm"}:
        df = pd.read_excel(src_path)
    elif ext in {".csv", ".txt"}:
        ok = False
        for enc in CSV_ENCS:
            for sep in CSV_SEPS:
                try:
                    df = pd.read_csv(src_path, engine="python", encoding=enc, sep=sep)
                    ok = True; break
                except Exception:
                    continue
            if ok: break
        if not ok:
            df = pd.read_csv(src_path, engine="python", encoding_errors="ignore")
    else:
        df = pd.read_csv(src_path, engine="python", encoding_errors="ignore")

    rename_map = {}
    for canonical, raw_name in mapping.items():
        if raw_name and raw_name in df.columns:
            rename_map[raw_name] = canonical
    if rename_map:
        df = df.rename(columns=rename_map)
    df.to_csv(dst_path, index=False, encoding="utf-8")

# ========= UI =========
st.title("📞 Go Mapper — Compilador AT&T (single-file)")
st.write("Sube sábanas de AT&T (XLS/XLSX/CSV/TXT), normaliza al formato *Limpieza* y descarga el Excel final.")

st.sidebar.header("Parámetros")
tz = st.sidebar.text_input("Zona horaria", value="America/Mazatlan")
show_preview = st.sidebar.checkbox("Mostrar preview de datos", value=True)
st.sidebar.markdown("---")
st.sidebar.caption("No hay geocoding externo. PLUS_CODE requiere `openlocationcode` y Lat/Lon en los datos.")

files = st.file_uploader(
    "Arrastra y suelta archivos de AT&T (puedes subir varios)",
    type=["xlsx", "xls", "csv", "txt"],
    accept_multiple_files=True,
)

# Asistente de mapeo manual
manual_map: Dict[str, Optional[str]] = {}
use_manual = False

if files:
    first = files[0]
    headers = sniff_headers_from_bytes(first.getvalue(), first.name) or []
    with st.expander("🧭 Asistente de mapeo de columnas (opcional)", expanded=True):
        st.caption("Si tus encabezados difieren, selecciona qué columna corresponde a cada campo clave. Se aplicará a **todos** los archivos antes de compilar.")
        if not headers:
            st.warning("No pude detectar encabezados. Puedes compilar; el motor hará heurísticas.")
        cols = [None] + headers
        c1, c2, c3 = st.columns(3)
        with c1:
            a = st.selectbox("Número A (MSISDN origen)", options=cols, index=0, key="map_a")
            dt = st.selectbox("FechaHora combinada", options=cols, index=0, key="map_datetime")
            rid = st.selectbox("Registro_ID (NO)", options=cols, index=0, key="map_rid")
        with c2:
            b = st.selectbox("Número B (MSISDN destino)", options=cols, index=0, key="map_b")
            f = st.selectbox("Fecha (si viene separada)", options=cols, index=0, key="map_fecha")
            dur = st.selectbox("Duración (seg o HH:MM:SS)", options=cols, index=0, key="map_dur")
        with c3:
            t = st.selectbox("Tipo (voz/datos/sms)", options=cols, index=0, key="map_tipo")
            h = st.selectbox("Hora (si viene separada)", options=cols, index=0, key="map_hora")
            imei = st.selectbox("IMEI (opcional)", options=cols, index=0, key="map_imei")
        st.caption("Puedes dejar campos vacíos — el compilador intentará inferirlos.")
        use_manual = st.checkbox("Usar este mapeo manual en la compilación", value=False, key="use_manual")

        if use_manual:
            manual_map = {
                "numero_a": a,
                "numero_b": b,
                "tipo": t,
                "datetime": dt,
                "fecha": f,
                "hora": h,
                "duracion_seg": dur,
                "registro_id": rid,
                "imei": imei,
            }
            pretty = {k: v for k, v in manual_map.items() if v}
            if pretty:
                st.success(f"Mapeo manual activo: {pretty}")
            else:
                st.info("Mapeo manual activado, pero sin asignaciones — se usarán heurísticas del motor.")

left, right = st.columns([1,1])
compile_clicked = left.button("🧩 Compilar sábanas AT&T", type="primary")
clear_clicked = right.button("🗑️ Limpiar sesión")

if clear_clicked:
    _rerun()

if compile_clicked:
    if not files:
        st.warning("Primero sube al menos un archivo.")
    else:
        tmp_paths: List[str] = []
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                for f in files:
                    suffix = ("." + f.name.split(".")[-1].lower()) if "." in f.name else ""
                    raw_path = tempfile.NamedTemporaryFile(delete=False, dir=tmpdir, suffix=suffix).name
                    with open(raw_path, "wb") as w:
                        w.write(f.getvalue())

                    if use_manual and any(manual_map.values()):
                        norm_path = tempfile.NamedTemporaryFile(delete=False, dir=tmpdir, suffix=".csv").name
                        rename_with_manual_map(raw_path, norm_path, mapping={
                            "numero_a": manual_map.get("numero_a"),
                            "numero_b": manual_map.get("numero_b"),
                            "tipo": manual_map.get("tipo"),
                            "datetime": manual_map.get("datetime"),
                            "fecha": manual_map.get("fecha"),
                            "hora": manual_map.get("hora"),
                            "duracion_seg": manual_map.get("duracion_seg"),
                            "registro_id": manual_map.get("registro_id"),
                            "imei": manual_map.get("imei"),
                        })
                        tmp_paths.append(norm_path)
                    else:
                        tmp_paths.append(raw_path)

                with st.spinner("Compilando y normalizando…"):
                    res = compile_att_sabanas(tmp_paths, tz=tz, out_xlsx=None)

                st.success(f"✅ Compilado: {len(res.df):,} filas | Archivos procesados: {len(files)}")

                if show_preview:
                    st.subheader("Preview — Datos_Limpios")
                    st.dataframe(res.df.head(500), width="stretch")

                st.subheader("Resumen rápido")
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.metric("Filas totales", f"{len(res.df):,}")
                with c2:
                    tipos = res.df["Tipo"].value_counts(dropna=False).rename_axis("Tipo").reset_index(name="Conteo") if not res.df.empty else pd.DataFrame()
                    st.write("**Distribución por Tipo**")
                    if not tipos.empty:
                        st.dataframe(tipos, width="stretch")
                    else:
                        st.caption("Sin datos")
                with c3:
                    st.write("**Duplicados (DATOS/min) removidos**")
                    st.metric("Filas duplicadas", f"{len(res.dupes):,}")

                with st.expander("📜 LOG de compilación"):
                    st.dataframe(res.log, width="stretch")

                if res.stats:
                    st.subheader("📊 ESTADÍSTICAS")
                    for name, sdf in res.stats.items():
                        st.markdown(f"**{name}**")
                        st.dataframe(sdf, width="stretch")
                else:
                    st.caption("No se generaron estadísticas (dataset vacío o columnas clave ausentes).")

                def build_excel_bytes(df: pd.DataFrame, log: pd.DataFrame, dupes: pd.DataFrame, stats: Dict[str, pd.DataFrame]) -> bytes:
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
                    bio.seek(0)
                    return bio.getvalue()

                xlsx_bytes = build_excel_bytes(res.df, res.log, res.dupes, res.stats)
                st.download_button(
                    label="⬇️ Descargar Excel Compilado",
                    data=xlsx_bytes,
                    file_name="ATT_compilado.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        except Exception as e:
            st.error("Ocurrió un error durante la compilación.")
            st.exception(e)

st.markdown("""
---
### Salida estandarizada (igual que *Limpieza*)
- **Datos_Limpios**: Registro_ID, Archivo_Origen, Operador, Tipo, Dirección del tráfico (VOZ), Número A, Número B, Datetime, Duración (seg), IMEI, IMSI, Tecnología, LAC_TAC, CI_ECI, Celda, Azimuth_deg, Latitud, Longitud, PLUS_CODE, PLUS_CODE_NOMBRE.
- **LOG_Compilación**: archivo, filas, columnas detectadas, columnas origen y errores (si hay).
- **Duplicados**: regla especial *DATOS/min* (se conserva la mayor duración por pareja A/B).
- **Estadísticas**: Top salientes/entrantes, IMEI, Antenas TOP por tipo.
""")
