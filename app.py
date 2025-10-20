# app.py — Go Mapper — Compilador AT&T (single-file v2.5)

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

def _read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xlsm"}:
        return pd.read_excel(path)  # openpyxl
    elif ext == ".xls":
        return pd.read_excel(path, engine="xlrd")  # xlrd==1.2.0
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

# ==================== Modo estricto AT&T ====================

STRICT_ATT_MAP = {
    # encabezados comunes normalizados → canónico
    "no": "registro_id",
    "serv": "tipo", "t reg": "tipo", "t_reg": "tipo", "tipo com": "tipo", "tipo_com": "tipo",
    "num a": "numero_a", "num_a": "numero_a",
    "num a imsi": "imsi", "num_a_imsi": "imsi",
    "num a imei": "imei", "num_a_imei": "imei",
    "dest": "numero_b", "id dest": "numero_b", "id_dest": "numero_b",
    "fecha": "fecha", "hora": "hora",
    "dur": "duracion_seg",
    "id celda": "ci_eci", "id_celda": "ci_eci",
    "latitud": "latitud", "longitud": "longitud",
    "azimuth": "azimuth_deg",
    # extras conservables (si llegan)
    "huso": "huso", "uso dw": "uso_dw", "uso_dw": "uso_dw",
    "uso up": "uso_up", "uso_up": "uso_up",
    "causa t": "causa_t", "causa_t": "causa_t", "pais": "pais",
}

_VOZ_OUT = {"mo", "saliente", "orig", "out", "originating", "salida"}
_VOZ_IN  = {"mt", "entrante", "term", "in", "terminating", "entrada"}
_MSG     = {"sms", "mensaje", "mensajes", "2 vias", "sms mo", "sms mt"}
_DATA    = {"gprs", "datos", "data", "internet"}
_TRANSF  = {"transfer", "desvio", "call forward", "cfu", "cfnr", "cfnry", "desvío"}

def _normalize_tipo(raw: Any) -> Optional[str]:
    if pd.isna(raw): return None
    s = _norm_colname(raw)
    if any(t in s for t in _TRANSF): return "TRANSFER"
    if any(t in s for t in _MSG):    return "MENSAJES 2 VÍAS"
    if any(t in s for t in _DATA):   return "DATOS"
    if any(t in s for t in _VOZ_OUT): return "VOZ SALIENTE"
    if any(t in s for t in _VOZ_IN):  return "VOZ ENTRANTE"
    if "voz" in s or "llamada" in s or "call" in s: return "VOZ SALIENTE"
    return None

def _dir_voz(tipo: Optional[str]) -> Optional[str]:
    if tipo == "VOZ SALIENTE": return "SALIENTE"
    if tipo == "VOZ ENTRANTE": return "ENTRANTE"
    return None

def _strict_att_normalize(raw_df: pd.DataFrame, tz: Optional[str]) -> pd.DataFrame:
    # 1) Renombrado exacto: normaliza encabezados y aplica STRICT_ATT_MAP
    norm_map = {_norm_colname(c): c for c in raw_df.columns}
    rename = {}
    for norm, orig in norm_map.items():
        if norm in STRICT_ATT_MAP:
            rename[orig] = STRICT_ATT_MAP[norm]
    df = raw_df.rename(columns=rename).copy()

    # 2) Campos base
    df["Operador"] = "AT&T"
    # A/B
    df["Número A"] = df.get("numero_a")
    df["Número B"] = df.get("numero_b")

    # Tipo + Dirección VOZ
    if "tipo" in df.columns:
        df["Tipo"] = df["tipo"].apply(_normalize_tipo)
    else:
        df["Tipo"] = None
    df["Dirección del tráfico (VOZ)"] = df["Tipo"].apply(_dir_voz)

    # 3) Datetime a partir de FECHA/HORA (texto o serial numérico)
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
            # caso HH:MM(:SS)
            idx1 = hhmmss & dt.notna()
            if idx1.any():
                dt[idx1] = pd.to_datetime(dt[idx1].dt.strftime("%Y-%m-%d") + " " + h_str[idx1], errors="coerce")
            # caso numérico (fracción de día o segundos)
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

    # 4) Duración
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
            raw = _read_any(path)
            raw["Archivo_Origen"] = os.path.basename(path)
            df = _strict_att_normalize(raw, tz=tz)
            frames.append(df)
            logs.append({
                "archivo": os.path.basename(path),
                "filas": len(df),
                "columnas_origen": ", ".join(map(str, raw.columns)),
                "modo": "estricto_AT&T"
            })
        except Exception as e:
            logs.append({"archivo": os.path.basename(path), "error": repr(e), "modo": "estricto_AT&T"})
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

def build_excel(df: pd.DataFrame, log: pd.DataFrame, dupes: pd.DataFrame, stats: Dict[str, pd.DataFrame]) -> bytes:
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

# ==================== Helpers UI ====================

CSV_SEPS = [",", ";", "\t", "|"]
CSV_ENCS = ["utf-8", "latin1"]

def sniff_headers_from_bytes(buf: bytes, filename: str) -> Optional[List[str]]:
    """Detecta encabezados del archivo en memoria."""
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

def rename_with_manual_map(src_path: str, dst_path: str, mapping: Dict[str, Optional[str]]) -> None:
    """Lee el archivo, renombra columnas (raw->canónicas) y escribe CSV UTF-8."""
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

# ==================== UI ====================

st.title("📞 Go Mapper — Compilador AT&T (single-file)")
st.caption("Modo **estricto AT&T** compatible con FECHA/HORA como serial de Excel o texto, y DUR en segundos o HH:MM:SS.")

st.sidebar.header("Parámetros")
tz = st.sidebar.text_input("Zona horaria", value="America/Mazatlan")
show_preview = st.sidebar.checkbox("Mostrar preview", value=True)

files = st.file_uploader(
    "Arrastra y suelta archivos AT&T (XLS/XLSX/CSV/TXT)",
    type=["xlsx","xls","csv","txt"],
    accept_multiple_files=True,
)

# Asistente de mapeo manual (opcional)
manual_map: Dict[str, Optional[str]] = {}
use_manual = False

if files:
    first = files[0]
    headers = sniff_headers_from_bytes(first.getvalue(), first.name) or []
    with st.expander("🧭 Asistente de mapeo (opcional)", expanded=True):
        st.caption("Si AT&T cambió encabezados, asigna aquí. Se aplicará a todos los archivos antes de compilar.")
        cols = [None] + headers
        c1, c2, c3 = st.columns(3)
        with c1:
            a   = st.selectbox("Número A (MSISDN origen)", options=cols, index=0, key="map_a")
            dtc = st.selectbox("FechaHora combinada", options=cols, index=0, key="map_datetime")
            rid = st.selectbox("Registro_ID (NO)", options=cols, index=0, key="map_rid")
        with c2:
            b   = st.selectbox("Número B (MSISDN destino)", options=cols, index=0, key="map_b")
            f   = st.selectbox("Fecha (si viene separada)", options=cols, index=0, key="map_fecha")
            dur = st.selectbox("Duración (seg o HH:MM:SS)", options=cols, index=0, key="map_dur")
        with c3:
            t   = st.selectbox("Tipo (voz/datos/sms)", options=cols, index=0, key="map_tipo")
            h   = st.selectbox("Hora (si viene separada)", options=cols, index=0, key="map_hora")
            imei= st.selectbox("IMEI (opcional)", options=cols, index=0, key="map_imei")

        use_manual = st.checkbox("Usar este mapeo manual en la compilación", value=False)
        if use_manual:
            manual_map = {
                "numero_a": a, "numero_b": b, "tipo": t, "datetime": dtc,
                "fecha": f, "hora": h, "duracion_seg": dur, "registro_id": rid, "imei": imei,
            }
            pretty = {k: v for k, v in manual_map.items() if v}
            if pretty: st.success(f"Mapeo manual activo: {pretty}")
            else:      st.info("Mapeo manual activado sin asignaciones — se usarán heurísticas del motor estricto.")

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
                    raw_path = tempfile.NamedTemporaryFile(delete=False, dir=tmpdir, suffix=suffix).name
                    with open(raw_path, "wb") as w:
                        w.write(f.getvalue())

                    # Si eligieron mapeo manual: renombramos antes a canónico en CSV intermedio
                    if use_manual and any(manual_map.values()):
                        norm_path = tempfile.NamedTemporaryFile(delete=False, dir=tmpdir, suffix=".csv").name
                        rename_with_manual_map(raw_path, norm_path, manual_map)
                        tmp_paths.append(norm_path)
                    else:
                        tmp_paths.append(raw_path)

                with st.spinner("Compilando y normalizando (estricto AT&T)…"):
                    res = compile_att_sabanas_strict(tmp_paths, tz=tz)

                st.success(f"✅ Compilado: {len(res.df):,} filas | Archivos: {len(files)}")

                if show_preview:
                    st.subheader("Preview — Datos_Limpios")
                    st.dataframe(res.df.head(500), width="stretch")

                st.subheader("📜 LOG de compilación")
                st.dataframe(res.log, width="stretch")

                if res.stats:
                    st.subheader("📊 ESTADÍSTICAS")
                    for k, v in res.stats.items():
                        st.markdown(f"**{k}**")
                        st.dataframe(v, width="stretch")

                # Descargar Excel
                xlsx = build_excel(res.df, res.log, res.dupes, res.stats)
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
- Encabezados esperados (insensible a mayúsculas/acentos):  
`NO, SERV, T_REG, NUM_A, NUM_A_IMSI, NUM_A_IMEI, DEST, ID_DEST, HUSO, FECHA, HORA, DUR, USO_DW, USO_UP, ID_CELDA, LATITUD, LONGITUD, AZIMUTH, CAUSA_T, TIPO_COM, PAIS`.
- FECHA/HORA: acepta serial de Excel (número) o texto (`dd/mm/aaaa` y `HH:MM(:SS)`).  
- DUR: acepta segundos, fracción de día Excel (<1) o `HH:MM:SS`.  
- Dedupe especial *DATOS/min*: conserva el registro de mayor duración por par A/B por minuto.
""")
