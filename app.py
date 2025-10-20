# app.py — Go Mapper AT&T (single-file v2.3, con “modo estricto AT&T” y UI)

from __future__ import annotations
import os, io, re, tempfile
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Go Mapper — Compilador AT&T (single-file)", layout="wide")

# --------- OLC opcional ----------
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
        # Si viene como número ya son segundos (o Excel-horas fraccionales; lo ajustamos abajo)
        return int(round(float(val)))
    s = str(val).strip()
    if not s: return None
    if re.match(r"^\d{1,2}:[0-5]\d:[0-5]\d$", s):
        h, m, sec = s.split(":"); return int(h)*3600 + int(m)*60 + int(sec)
    if re.match(r"^[0-5]?\d:[0-5]\d$", s):
        m, sec = s.split(":"); return int(m)*60 + int(sec)
    s2 = re.sub(r"[^0-9]", "", s)
    if s2.isdigit(): return int(s2)
    return None

def _excel_days_to_datetime(series: pd.Series) -> pd.Series:
    # Excel base 1899-12-30
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

# Mapeo exacto (normalizado) de tus encabezados AT&T → columnas canónicas
STRICT_ATT_MAP = {
    "no": "registro_id",
    "serv": "tipo",
    "t reg": "tipo",
    "t_reg": "tipo",
    "tipo com": "tipo",
    "tipo_com": "tipo",
    "num a": "numero_a",
    "num_a": "numero_a",
    "num a imsi": "imsi",
    "num_a_imsi": "imsi",
    "num a imei": "imei",
    "num_a_imei": "imei",
    "dest": "numero_b",
    "id dest": "numero_b",
    "id_dest": "numero_b",
    "fecha": "fecha",
    "hora": "hora",
    "dur": "duracion_seg",
    "id celda": "ci_eci",
    "id_celda": "ci_eci",
    "latitud": "latitud",
    "longitud": "longitud",
    "azimuth": "azimuth_deg",
    # extras que podemos conservar si llegan:
    "huso": "huso",
    "uso dw": "uso_dw",
    "uso_up": "uso_up",
    "uso up": "uso_up",
    "causa t": "causa_t",
    "causa_t": "causa_t",
    "pais": "pais",
}

# Tokens para clasificar Tipo
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
    # 1) Renombrado exacto por normalización
    orig_cols = list(raw_df.columns)
    norm_map = {_norm_colname(c): c for c in raw_df.columns}
    rename = {}
    for norm, orig in norm_map.items():
        if norm in STRICT_ATT_MAP:
            rename[orig] = STRICT_ATT_MAP[norm]
    df = raw_df.rename(columns=rename).copy()

    # 2) Campos básicos
    df["Archivo_Origen"] = df.get("Archivo_Origen")  # lo setea el pipeline
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

    # Datetime a partir de FECHA/HORA o DATETIME si viniera
    fecha = df.get("fecha")
    hora  = df.get("hora")
    dt = pd.Series([pd.NaT]*len(df), dtype="datetime64[ns]")

    if fecha is not None:
        # Excel serial o texto
        f_num_mask = pd.to_numeric(fecha, errors="coerce").notna()
        dt[f_num_mask] = _excel_days_to_datetime(pd.to_numeric(fecha[f_num_mask], errors="coerce"))
        dt[~f_num_mask] = pd.to_datetime(fecha[~f_num_mask], errors="coerce", dayfirst=True)

        if hora is not None:
            # Hora como texto HH:MM:SS
            h = hora.astype(str).str.strip()
            hhmmss = h.str.match(r"^\d{1,2}:[0-5]\d(:[0-5]\d)?$")
            # Hora numérica: fracción de día o segundos
            h_num = pd.to_numeric(h, errors="coerce")

            # HH:MM(:SS)
            dt[hhmmss] = pd.to_datetime(
                dt[hhmmss].dt.strftime("%Y-%m-%d") + " " + h[hhmmss],
                errors="coerce",
            )

            # Numérico (fracción de día si 0–1; si >1 lo tratamos como segundos)
            idx_num = h_num.notna() & dt.notna()
            if idx_num.any():
                frac = h_num.between(0, 1, inclusive="both")
                secs = (~frac) & idx_num
                if (idx_num & frac).any():
                    add = pd.to_timedelta((h_num[idx_num & frac] * 86400).round().astype(int), unit="s")
                    dt[idx_num & frac] = dt[idx_num & frac] + add
                if (idx_num & secs).any():
                    add = pd.to_timedelta(h_num[idx_num & secs].round().astype(int), unit="s")
                    dt[idx_num & secs] = dt[idx_num & secs] + add
    else:
        # Si no hay 'fecha', probamos 'datetime' si venía ya renombrado
        if "datetime" in df.columns:
            dt = pd.to_datetime(df["datetime"], errors="coerce", dayfirst=True)

    df["Datetime"] = dt.apply(lambda x: _to_local_naive(x, tz) if pd.notna(x) else x)

    # Duración
    if "duracion_seg" in df.columns:
        dur = df["duracion_seg"].apply(_parse_duration_to_seconds)
        # Si parece fracción de día (muchos < 1), convertir a segundos
        as_num = pd.to_numeric(df["duracion_seg"], errors="coerce")
        frac_mask = as_num.notna() & (as_num.between(0, 1, inclusive="both"))
        if frac_mask.any():
            dur = dur.fillna(0)
            dur.loc[frac_mask] = (as_num.loc[frac_mask] * 86400).round().astype(int)
        df["Duración (seg)"] = dur
    elif "dur" in df.columns:
        dur = df["dur"].apply(_parse_duration_to_seconds)
        df["Duración (seg)"] = dur
    else:
        df["Duración (seg)"] = None

    # IDs
    df["IMEI"] = df.get("imei")
    df["IMSI"] = df.get("imsi")

    # Radio / celda
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
    df = df[cols_final]
    return df

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

# ==================== UI ====================

st.title("📞 Go Mapper — Compilador AT&T (single-file)")
st.caption("Ruta rápida con mapeo **estricto AT&T** y tolerante a Excel (FECHA/HORA numéricas, HH:MM:SS, DUR en segundos o HH:MM:SS).")

st.sidebar.header("Parámetros")
tz = st.sidebar.text_input("Zona horaria", value="America/Mazatlan")
show_preview = st.sidebar.checkbox("Mostrar preview", value=True)

files = st.file_uploader(
    "Arrastra y suelta archivos AT&T (XLS/XLSX/CSV/TXT)",
    type=["xlsx","xls","csv","txt"],
    accept_multiple_files=True,
)

left, right = st.columns(2)
go = left.button("🧩 Compilar (modo estricto AT&T)", type="primary")
clear = right.button("🗑️ Limpiar sesión")

if clear:
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()

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
- Encabezados esperados por el modo estricto (insensible a may/minúsculas y acentos):  
`NO, SERV, T_REG, NUM_A, NUM_A_IMSI, NUM_A_IMEI, DEST, ID_DEST, HUSO, FECHA, HORA, DUR, USO_DW, USO_UP, ID_CELDA, LATITUD, LONGITUD, AZIMUTH, CAUSA_T, TIPO_COM, PAIS`.
- FECHA/HORA: acepta serial de Excel (número) o texto `dd/mm/aaaa` y `HH:MM(:SS)`.  
- DUR: acepta segundos, fracción de día Excel (<1) o `HH:MM:SS`.
""")
