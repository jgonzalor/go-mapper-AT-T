# app_compilador_unico.py
# -*- coding: utf-8 -*-
# =============================================================================
# MÓDULO: CONFIGURACIÓN INICIAL
# =============================================================================
import io
import re
import json
import math
import unicodedata
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Compilador Único → TELCEL_CRUDO (11 columnas)",
    layout="wide"
)

APP_TITLE = "Compilador Único → TELCEL_CRUDO (11 columnas)"
TARGET_COLUMNS = [
    "Telefono", "Tipo", "Numero A", "Numero B", "Fecha", "Hora",
    "Durac. Seg.", "IMEI", "LATITUD", "LONGITUD", "Azimuth"
]

# =============================================================================
# MÓDULO: SINÓNIMOS / DICCIONARIOS
# =============================================================================
def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def norm(s: str) -> str:
    s = strip_accents(s or "").lower().strip()
    s = re.sub(r"[\s\-\._/]+", " ", s)
    return s

# Sinónimos por campo destino (ajústalos libremente)
SYNONYMS: Dict[str, List[str]] = {
    "Telefono": [
        "telefono", "teléfono", "msisdn", "linea", "línea", "subscriber", "abonado",
        "phone", "num subs", "numero de linea", "numero", "account msisdn"
    ],
    "Tipo": [
        "tipo", "service type", "call type", "evento", "registro", "cdr type", "bearer",
        "clase", "servicio", "trafico", "tráfico", "usage type", "serv"  # ← SERV aquí
    ],
    "Dirección (ENT/SAL)": [
        "t_reg", "t reg", "sentido", "entrada/salida", "in/out", "direccion", "direction", "dir",
        "sentido llamada", "entrada", "salida", "ent", "sal"
    ],
    "Numero A": [
        "numero a", "a", "origen", "calling", "caller", "calling number", "originating",
        "ani", "calling party", "msc origin", "numero origen", "num a", "from", "source",
        "num_a", "a_party"
    ],
    "Numero B": [
        "numero b", "b", "destino", "called", "callee", "b-party", "terminating",
        "called number", "numero destino", "num b", "to", "target", "destination",
        "dest", "b_party"
    ],
    "Fecha": [
        "fecha", "date", "start date", "call date", "fecha de la comunicacion",
        "event date", "fecha inicio", "dia", "day"
    ],
    "Hora": [
        "hora", "time", "start time", "call time", "timestamp", "hora de la comunicacion",
        "hora inicio"
    ],
    "Durac. Seg.": [
        "durac seg", "duracion", "duración", "duration", "duration ms", "duration sec",
        "call duration", "tiempo", "dur", "duracion s", "dur ms", "dur sec", "durac seg."
    ],
    "IMEI": ["imei", "imeisv", "equipo", "device id", "handset", "terminal id", "num_a_imei"],
    "LATITUD": ["latitud", "lat", "latitude", "y", "coord y", "lat dms"],
    "LONGITUD": ["longitud", "lon", "long", "longitude", "x", "coord x", "lon dms"],
    "Azimuth": ["azimuth", "azimut", "bearing", "az", "angulo", "ángulo", "direction", "sector azimuth", "azimuth_gis", "azimuth°"],
}

VALUE_MAP_TIPO = {
    "voice": "VOZ", "voz": "VOZ", "llamada": "VOZ", "call": "VOZ",
    "data": "DATOS", "datos": "DATOS", "gprs": "DATOS", "4g": "DATOS", "lte": "DATOS",
    "sms": "MENSAJES 2 VÍAS", "mensajes": "MENSAJES 2 VÍAS", "esms": "MENSAJES 2 VÍAS",
    "2vias": "MENSAJES 2 VÍAS", "2 vias": "MENSAJES 2 VÍAS", "smst": "MENSAJES 2 VÍAS",
    "esms;smst": "MENSAJES 2 VÍAS",
    "transfer": "TRANSFER", "desvio": "TRANSFER", "desvío": "TRANSFER", "forward": "TRANSFER"
}

DIR_MAP = {
    "ent": "ENTRANTE", "entrada": "ENTRANTE", "in": "ENTRANTE", "incoming": "ENTRANTE",
    "sal": "SALIENTE", "salida": "SALIENTE", "out": "SALIENTE", "outgoing": "SALIENTE"
}

# =============================================================================
# MÓDULO: PARSERS Y NORMALIZADORES
# =============================================================================
NUM_RE = re.compile(r'[-+]?\d+(?:\.\d+)?')

def extract_number(text, prefer_last=True):
    """Extrae el primer/último número de un texto (sirve para valores en '[]' o 'a:b')."""
    if pd.isna(text):
        return np.nan
    nums = NUM_RE.findall(str(text))
    if not nums:
        return np.nan
    try:
        return float(nums[-1] if prefer_last else nums[0])
    except Exception:
        return np.nan

RE_DMS = re.compile(
    r"""^\s*
        (?P<deg>\d{1,3})[°\s]?
        (?P<min>\d{1,2})['\s]?
        (?P<sec>\d{1,2}(?:\.\d+)?)["\s]?
        (?P<hem>[NSEWnsew])?
        \s*$""",
    re.VERBOSE,
)

def dms_to_decimal(s: str) -> Optional[float]:
    """Convierte '32°27'04\"N' o '32 27 04 N' a decimal."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return None
    if isinstance(s, (int, float)):
        return float(s)
    txt = str(s).strip()
    m = RE_DMS.match(txt)
    if not m:
        parts = re.split(r"[^\d\.A-Za-z]+", txt)
        parts = [p for p in parts if p]
        if 3 <= len(parts) <= 4:
            try:
                deg = float(parts[0]); minu = float(parts[1]); sec = float(parts[2])
                hem = parts[3].upper() if len(parts) == 4 else None
                val = deg + minu/60 + sec/3600
                if hem in ("S","W"):
                    val = -abs(val)
                return val
            except Exception:
                return None
        return None
    deg = float(m.group("deg")); minu = float(m.group("min")); sec = float(m.group("sec"))
    hem = (m.group("hem") or "").upper()
    val = deg + minu/60 + sec/3600
    if hem in ("S","W"):
        val = -abs(val)
    return val

def parse_maybe_dms(x):
    """Intenta float directo; si no, intenta [a:b]/corchetes o DMS."""
    if pd.isna(x) or str(x).strip()=="":
        return np.nan
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        pass
    v = extract_number(x, prefer_last=True)
    if not (v is None or (isinstance(v, float) and math.isnan(v))):
        return v
    v = dms_to_decimal(str(x))
    return np.nan if v is None else v

def guess_units_and_to_seconds(series: pd.Series) -> pd.Series:
    """Convierte duraciones a segundos desde hh:mm:ss, mm:ss, ms, s."""
    s = series.astype(str).str.strip()
    out = []
    for v in s:
        if v == "" or v.lower() in ("nan", "none"):
            out.append(np.nan); continue
        if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", v):
            parts = [int(p) for p in v.split(":")]
            sec = parts[0]*3600 + parts[1]*60 + (parts[2] if len(parts)==3 else 0 if len(parts)==2 else 0)
            if len(parts)==2:
                sec = parts[0]*60 + parts[1]
            out.append(sec); continue
        try:
            fv = float(v)
            out.append(int(round(fv/1000.0)) if fv > 60000 else int(round(fv)))
            continue
        except Exception:
            pass
        m = re.match(r"^(\d+(?:\.\d+)?)(ms|s)?$", v)
        if m:
            num = float(m.group(1)); unit = (m.group(2) or "s").lower()
            out.append(int(round(num/1000.0 if unit == "ms" else num)))
            continue
        out.append(np.nan)
    return pd.Series(out, index=series.index, dtype="Int64").astype("float").astype("Int64")

def parse_any_datetime(x: str) -> Optional[pd.Timestamp]:
    if pd.isna(x) or str(x).strip() == "":
        return None
    try:
        return pd.to_datetime(x, errors="raise", dayfirst=True)
    except Exception:
        return None

def split_fecha_hora(src_date: Optional[pd.Series], src_time: Optional[pd.Series]) -> Tuple[pd.Series, pd.Series]:
    """Si Fecha/Hora están juntas, las divide; si están separadas, las combina."""
    n = max(len(src_date) if src_date is not None else 0, len(src_time) if src_time is not None else 0)
    fecha_out, hora_out = [], []
    for i in range(n):
        vd = src_date.iloc[i] if src_date is not None and i < len(src_date) else None
        vt = src_time.iloc[i] if src_time is not None and i < len(src_time) else None
        if src_date is None and src_time is not None:
            vd = vt
        if src_time is None and src_date is not None:
            vt = vd
        if (src_date is None) or (src_time is None) or (src_date is src_time):
            dt = parse_any_datetime(vt)
            if dt is None:
                dt = parse_any_datetime(vd)
            if dt is not None:
                fecha_out.append(dt.strftime("%Y-%m-%d"))
                hora_out.append(dt.strftime("%H:%M:%S"))
                continue
        val = f"{vd} {vt}".strip()
        dt = parse_any_datetime(val)
        if dt is None:
            dt = parse_any_datetime(vd) or parse_any_datetime(vt)
        if dt is not None:
            fecha_out.append(dt.strftime("%Y-%m-%d"))
            hora_out.append(dt.strftime("%H:%M:%S"))
            continue
        fecha_out.append("")
        hora_out.append("")
    return pd.Series(fecha_out), pd.Series(hora_out)

def normalize_tipo(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.lower().str.strip()
    out = []
    for v in x:
        if v in VALUE_MAP_TIPO:
            out.append(VALUE_MAP_TIPO[v]); continue
        found = None
        for k, val in VALUE_MAP_TIPO.items():
            if k in v:
                found = val; break
        out.append(found if found else s.astype(str).str.strip())
    return pd.Series(out, index=s.index).astype(str)

def normalize_direction(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.lower().str.strip()
    out = []
    for v in x:
        found = ""
        for k, val in DIR_MAP.items():
            if v == k or k in v:
                found = val; break
        out.append(found)
    return pd.Series(out, index=s.index).astype(str)

def compose_tipo(serv: pd.Series, dire: pd.Series, include_data_dir: bool) -> pd.Series:
    res = []
    for s, d in zip(serv.astype(str), dire.astype(str)):
        if d == "" or d.lower() == "nan":
            res.append(s)
        else:
            if s == "DATOS" and not include_data_dir:
                res.append(s)
            else:
                res.append(f"{s} {d}")
    return pd.Series(res, index=serv.index)

def normalize_msisdn_mx(s: pd.Series, enable: bool) -> pd.Series:
    if not enable:
        return s.astype(str).str.strip()
    out = []
    for v in s.astype(str):
        digits = re.sub(r"\D+", "", v)
        if digits == "":
            out.append("")
            continue
        if digits.startswith("52") and len(digits) >= 12:
            out.append("+" + digits); continue
        if len(digits) == 10:
            out.append("+52" + digits); continue
        out.append("+" + digits)
    return pd.Series(out, index=s.index)

def to_int64_digits(s: pd.Series) -> pd.Series:
    """Convierte a enteros (solo dígitos) preservando NA."""
    return pd.to_numeric(s.astype(str).str.replace(r"\D+", "", regex=True),
                         errors="coerce").astype("Int64")

# =============================================================================
# MÓDULO: AUTODETECCIÓN / SUGERENCIA DE MAPEO
# =============================================================================
from difflib import get_close_matches

def suggest_mapping(src_cols: List[str], targets: List[str]) -> Dict[str, Optional[str]]:
    norm_src = {norm(c): c for c in src_cols}
    mapping = {t: None for t in targets}

    for tgt in targets:
        alias_list = SYNONYMS.get(tgt, [])
        chosen = None
        for alias in alias_list:
            a = norm(alias)
            if a in norm_src:
                chosen = norm_src[a]; break
        if not chosen and alias_list:
            candidates = get_close_matches(
                norm("|".join(alias_list)), list(norm_src.keys()), n=1, cutoff=0.85
            )
            if candidates:
                chosen = norm_src[candidates[0]]
        # heurísticas A/B
        if not chosen and tgt == "Numero A":
            for key in ("a", "a party", "a_number", "from", "num_a"):
                k = norm(key)
                if k in norm_src: chosen = norm_src[k]; break
        if not chosen and tgt == "Numero B":
            for key in ("b", "b party", "b_number", "to", "dest"):
                k = norm(key)
                if k in norm_src: chosen = norm_src[k]; break
        mapping[tgt] = chosen
    return mapping

# =============================================================================
# MÓDULO: UI – SIDEBAR / OPCIONES
# =============================================================================
st.title(APP_TITLE)
st.caption("Mapea cualquier CDR (AT&T u otros) al esquema TELCEL_CRUDO de 11 columnas. "
           "Incluye normalización de teléfonos, fechas/horas, duración, coordenadas y combinación SERV+T_REG.")

with st.sidebar:
    st.markdown("### Opciones de normalización")
    opt_norm_msisdn = st.checkbox("Normalizar teléfonos a E.164 MX (+52…)", value=False)
    opt_fix_west = st.checkbox("Forzar LONGITUD negativa (hemisferio Oeste)", value=True)
    opt_drop_empty_coords = st.checkbox("Descartar filas sin coordenadas válidas", value=False)
    opt_prefer_last_in_brackets = st.checkbox("Si LAT/LON/Azimuth vienen como [a:b], tomar el último valor", value=True)

    st.markdown("---")
    st.markdown("### Composición de Tipo (SERV + T_REG)")
    opt_add_dir_to_tipo = st.checkbox("Agregar ENT/SAL a VOZ y MENSAJES 2 VÍAS", value=True)
    opt_add_dir_to_datos = st.checkbox("Agregar ENT/SAL también a DATOS", value=False)

    st.markdown("---")
    st.markdown("### Exportación")
    opt_export_ab_int = st.checkbox("Exportar Numero A / Numero B como enteros (sin decimales)", value=True)
    out_name = st.text_input("Nombre base del archivo de salida", value="CRUDO_UNIFICADO")

    st.info(
        "- Si Fecha y Hora vienen en una sola columna, selecciónala en cualquiera; el parser la dividirá.\n"
        "- Duración acepta hh:mm:ss, mm:ss, ms o segundos.\n"
        "- LAT/LON en DMS o con corchetes tipo `[23.24:23.25]` se convierten automáticamente."
    )

# =============================================================================
# MÓDULO: CARGA DE ARCHIVO
# =============================================================================
file = st.file_uploader("Sube un CDR .xlsx/.xls/.csv/.txt", type=["xlsx","xls","csv","txt"])

def read_any(file):
    ext = file.name.lower().split(".")[-1]
    if ext in ("xlsx", "xls"):
        xls = pd.ExcelFile(file)
        sh = st.selectbox("Hoja de Excel", xls.sheet_names, index=0)
        df = pd.read_excel(file, sheet_name=sh)
        return df
    else:
        content = file.getvalue().decode("utf-8", errors="ignore")
        sep_counts = {",": content.count(","), "\t": content.count("\t"), ";": content.count(";")}
        sep = max(sep_counts, key=sep_counts.get)
        df = pd.read_csv(io.StringIO(content), sep=sep)
        return df

if file:
    try:
        df = read_any(file)
    except Exception as e:
        st.error(f"Error leyendo archivo: {e}")
        st.stop()

    st.subheader("1) Columnas detectadas")
    st.write(list(df.columns))

    # =============================================================================
    # MÓDULO: EDITOR DE MAPEO
    # =============================================================================
    st.subheader("2) Mapeo de columnas (editable)")

    # Campos a mapear (incluimos Dirección adicional para T_REG)
    MAPPING_FIELDS = [
        "Telefono", "Tipo", "Dirección (ENT/SAL)", "Numero A", "Numero B", "Fecha",
        "Hora", "Durac. Seg.", "IMEI", "LATITUD", "LONGITUD", "Azimuth"
    ]

    suggested = suggest_mapping(list(df.columns), MAPPING_FIELDS)
    cols_ui = st.columns(3)
    mapping_user: Dict[str, Optional[str]] = {}
    choices = ["<vacío>"] + list(df.columns)

    for idx, tgt in enumerate(MAPPING_FIELDS):
        default = suggested.get(tgt)
        default_idx = choices.index(default) if default in choices else 0
        mapping_user[tgt] = cols_ui[idx % 3].selectbox(
            f"{tgt} ←", choices, index=default_idx
        )
        if mapping_user[tgt] == "<vacío>":
            mapping_user[tgt] = None

    # Guardar receta
    recipe = {"target": "TELCEL_CRUDO", "mapping": mapping_user}
    recipe_bytes = json.dumps(recipe, ensure_ascii=False, indent=2).encode("utf-8")
    st.download_button("💾 Descargar receta JSON", recipe_bytes, file_name="receta_compilador.json")

    # =============================================================================
    # MÓDULO: CONVERSIÓN / NORMALIZACIÓN
    # =============================================================================
    st.subheader("3) Conversión / Normalización")

    if st.button("Convertir y Normalizar", type="primary"):
        log_rows = []

        def col_or_empty(sel: Optional[str]) -> pd.Series:
            if sel is None or sel not in df.columns:
                return pd.Series([""] * len(df))
            return df[sel]

        s_tel = col_or_empty(mapping_user["Telefono"]).astype(str)
        s_tipo_serv = col_or_empty(mapping_user["Tipo"]).astype(str)
        s_dir = col_or_empty(mapping_user["Dirección (ENT/SAL)"]).astype(str)
        s_a   = col_or_empty(mapping_user["Numero A"]).astype(str)
        s_b   = col_or_empty(mapping_user["Numero B"]).astype(str)
        s_f   = col_or_empty(mapping_user["Fecha"])
        s_h   = col_or_empty(mapping_user["Hora"])
        s_dur = col_or_empty(mapping_user["Durac. Seg."])
        s_imei= col_or_empty(mapping_user["IMEI"]).astype(str)
        s_lat = col_or_empty(mapping_user["LATITUD"])
        s_lon = col_or_empty(mapping_user["LONGITUD"])
        s_azi = col_or_empty(mapping_user["Azimuth"])

        # Fecha y Hora
        fecha_out, hora_out = split_fecha_hora(s_f, s_h)

        # Duración
        dur_out = guess_units_and_to_seconds(s_dur)

        # Tipo (SERV) + Dirección (T_REG)
        tipo_serv = normalize_tipo(s_tipo_serv)
        dire_norm = normalize_direction(s_dir)
        if opt_add_dir_to_tipo or opt_add_dir_to_datos:
            tipo_out = compose_tipo(tipo_serv, dire_norm, include_data_dir=opt_add_dir_to_datos)
        else:
            tipo_out = tipo_serv

        # Teléfonos (normalización opcional a E.164)
        tel_out = normalize_msisdn_mx(s_tel, opt_norm_msisdn)
        a_out   = normalize_msisdn_mx(s_a, opt_norm_msisdn)
        b_out   = normalize_msisdn_mx(s_b, opt_norm_msisdn)

        # Coordenadas (corchetes/DMS)
        def coord_any(x):
            v = extract_number(x, prefer_last=opt_prefer_last_in_brackets)
            if pd.isna(v):
                v = parse_maybe_dms(x)
            return v

        lat_out = s_lat.apply(coord_any)
        lon_out = s_lon.apply(coord_any)

        # 0 como NaN
        lat_out = lat_out.where(lat_out != 0, np.nan)
        lon_out = lon_out.where(lon_out != 0, np.nan)

        # Hemisferio Oeste (MX)
        if opt_fix_west:
            lon_out = -lon_out.abs()

        # Azimuth
        azi_out = s_azi.apply(lambda x: extract_number(x, prefer_last=opt_prefer_last_in_brackets)).astype(float)

        out = pd.DataFrame({
            "Telefono": tel_out,
            "Tipo": tipo_out,
            "Numero A": a_out,
            "Numero B": b_out,
            "Fecha": fecha_out,
            "Hora": hora_out,
            "Durac. Seg.": dur_out.astype("Int64"),
            "IMEI": s_imei.str.replace(r"\.0$", "", regex=True).str.strip().replace({"nan": ""}),
            "LATITUD": lat_out,
            "LONGITUD": lon_out,
            "Azimuth": azi_out
        }).reset_index(drop=True)

        # Validaciones y filtros
        lat_bad = (~out["LATITUD"].between(-90, 90)) & (~out["LATITUD"].isna())
        lon_bad = (~out["LONGITUD"].between(-180, 180)) & (~out["LONGITUD"].isna())
        if lat_bad.any() or lon_bad.any():
            log_rows.append({"tipo":"WARN","detalle":f"Coordenadas fuera de rango. LAT malas: {int(lat_bad.sum())}, LON malas: {int(lon_bad.sum())}"})

        if opt_drop_empty_coords:
            out = out[~(out["LATITUD"].isna() | out["LONGITUD"].isna())].reset_index(drop=True)

        # Numeración A/B como enteros (sin decimales) si se solicita
        if opt_export_ab_int:
            out["Numero A"] = to_int64_digits(out["Numero A"])
            out["Numero B"] = to_int64_digits(out["Numero B"])

        # LOG
        log_rows.append({"tipo":"INFO","detalle":f"Filas entrada: {len(df)}, filas salida: {len(out)}"})
        log_rows.append({"tipo":"INFO","detalle":f"Mapeo aplicado: {json.dumps(mapping_user, ensure_ascii=False)}"})
        for c in TARGET_COLUMNS:
            n_nulls = int(out[c].isna().sum() + (out[c] == "").sum()) if c in out.columns else 0
            log_rows.append({"tipo":"NULLS","detalle":f"{c}: {n_nulls} nulos/vacíos"})
        log_df = pd.DataFrame(log_rows)

        st.success("Conversión realizada.")
        st.markdown("**Vista previa (primeras 100 filas):**")
        st.dataframe(out.head(100), use_container_width=True)

        # =============================================================================
        # MÓDULO: EXPORTACIÓN
        # =============================================================================
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
            out.to_excel(writer, sheet_name="CRUDO_UNIFICADO", index=False)
            log_df.to_excel(writer, sheet_name="LOG_Mapeo", index=False)

            # Formato sin decimales para Numero A/B si procede
            workbook = writer.book
            ws = writer.sheets["CRUDO_UNIFICADO"]
            fmt_int = workbook.add_format({"num_format": "0"})
            try:
                col_a = out.columns.get_loc("Numero A")
                col_b = out.columns.get_loc("Numero B")
                ws.set_column(col_a, col_a, 18, fmt_int)
                ws.set_column(col_b, col_b, 18, fmt_int)
            except Exception:
                pass  # por si los nombres cambian

        st.download_button(
            label="⬇️ Descargar Excel (CRUDO_UNIFICADO + LOG)",
            data=bio.getvalue(),
            file_name=f"{out_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        st.download_button(
            label="⬇️ Descargar CSV (solo datos)",
            data=out.to_csv(index=False).encode("utf-8"),
            file_name=f"{out_name}.csv",
            mime="text/csv"
        )

else:
    st.info("Sube un CDR para comenzar. Soporta .xlsx/.xls/.csv/.txt.")
