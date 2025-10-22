# app_compilador_unico.py
# -*- coding: utf-8 -*-
import io
import re
import sys
import json
import math
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import streamlit as st
from dateutil import parser as dtparser
from difflib import get_close_matches

APP_TITLE = "Compilador Único → Esquema TELCEL_CRUDO (11 columnas)"
TARGET_COLUMNS = [
    "Telefono", "Tipo", "Numero A", "Numero B", "Fecha", "Hora",
    "Durac. Seg.", "IMEI", "LATITUD", "LONGITUD", "Azimuth"
]

# ---------- Utilidades de texto / matching ----------
def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def norm(s: str) -> str:
    s = strip_accents(s or "").lower().strip()
    s = re.sub(r"[\s\-\._/]+", " ", s)
    return s

SYNONYMS = {
    "Telefono": [
        "telefono", "teléfono", "msisdn", "linea", "línea", "subscriber", "abonado",
        "phone", "num subs", "numero de linea", "numero", "account msisdn"
    ],
    "Tipo": [
        "tipo", "service type", "call type", "evento", "registro", "cdr type", "bearer",
        "clase", "servicio", "trafico", "tráfico", "usage type"
    ],
    "Numero A": [
        "numero a", "a", "origen", "calling", "caller", "calling number", "originating",
        "ani", "calling party", "msc origin", "numero origen", "num a", "from", "source"
    ],
    "Numero B": [
        "numero b", "b", "destino", "called", "callee", "b-party", "terminating",
        "called number", "numero destino", "num b", "to", "target", "destination"
    ],
    "Fecha": [
        "fecha", "date", "start date", "call date", "fecha de la comunicacion",
        "event date", "fecha inicio", "dia"
    ],
    "Hora": [
        "hora", "time", "start time", "call time", "timestamp", "hora de la comunicacion",
        "hora inicio"
    ],
    "Durac. Seg.": [
        "durac seg", "duracion", "duración", "duration", "duration ms", "duration sec",
        "call duration", "tiempo", "dur", "duracion s", "dur ms", "dur sec"
    ],
    "IMEI": ["imei", "imeisv", "equipo", "device id", "handset", "terminal id"],
    "LATITUD": ["latitud", "lat", "latitude", "y", "coord y", "lat dms"],
    "LONGITUD": ["longitud", "lon", "long", "longitude", "x", "coord x", "lon dms"],
    "Azimuth": ["azimuth", "azimut", "bearing", "az", "angulo", "ángulo", "direction", "sector azimuth"],
}

VALUE_MAP_TIPO = {
    # normalizaciones frecuentes
    "voice": "VOZ",
    "voz": "VOZ",
    "llamada": "VOZ",
    "call": "VOZ",
    "data": "DATOS",
    "datos": "DATOS",
    "gprs": "DATOS",
    "4g": "DATOS",
    "lte": "DATOS",
    "sms": "MENSAJES 2 VÍAS",
    "mensajes": "MENSAJES 2 VÍAS",
    "2vias": "MENSAJES 2 VÍAS",
    "2 vias": "MENSAJES 2 VÍAS",
    "transfer": "TRANSFER",
    "desvio": "TRANSFER",
    "desvío": "TRANSFER",
    "forward": "TRANSFER",
}

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
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return None
    if isinstance(s, (int, float)):
        return float(s)
    txt = str(s).strip()
    m = RE_DMS.match(txt)
    if not m:
        # también intentamos "DD MM SS H"
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
    if pd.isna(x):
        return np.nan
    # si ya es número "normal"
    try:
        val = float(str(x).replace(",", "."))
        return val
    except Exception:
        pass
    # intentar DMS
    v = dms_to_decimal(str(x))
    return np.nan if v is None else v

def guess_units_and_to_seconds(series: pd.Series) -> pd.Series:
    """Convierte duraciones a segundos desde varias formas."""
    s = series.astype(str).str.strip()
    out = []
    for v in s:
        if v == "" or v.lower() in ("nan", "none"):
            out.append(np.nan); continue
        # HH:MM:SS o MM:SS
        if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", v):
            parts = [int(p) for p in v.split(":")]
            if len(parts) == 3:
                sec = parts[0]*3600 + parts[1]*60 + parts[2]
            else:
                sec = parts[0]*60 + parts[1]
            out.append(sec); continue
        # numérico: ¿ms o s?
        try:
            fv = float(v)
            if fv > 1e6:  # demasiado grande, probablemente ms acumulados
                out.append(int(round(fv/1000.0)))
            elif fv > 60000:  # > 60k => ms
                out.append(int(round(fv/1000.0)))
            elif fv > 10000 and fv < 60000:
                # entre 10k y 60k: ambiguo pero suele ser ms
                out.append(int(round(fv/1000.0)))
            else:
                # ya está en segundos
                out.append(int(round(fv)))
            continue
        except Exception:
            pass
        # palabras tipo "45s" o "120ms"
        m = re.match(r"^(\d+(?:\.\d+)?)(ms|s)?$", v)
        if m:
            num = float(m.group(1)); unit = (m.group(2) or "s").lower()
            out.append(int(round(num/1000.0 if unit == "ms" else num)))
            continue
        out.append(np.nan)
    return pd.Series(out, index=series.index, dtype="Int64").astype("float").astype("Int64")

def parse_any_datetime(x) -> Optional[datetime]:
    if pd.isna(x) or str(x).strip() == "":
        return None
    # pandas ya reconoce muchos formatos
    try:
        dt = pd.to_datetime(x, errors="raise", dayfirst=True)
        if isinstance(dt, pd.Series):
            dt = dt.iloc[0]
        return dt.to_pydatetime()
    except Exception:
        pass
    # dateutil como fallback
    try:
        return dtparser.parse(str(x), dayfirst=True, fuzzy=True)
    except Exception:
        return None

def split_fecha_hora(src_date: Optional[pd.Series], src_time: Optional[pd.Series]) -> Tuple[pd.Series, pd.Series]:
    """
    Dado columnas de fecha y hora (o una sola combinada), devuelve dos series string: Fecha (YYYY-MM-DD) y Hora (HH:MM:SS).
    Si el usuario eligió la misma columna para Fecha y Hora, se parte automáticamente.
    """
    n = max(len(src_date) if src_date is not None else 0, len(src_time) if src_time is not None else 0)
    fecha_out, hora_out = [], []
    for i in range(n):
        vd = src_date.iloc[i] if src_date is not None and i < len(src_date) else None
        vt = src_time.iloc[i] if src_time is not None and i < len(src_time) else None
        if src_date is None and src_time is not None:
            vd = vt
        if src_time is None and src_date is not None:
            vt = vd
        # si ambas referencian la misma fuente, parsear una sola
        val = vt if (src_date is None or src_time is None or src_date is src_time) else None
        if val is not None:
            dt = parse_any_datetime(val)
            if dt:
                fecha_out.append(dt.strftime("%Y-%m-%d"))
                hora_out.append(dt.strftime("%H:%M:%S"))
                continue
        # combinar cuando vienen separadas
        if (vd is not None) or (vt is not None):
            try:
                dt = parse_any_datetime(f"{vd} {vt}")
            except Exception:
                dt = None
            if not dt:
                dt = parse_any_datetime(vd) or parse_any_datetime(vt)
            if dt:
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
        vv = VALUE_MAP_TIPO.get(v)
        if vv:
            out.append(vv); continue
        # búsqueda por contiene
        found = None
        for k, val in VALUE_MAP_TIPO.items():
            if k in v:
                found = val; break
        out.append(found if found else s.astype(str).str.strip())
    return pd.Series(out, index=s.index).astype(str)

def normalize_msisdn_mx(s: pd.Series, enable: bool) -> pd.Series:
    if not enable:
        return s.astype(str).str.strip()
    out = []
    for v in s.astype(str):
        digits = re.sub(r"\D+", "", v)
        if digits == "":
            out.append("")
            continue
        # si ya viene con 52 al inicio y 10 siguientes
        if digits.startswith("52") and len(digits) >= 12:
            out.append("+" + digits)
            continue
        # si trae 10 dígitos locales, añadir +52
        if len(digits) == 10:
            out.append("+52" + digits)
            continue
        # otros largos: devolver con +
        out.append("+" + digits)
    return pd.Series(out, index=s.index)

# ---------- Sugerencia de mapeo ----------
def suggest_mapping(src_cols: List[str]) -> Dict[str, Optional[str]]:
    norm_src = {norm(c): c for c in src_cols}
    mapping = {t: None for t in TARGET_COLUMNS}

    for tgt, alias_list in SYNONYMS.items():
        chosen = None
        # match directo por alias
        for alias in alias_list:
            a = norm(alias)
            if a in norm_src:
                chosen = norm_src[a]; break
        if not chosen:
            # buscar coincidencia aproximada
            candidates = get_close_matches(
                norm("|".join(alias_list)), list(norm_src.keys()), n=1, cutoff=0.8
            )
            if candidates:
                chosen = norm_src[candidates[0]]
        # heurísticas simples A/B
        if not chosen:
            if tgt == "Numero A":
                for key in ("a", "a party", "a_number", "from"):
                    k = norm(key)
                    if k in norm_src:
                        chosen = norm_src[k]; break
            if tgt == "Numero B":
                for key in ("b", "b party", "b_number", "to"):
                    k = norm(key)
                    if k in norm_src:
                        chosen = norm_src[k]; break
        mapping[tgt] = chosen
    return mapping

# ---------- UI ----------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.caption("Sube cualquier CDR (CSV/Excel). Mapea columnas al esquema objetivo y normaliza formatos.")

with st.sidebar:
    st.markdown("### Opciones")
    opt_norm_msisdn = st.checkbox("Normalizar teléfonos a E.164 MX (+52…)", value=False)
    opt_fix_west = st.checkbox("Forzar LONGITUD negativa (hemisferio Oeste)", value=True)
    opt_drop_empty_coords = st.checkbox("Descartar filas sin coordenadas válidas", value=False)
    st.markdown("---")
    st.markdown("**Salida**")
    out_name = st.text_input("Nombre base del archivo de salida", value="CRUDO_UNIFICADO")
    st.markdown("---")
    st.markdown("**Ayuda rápida**")
    st.info(
        "- Si Fecha y Hora vienen en una sola columna, selecciónala en cualquiera de las dos; el parser la dividirá.\n"
        "- Duración acepta hh:mm:ss, mm:ss, ms o segundos.\n"
        "- LAT/LON en DMS tipo `32°27'04\"N` también se convierten."
    )

file = st.file_uploader("Sube un archivo .xlsx/.xls/.csv/.txt", type=["xlsx","xls","csv","txt"])

if file:
    # lectura
    ext = file.name.lower().split(".")[-1]
    try:
        if ext in ("xlsx", "xls"):
            sheet = st.text_input("Nombre de hoja (en blanco para la primera)", value="")
            if sheet.strip():
                df = pd.read_excel(file, sheet_name=sheet.strip())
            else:
                df = pd.read_excel(file)
        else:
            # autodetectar separador
            content = file.getvalue().decode("utf-8", errors="ignore")
            sep = "," if content.count(",") >= content.count("\t") else "\t"
            df = pd.read_csv(io.StringIO(content), sep=sep)
    except Exception as e:
        st.error(f"Error leyendo archivo: {e}")
        st.stop()

    st.subheader("1) Vista de columnas detectadas")
    st.write(list(df.columns))

    # sugerencia de mapeo
    suggested = suggest_mapping(list(df.columns))

    st.subheader("2) Mapeo de columnas (editable)")
    cols_ui = st.columns(3)
    mapping_user: Dict[str, Optional[str]] = {}
    choices = ["<vacío>"] + list(df.columns)

    for idx, tgt in enumerate(TARGET_COLUMNS):
        default = suggested.get(tgt)
        default_idx = choices.index(default) if default in choices else 0
        mapping_user[tgt] = cols_ui[idx % 3].selectbox(
            f"{tgt} ←", choices, index=default_idx
        )
        if mapping_user[tgt] == "<vacío>":
            mapping_user[tgt] = None

    st.subheader("3) Previsualización / Conversión")
    if st.button("Convertir y Normalizar", type="primary"):
        log_rows = []
        out = pd.DataFrame(columns=TARGET_COLUMNS)

        # helper para tomar serie fuente o serie vacía
        def col_or_empty(sel: Optional[str]) -> pd.Series:
            if sel is None or sel not in df.columns:
                return pd.Series([""] * len(df))
            return df[sel]

        s_tel = col_or_empty(mapping_user["Telefono"]).astype(str)
        s_tipo = col_or_empty(mapping_user["Tipo"]).astype(str)
        s_a   = col_or_empty(mapping_user["Numero A"]).astype(str)
        s_b   = col_or_empty(mapping_user["Numero B"]).astype(str)
        s_f   = col_or_empty(mapping_user["Fecha"])
        s_h   = col_or_empty(mapping_user["Hora"])
        s_dur = col_or_empty(mapping_user["Durac. Seg."])
        s_imei= col_or_empty(mapping_user["IMEI"]).astype(str)
        s_lat = col_or_empty(mapping_user["LATITUD"])
        s_lon = col_or_empty(mapping_user["LONGITUD"])
        s_azi = col_or_empty(mapping_user["Azimuth"])

        # Fecha/Hora
        fecha_out, hora_out = split_fecha_hora(s_f, s_h)

        # Duración → seg
        dur_out = guess_units_and_to_seconds(s_dur)

        # Tipo
        tipo_out = normalize_tipo(s_tipo)

        # Teléfonos
        tel_out = normalize_msisdn_mx(s_tel, opt_norm_msisdn)
        a_out   = normalize_msisdn_mx(s_a, opt_norm_msisdn)
        b_out   = normalize_msisdn_mx(s_b, opt_norm_msisdn)

        # Coordenadas
        lat_out = s_lat.apply(parse_maybe_dms)
        lon_out = s_lon.apply(parse_maybe_dms)
        if opt_fix_west:
            # México y región: longitudes negativas
            lon_out = -lon_out.abs()

        # Validaciones básicas
        lat_bad = (~lat_out.between(-90, 90)) & (~lat_out.isna())
        lon_bad = (~lon_out.between(-180, 180)) & (~lon_out.isna())
        if lat_bad.any() or lon_bad.any():
            log_rows.append({"tipo":"WARN","detalle":f"Coordenadas fuera de rango. LAT malas: {int(lat_bad.sum())}, LON malas: {int(lon_bad.sum())}"})

        if opt_drop_empty_coords:
            keep = ~(lat_out.isna() | lon_out.isna())
        else:
            keep = pd.Series([True]*len(df))

        # Azimuth → num
        def to_num(x):
            try:
                return float(str(x).replace(",", "."))
            except Exception:
                return np.nan
        azi_out = s_azi.apply(to_num)

        out = pd.DataFrame({
            "Telefono": tel_out,
            "Tipo": tipo_out,
            "Numero A": a_out,
            "Numero B": b_out,
            "Fecha": fecha_out,
            "Hora": hora_out,
            "Durac. Seg.": dur_out.astype("Int64"),
            "IMEI": s_imei.str.replace(r"\.0$", "", regex=True).str.strip(),
            "LATITUD": lat_out,
            "LONGITUD": lon_out,
            "Azimuth": azi_out
        })
        out = out[keep].reset_index(drop=True)

        # LOG de mapeo
        log_rows.append({"tipo":"INFO","detalle":f"Filas entrada: {len(df)}, filas salida: {len(out)}"})
        log_rows.append({"tipo":"INFO","detalle":f"Mapeo aplicado: {json.dumps(mapping_user, ensure_ascii=False)}"})

        # nulos por campo
        for c in TARGET_COLUMNS:
            log_rows.append({"tipo":"NULLS","detalle":f"{c}: {int(out[c].isna().sum() + (out[c]=='').sum())} nulos/vacíos"})

        log_df = pd.DataFrame(log_rows)

        st.success("Conversión realizada.")
        st.markdown("**Vista previa (primeras 100 filas):**")
        st.dataframe(out.head(100))

        # Descargar Excel con LOG
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
            out.to_excel(writer, sheet_name="CRUDO_UNIFICADO", index=False)
            log_df.to_excel(writer, sheet_name="LOG_Mapeo", index=False)
        st.download_button(
            label="⬇️ Descargar Excel (CRUDO_UNIFICADO + LOG)",
            data=bio.getvalue(),
            file_name=f"{out_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # También CSV
        st.download_button(
            label="⬇️ Descargar CSV (solo datos)",
            data=out.to_csv(index=False).encode("utf-8"),
            file_name=f"{out_name}.csv",
            mime="text/csv"
        )
else:
    st.info("Sube un CDR para comenzar. Soporta .xlsx/.xls/.csv/.txt.")
