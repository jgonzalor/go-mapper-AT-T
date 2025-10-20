"""
Go Mapper — Compilador AT&T (v2)

Módulo para compilar/normalizar múltiples sábanas de llamada (AT&T México) en un
esquema unificado compatible con Go Mapper / Limpieza.

Novedades v2
- Soporta columna "NO" (u homólogos) como Registro_ID y la conserva.
- Si no viene, genera un consecutivo global 1..N.
- Campo opcional Archivo_Origen para auditoría multiarchivo.

Características clave
- Ingesta de varios archivos Excel/CSV/TXT (detecta separadores y codificación básica).
- Normalización de columnas con sinónimos comunes de AT&T.
- Parser robusto de fecha/hora (dd/mm/yyyy, dd-mm-yyyy, yyyy-mm-dd; 12/24 h; AM/PM;
  columnas separadas o combinadas) con TZ local a naive.
- Estandariza Tipo a: "VOZ ENTRANTE", "VOZ SALIENTE", "MENSAJES 2 VÍAS", "DATOS", "TRANSFER".
- Deriva "Dirección del tráfico (VOZ)" = ENT/ SAL cuando aplica.
- Dedupe especial para DATOS por minuto (conserva la mayor duración por pareja A/B).
- Maneja IMEI/IMSI, Celda (LAC/TAC + CI/ECI), Tecnología, Azimuth, etc., si existen.
- Plus Codes opcionales a partir de Lat/Lon (si está disponible `openlocationcode`).
- Exporta a Excel con hojas: Datos_Limpios, LOG_Compilación, Duplicados, ESTADISTICAS.

Uso
-----
from modules.att_compiler import compile_att_sabanas

res = compile_att_sabanas(
    file_paths=["/ruta/att_enero.xlsx", "/ruta/att_feb.csv"],
    tz="America/Mazatlan",
    out_xlsx="/salidas/ATT_compilado.xlsx"
)

Devuelve `CompileResult` con:
- df: DataFrame normalizado
- log: DataFrame con bitácora
- dupes: DataFrame de registros duplicados (removidos por regla DATOS/minuto)
- stats: dict con DataFrames estadísticos

Requisitos
----------
- pandas, numpy
- (Opcional) openlocationcode para PLUS_CODE (`pip install openlocationcode`).

Nota: Este módulo NO hace geocoding externo.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

import numpy as np
import pandas as pd

try:
    # openlocationcode es opcional
    from openlocationcode import openlocationcode as olc
    _HAS_OLC = True
except Exception:
    _HAS_OLC = False

# ==========================
# Utilidades de normalización
# ==========================

def _strip_accents(text: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def _norm_colname(name: str) -> str:
    name = _strip_accents(str(name)).strip().lower()
    name = re.sub(r"\s+", " ", name)
    name = name.replace("/", " ")
    name = name.replace("-", " ")
    name = name.replace("(", " ").replace(")", " ")
    name = name.replace("[", " ").replace("]", " ")
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

# Sinónimos -> nombre estándar
COLUMN_SYNONYMS: Dict[str, List[str]] = {
    # Registro (AT&T suele usar 'NO', 'No.', etc.)
    "registro_id": [
        "no", "no.", "n°", "numero", "num", "nro", "id", "folio", "consecutivo", "id registro"
    ],
    # Núcleos A/B, tipo, fecha/hora
    "numero_a": [
        "numero a", "msisdn a", "origen", "abonado a", "a", "numero de origen",
        "num a", "telefono a", "tel a", "caller a"
    ],
    "numero_b": [
        "numero b", "msisdn b", "destino", "abonado b", "b", "numero de destino",
        "num b", "telefono b", "tel b", "called b"
    ],
    "tipo": [
        "tipo", "tipo de registro", "tipo de evento", "servicio", "call type",
        "event type", "clase de servicio"
    ],
    "fecha": ["fecha", "date", "start date", "fecha inicio", "fecha llamada"],
    "hora": ["hora", "time", "start time", "hora inicio", "hora llamada"],
    "datetime": ["fecha hora", "fecha y hora", "datetime", "inicio", "timestamp"],
    "duracion_seg": [
        "duracion seg", "duracion", "duracion segundos", "duration", "segundos",
        "tiempo de conexion", "tiempo", "duracion s"
    ],
    # Radio / celda
    "lac_tac": ["lac", "tac", "lac tac", "area", "lac t", "tac lac"],
    "ci_eci": ["ci", "eci", "cell id", "cid", "cellid", "id de celda"],
    "tecnologia": ["tecnologia", "tecnologia radio", "radio", "rat", "2g 3g 4g 5g"],
    "celda": ["celda", "site", "sitio", "e nodeb", "enodeb", "sector"],
    "azimuth_deg": ["azimuth deg", "azimuth", "azimut", "angulo", "bearing"],
    # Identificadores
    "imei": ["imei"],
    "imsi": ["imsi"],
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

# ==========================
# Mapeo de columnas
# ==========================

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

# ==========================
# Lectura de archivos heterogéneos
# ==========================

def _read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xls", ".xlsx", ".xlsm"}:
        return pd.read_excel(path)
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

# ==========================
# Parsing de fechas/horas y duración
# ==========================

def _parse_duration_to_seconds(val: Any) -> Optional[int]:
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)) and not pd.isna(val):
        return int(round(float(val)))
    s = str(val).strip()
    if not s:
        return None
    if re.match(r"^\d{1,2}:[0-5]\d:[0-5]\d$", s):
        h, m, sec = s.split(":")
        return int(h) * 3600 + int(m) * 60 + int(sec)
    if re.match(r"^[0-5]?\d:[0-5]\d$", s):
        m, sec = s.split(":")
        return int(m) * 60 + int(sec)
    s2 = re.sub(r"[^0-9]", "", s)
    if s2.isdigit():
        return int(s2)
    return None


def _to_local_naive(ts: pd.Timestamp, tz: Optional[str]) -> pd.Timestamp:
    if tz is None or pd.isna(ts):
        return ts if getattr(ts, "tzinfo", None) is None else ts.tz_localize(None)
    if getattr(ts, "tzinfo", None) is None:
        # Naive -> local tz -> quitar tz
        return ts.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT").tz_convert(tz).tz_localize(None)
    # Aware -> convertir a tz local y quitar tz
    return ts.tz_convert(tz).tz_localize(None)


def _combine_datetime(row: pd.Series, cols: Dict[str, str], tz: Optional[str]) -> Optional[pd.Timestamp]:
    # 1) Columna combinada
    if "datetime" in cols:
        dt = pd.to_datetime(row[cols["datetime"]], errors="coerce", dayfirst=True, infer_datetime_format=True)
        if pd.notna(dt):
            return _to_local_naive(dt, tz)
    # 2) Fecha + hora
    f = row[cols["fecha"]] if "fecha" in cols else None
    h = row[cols["hora"]] if "hora" in cols else None
    if f is not None and h is not None:
        combo = f"{f} {h}"
        dt = pd.to_datetime(combo, errors="coerce", dayfirst=True, infer_datetime_format=True)
        if pd.notna(dt):
            return _to_local_naive(dt, tz)
    # 3) Solo fecha
    if "fecha" in cols:
        dt = pd.to_datetime(row[cols["fecha"]], errors="coerce", dayfirst=True, infer_datetime_format=True)
        if pd.notna(dt):
            return _to_local_naive(dt, tz)
    return None

# ==========================
# Normalización de Tipo / Dirección VOZ
# ==========================

_VOZ_OUT_TOKENS = {"mo", "saliente", "orig", "out", "originating"}
_VOZ_IN_TOKENS = {"mt", "entrante", "term", "in", "terminating"}
_MSG_TOKENS = {"sms", "mensaje", "mensajes", "2 vias"}
_DATA_TOKENS = {"gprs", "datos", "data", "internet"}
_TRANSF_TOKENS = {"transfer", "desvio", "call forward", "cfu", "cfnr", "cfnry"}


def _normalize_tipo(raw: Any) -> Optional[str]:
    if pd.isna(raw):
        return None
    s = _norm_colname(str(raw))
    if any(tok in s for tok in _TRANSF_TOKENS):
        return "TRANSFER"
    if any(tok in s for tok in _MSG_TOKENS):
        return "MENSAJES 2 VÍAS"
    if any(tok in s for tok in _DATA_TOKENS):
        return "DATOS"
    if any(tok in s for tok in _VOZ_OUT_TOKENS):
        return "VOZ SALIENTE"
    if any(tok in s for tok in _VOZ_IN_TOKENS):
        return "VOZ ENTRANTE"
    if "voz" in s or "llamada" in s or "call" in s:
        return "VOZ SALIENTE"
    return None


def _dir_trafico_voz(tipo: Optional[str]) -> Optional[str]:
    if tipo == "VOZ SALIENTE":
        return "SALIENTE"
    if tipo == "VOZ ENTRANTE":
        return "ENTRANTE"
    return None

# ==========================
# Plus code helper
# ==========================

def _maybe_plus(lat: Any, lon: Any) -> Optional[str]:
    if not _HAS_OLC:
        return None
    try:
        latf = float(lat)
        lonf = float(lon)
        if not (-90 <= latf <= 90 and -180 <= lonf <= 180):
            return None
        return olc.encode(latf, lonf, codeLength=10)
    except Exception:
        return None

# ==========================
# Estadísticas
# ==========================

def _build_stats(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    stats: Dict[str, pd.DataFrame] = {}
    if df.empty:
        return stats
    try:
        sal = df[df["Tipo"] == "VOZ SALIENTE"].groupby(["Número A", "Número B"], dropna=False).size().reset_index(name="Conteo").sort_values("Conteo", ascending=False).head(10)
        stats["Top10_Salientes"] = sal
    except Exception:
        pass
    try:
        ent = df[df["Tipo"] == "VOZ ENTRANTE"].groupby(["Número A", "Número B"], dropna=False).size().reset_index(name="Conteo").sort_values("Conteo", ascending=False).head(10)
        stats["Top10_Entrantes"] = ent
    except Exception:
        pass
    if "IMEI" in df.columns:
        try:
            imei = df.dropna(subset=["IMEI"]).groupby("IMEI").size().reset_index(name="Registros").sort_values("Registros", ascending=False).head(20)
            stats["Top_IMEI"] = imei
        except Exception:
            pass
    if {"LAC_TAC", "CI_ECI"}.issubset(df.columns):
        try:
            for t in ["DATOS", "VOZ ENTRANTE", "VOZ SALIENTE", "MENSAJES 2 VÍAS", "TRANSFER"]:
                sub = df[df["Tipo"] == t]
                if not sub.empty:
                    k = sub.groupby(["LAC_TAC", "CI_ECI"], dropna=False).size().reset_index(name="Eventos").sort_values("Eventos", ascending=False).head(10)
                    stats[f"Antenas_TOP__{t}"] = k
        except Exception:
            pass
    return stats

# ==========================
# Hoja de salida
# ==========================

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

# ==========================
# Pipeline principal
# ==========================

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
    """
    Compila y normaliza múltiples archivos de sábanas AT&T.

    :param file_paths: lista de rutas a archivos .xlsx/.xls/.csv/.txt
    :param tz: zona horaria local (para normalizar Datetime). Ej.: 'America/Mazatlan'
    :param out_xlsx: ruta de salida para Excel compilado (opcional)
    :return: CompileResult con DataFrames y ruta de salida
    """
    frames: List[pd.DataFrame] = []
    logs: List[Dict[str, Any]] = []

    for path in file_paths:
        try:
            raw = _read_any(path)
            orig_cols = list(raw.columns)
            mapping = _detect_columns(raw)

            # Renombrar columnas mapeadas -> canonical
            rename_map = {v: k for k, v in mapping.items()}
            df = raw.rename(columns=rename_map).copy()

            # Archivo de origen para auditoría
            df["Archivo_Origen"] = os.path.basename(path)

            # Duración a segundos
            if "duracion_seg" in df.columns:
                df["duracion_seg"] = df["duracion_seg"].apply(_parse_duration_to_seconds)

            # Datetime robusto
            if "fecha" in df.columns or "datetime" in df.columns:
                df["Datetime"] = df.apply(lambda r: _combine_datetime(r, mapping, tz), axis=1)

            # Tipo normalizado
            if "tipo" in df.columns:
                df["Tipo"] = df["tipo"].apply(_normalize_tipo)
            else:
                df["Tipo"] = None

            # Dirección tráfico VOZ
            df["Dirección del tráfico (VOZ)"] = df["Tipo"].apply(_dir_trafico_voz)

            # Registro_ID (si viene 'NO' u otro sinónimo)
            df["Registro_ID"] = pd.to_numeric(df.get("registro_id"), errors="coerce").astype("Int64") if "registro_id" in df.columns else pd.Series([pd.NA]*len(df), dtype="Int64")

            # Número A/B normalizados
            df["Número A"] = df.get("numero_a")
            df["Número B"] = df.get("numero_b")

            # Radio/celda/tec
            df["LAC_TAC"] = df.get("lac_tac")
            df["CI_ECI"] = df.get("ci_eci")
            df["Tecnología"] = df.get("tecnologia")
            df["Celda"] = df.get("celda")
            df["Azimuth_deg"] = pd.to_numeric(df.get("azimuth_deg"), errors="coerce") if "azimuth_deg" in df.columns else None

            # Identificadores
            df["IMEI"] = df.get("imei")
            df["IMSI"] = df.get("imsi")

            # Geografía
            df["Latitud"] = pd.to_numeric(df.get("latitud"), errors="coerce") if "latitud" in df.columns else None
            df["Longitud"] = pd.to_numeric(df.get("longitud"), errors="coerce") if "longitud" in df.columns else None

            # Plus code si hay lat/lon
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

            # Nombre (si ya viene)
            df["PLUS_CODE_NOMBRE"] = df.get("plus_code_nombre") if "plus_code_nombre" in df.columns else df.get("direccion")

            # Duración (seg) salida final
            df["Duración (seg)"] = df.get("duracion_seg")

            # Operador
            df["Operador"] = "AT&T"

            # Reorden final de columnas principales
            cols_final = [
                "Registro_ID", "Archivo_Origen",
                "Operador", "Tipo", "Dirección del tráfico (VOZ)",
                "Número A", "Número B", "Datetime", "Duración (seg)",
                "IMEI", "IMSI", "Tecnología",
                "LAC_TAC", "CI_ECI", "Celda", "Azimuth_deg",
                "Latitud", "Longitud", "PLUS_CODE", "PLUS_CODE_NOMBRE",
            ]
            for c in cols_final:
                if c not in df.columns:
                    df[c] = None
            df = df[cols_final]

            frames.append(df)
            logs.append({
                "archivo": os.path.basename(path),
                "filas": len(df),
                "columnas_detectadas": ", ".join(sorted(mapping.keys())),
                "columnas_origen": ", ".join(map(str, orig_cols)),
            })
        except Exception as e:
            logs.append({
                "archivo": os.path.basename(path),
                "error": str(e)
            })

    if not frames:
        empty = pd.DataFrame(columns=[
            "Registro_ID", "Archivo_Origen", "Operador", "Tipo", "Dirección del tráfico (VOZ)", "Número A", "Número B",
            "Datetime", "Duración (seg)", "IMEI", "IMSI", "Tecnología", "LAC_TAC",
            "CI_ECI", "Celda", "Azimuth_deg", "Latitud", "Longitud", "PLUS_CODE",
            "PLUS_CODE_NOMBRE"
        ])
        logdf = pd.DataFrame(logs)
        return CompileResult(empty, logdf, pd.DataFrame(), {}, out_xlsx)

    all_df = pd.concat(frames, ignore_index=True)

    # ===============================
    # Dedupe especial para DATOS/min
    # ===============================
    dupes = pd.DataFrame()
    if not all_df.empty and "Tipo" in all_df.columns and "Datetime" in all_df.columns:
        datos = all_df[all_df["Tipo"] == "DATOS"].copy()
        otros = all_df[all_df["Tipo"] != "DATOS"].copy()
        if not datos.empty:
            datos["_min"] = pd.to_datetime(datos["Datetime"], errors="coerce").dt.floor("min")
            datos["Duración (seg)"] = pd.to_numeric(datos["Duración (seg)"], errors="coerce")
            idx = datos.sort_values("Duración (seg)", ascending=False).groupby(["Número A", "Número B", "_min"], dropna=False).head(1).index
            kept = datos.loc[idx]
            removed = datos.drop(index=idx)
            dupes = removed.drop(columns=["_min"], errors="ignore").copy()
            datos = kept.drop(columns=["_min"], errors="ignore")
            all_df = pd.concat([otros, datos], ignore_index=True)

    # Si no vino Registro_ID, generamos uno global (1..N)
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

    # Orden final por Datetime ascendente
    if "Datetime" in all_df.columns:
        all_df = all_df.sort_values("Datetime", na_position="last").reset_index(drop=True)

    logdf = pd.DataFrame(logs)

    # Estadísticas
    stats = _build_stats(all_df)

    # Salida Excel
    if out_xlsx:
        _write_excel(out_xlsx, all_df, logdf, dupes, stats)

    return CompileResult(all_df, logdf, dupes, stats, out_xlsx)


if __name__ == "__main__":
    # CLI mínimo
    import argparse

    ap = argparse.ArgumentParser(description="Compilar sábanas AT&T")
    ap.add_argument("files", nargs="+", help="Rutas a archivos AT&T (.xlsx/.xls/.csv/.txt)")
    ap.add_argument("--tz", default="America/Mazatlan", help="Zona horaria local")
    ap.add_argument("--out", default=None, help="Ruta de salida .xlsx (opcional)")
    args = ap.parse_args()

    res = compile_att_sabanas(args.files, tz=args.tz, out_xlsx=args.out)
    print("Filas compiladas:", len(res.df))
    if args.out:
        print("Archivo Excel generado:", args.out)
