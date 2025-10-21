# app.py — Compilador AT&T → Datos_Limpios (20 cols) + Estadísticos — single file v3.3

from __future__ import annotations
import io, os, re, tempfile
from typing import Any, List, Dict, Optional

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Compilador AT&T → Datos_Limpios (20 cols) + Stats", layout="wide")
st.title("📞 Compilador AT&T → Datos_Limpios (20 columnas) + 📊 Estadísticos")
st.caption("Convierte la sábana de AT&T al formato de 20 columnas y genera los estadísticos como en el módulo de limpieza.")

# ===== PLUS CODE opcional
try:
    from openlocationcode import openlocationcode as olc
    HAS_OLC = True
except Exception:
    HAS_OLC = False

# ===== Helpers
def parse_duration_to_seconds(val: Any) -> Optional[int]:
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
    return int(s2) if s2.isdigit() else None

def norm_text(x: Any) -> str:
    import unicodedata
    s = unicodedata.normalize("NFD", str(x))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn").lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()

# Derivación de Tipo (igual que acordamos con Telcel)
VOZ_OUT = {"mo","moc","saliente","orig","out","originating","salida"}
VOZ_IN  = {"mt","mtc","entrante","term","in","terminating","entrada"}
MSG     = {"sms","mensaje","mensajes","2 vias","2vias","mms"}
DATA    = {"gprs","datos","data","internet","ps","pdp","packet"}
TRANSF  = {"transfer","desvio","desvío","call forward","cfu","cfb","cfnry","cfnr","cfnrc"}
VOICE_TOK = {"voz","llamada","call","moc","mtc"}

def derive_tipo(serv: Any, t_reg: Any, tipo_com: Any) -> Optional[str]:
    s = norm_text(serv) if serv is not None else ""
    t = norm_text(t_reg) if t_reg is not None else ""
    c = norm_text(tipo_com) if tipo_com is not None else ""
    if any(tok in s or tok in t or tok in c for tok in TRANSF): return "TRANSFER"
    if any(tok in s or tok in t or tok in c for tok in MSG):    return "MENSAJES 2 VÍAS"
    if any(tok in s or tok in t or tok in c for tok in DATA):   return "DATOS"
    is_voice = any(tok in s or tok in c for tok in VOICE_TOK) or ("call" in t) or ("moc" in t) or ("mtc" in t)
    if is_voice:
        if any(tok in t or tok in s or tok in c for tok in VOZ_OUT): return "VOZ SALIENTE"
        if any(tok in t or tok in s or tok in c for tok in VOZ_IN):  return "VOZ ENTRANTE"
        return "VOZ SALIENTE"
    if any(tok in t for tok in VOZ_OUT): return "VOZ SALIENTE"
    if any(tok in t for tok in VOZ_IN):  return "VOZ ENTRANTE"
    return None

def plus_code(lat, lon):
    if not HAS_OLC: return None
    try:
        latf, lonf = float(lat), float(lon)
        if not (-90 <= latf <= 90 and -180 <= lonf <= 180): return None
        return olc.encode(latf, lonf, codeLength=10)
    except Exception:
        return None

# ===== Detección de encabezado real (AT&T a veces pone “portada”)
REQ = {"NO","FECHA"}
ANY = {"DUR","DURACIÓN"}

def looks_like_header(vals: List[Any]) -> bool:
    up = {str(x).strip().upper() for x in vals if pd.notna(x)}
    return REQ.issubset(up) and len(ANY.intersection(up)) > 0

def read_any_with_sniff(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx",".xlsm",".xls"}:
        raw = pd.read_excel(path, sheet_name=0, header=None, dtype=str)
        header_row = None
        for i in range(min(120, len(raw))):
            if looks_like_header(raw.iloc[i].tolist()):
                header_row = i; break
        if header_row is not None:
            headers = raw.iloc[header_row].tolist()
            df = raw.iloc[header_row+1:].copy()
            df.columns = headers
            return df.dropna(how="all", axis=1).dropna(how="all").reset_index(drop=True)
        return pd.read_excel(path, sheet_name=0)
    elif ext in {".csv",".txt"}:
        try:
            df = pd.read_csv(path, engine="python")
            if looks_like_header(list(df.columns)): return df
        except Exception: pass
        try:
            raw = pd.read_csv(path, engine="python", header=None, dtype=str)
            header_row = None
            for i in range(min(120, len(raw))):
                if looks_like_header(raw.iloc[i].tolist()):
                    header_row = i; break
            if header_row is not None:
                headers = raw.iloc[header_row].tolist()
                df = raw.iloc[header_row+1:].copy()
                df.columns = headers
                return df.dropna(how="all", axis=1).dropna(how="all").reset_index(drop=True)
        except Exception: pass
        return pd.read_csv(path, engine="python", encoding_errors="ignore")
    else:
        return pd.read_csv(path, engine="python", encoding_errors="ignore")

# ===== Transformación a “Datos_Limpios” (20 columnas)
OUT_COLS = ['Teléfono','Tipo','Número A','Número B','Fecha','Hora','Duración (seg)','IMEI',
            'Latitud','Longitud','Azimuth','Latitud_raw','Longitud_raw','Azimuth_raw',
            'PLUS_CODE','PLUS_CODE_NOMBRE','Azimuth_deg','Datetime','Es_Duplicado','Cuenta_GrupoDup']

def transform_att_to_limpio(df: pd.DataFrame, telefono_fijo: Optional[str]) -> pd.DataFrame:
    # Columnas AT&T típicas: 'NO','SERV','T_REG','NUM_A','NUM_A_IMSI','NUM_A_IMEI','DEST','ID_DEST','HUSO',
    # 'FECHA','HORA','DUR','USO_DW','USO_UP','ID_CELDA','LATITUD','LONGITUD','AZIMUTH','CAUSA_T','TIPO_COM','PAIS'
    out = pd.DataFrame(index=range(len(df)), columns=OUT_COLS)

    # Teléfono: fijo si lo proporcionan; si no, NUM_A (igual al archivo que te generé)
    out['Teléfono'] = (str(telefono_fijo).strip() if telefono_fijo else None) or df.get('NUM_A')

    # Tipo
    serv = df.get('SERV'); treg = df.get('T_REG'); tipc = df.get('TIPO_COM')
    if serv is None and treg is None and tipc is None:
        out['Tipo'] = None
    else:
        out['Tipo'] = [derive_tipo(s, t, c) for s,t,c in zip(serv if serv is not None else [None]*len(df),
                                                            treg if treg is not None else [None]*len(df),
                                                            tipc if tipc is not None else [None]*len(df))]

    # Número A / B
    out['Número A'] = df.get('NUM_A')
    out['Número B'] = df.get('DEST')
    mask_b = out['Número B'].isna() | (out['Número B'].astype(str).str.strip()=="")
    if 'ID_DEST' in df.columns:
        out.loc[mask_b,'Número B'] = df.loc[mask_b,'ID_DEST']

    # Fecha / Hora
    out['Fecha'] = df.get('FECHA')
    out['Hora']  = df.get('HORA')

    # Duración
    out['Duración (seg)'] = df.get('DUR').apply(parse_duration_to_seconds) if 'DUR' in df.columns else None

    # IMEI
    out['IMEI'] = df.get('NUM_A_IMEI')

    # Lat/Lon/Azimuth (raw y num)
    out['Latitud_raw'] = df.get('LATITUD')
    out['Longitud_raw'] = df.get('LONGITUD')
    out['Azimuth_raw'] = df.get('AZIMUTH')
    out['Latitud']  = pd.to_numeric(df.get('LATITUD'), errors='coerce') if 'LATITUD' in df.columns else None
    out['Longitud'] = pd.to_numeric(df.get('LONGITUD'), errors='coerce') if 'LONGITUD' in df.columns else None
    out['Azimuth'] = df.get('AZIMUTH')
    out['Azimuth_deg'] = pd.to_numeric(df.get('AZIMUTH'), errors='coerce') if 'AZIMUTH' in df.columns else None

    # PLUS_CODE
    out['PLUS_CODE'] = [plus_code(lat, lon) if pd.notna(lat) and pd.notna(lon) else None
                        for lat, lon in zip(out['Latitud'], out['Longitud'])]
    out['PLUS_CODE_NOMBRE'] = None

    # Datetime (dayfirst como en limpieza)
    dt = pd.to_datetime(out['Fecha'].astype(str).str.strip() + " " + out['Hora'].astype(str).str.strip(),
                        errors="coerce", dayfirst=True)
    out['Datetime'] = dt

    # Duplicados (solo DATOS, por minuto, conservar mayor DUR por par A/B)
    out['Es_Duplicado'] = False
    out['Cuenta_GrupoDup'] = 1
    mask_datos = out['Tipo'] == 'DATOS'
    if mask_datos.any():
        datos = out.loc[mask_datos].copy()
        datos['_min'] = pd.to_datetime(datos['Datetime'], errors='coerce').dt.floor('min')
        datos['Duración (seg)'] = pd.to_numeric(datos['Duración (seg)'], errors='coerce')
        grp = datos.groupby(['Número A','Número B','_min'], dropna=False)
        cnt = grp.size().rename('Cuenta_GrupoDup')
        datos = datos.join(cnt, on=['Número A','Número B','_min'])
        keep_idx = grp['Duración (seg)'].idxmax()
        datos['Es_Duplicado'] = True
        datos.loc[keep_idx, 'Es_Duplicado'] = False
        out.loc[datos.index, 'Es_Duplicado'] = datos['Es_Duplicado']
        out.loc[datos.index, 'Cuenta_GrupoDup'] = datos['Cuenta_GrupoDup']

    return out[OUT_COLS]

# ===== Estadísticos (como Limpieza)
def build_stats(limpio: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    stats: Dict[str, pd.DataFrame] = {}
    if limpio.empty:
        return stats

    df = limpio.copy()
    # Excluir duplicados
    df = df[~df['Es_Duplicado'].fillna(False)].copy()

    # Tipos y fechas/horas
    df['Tipo'] = df['Tipo'].astype('string')
    df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
    df['Fecha_dia'] = df['Datetime'].dt.date
    df['Hora_int'] = df['Datetime'].dt.hour

    # ===== Resumen
    filas_tot = len(limpio)
    filas_efec = len(df)
    uniq_a = df['Número A'].dropna().nunique()
    uniq_b = df['Número B'].dropna().nunique()
    uniq_imei = df['IMEI'].dropna().nunique()
    rng_min = df['Datetime'].min()
    rng_max = df['Datetime'].max()
    resumen = pd.DataFrame({
        "Métrica": ["Filas totales", "Filas (sin duplicados)", "Números A únicos", "Números B únicos", "IMEI únicos", "Rango fechas (min)", "Rango fechas (max)"],
        "Valor": [filas_tot, filas_efec, uniq_a, uniq_b, uniq_imei, rng_min, rng_max]
    })
    stats["Resumen"] = resumen

    # ===== Distribución por Tipo
    dist = df.groupby('Tipo', dropna=False).size().reset_index(name='Conteo').sort_values('Conteo', ascending=False)
    total = dist['Conteo'].sum()
    dist['%'] = (dist['Conteo'] / total * 100).round(2)
    stats["Distrib_Tipo"] = dist

    # ===== Top10 VOZ SALIENTE (A→B)
    sal = df[df['Tipo'] == 'VOZ SALIENTE'].copy()
    if not sal.empty:
        g = sal.groupby(['Número A','Número B'], dropna=False).agg(
            Eventos=('Teléfono','count'),
            Duracion_total_s=('Duración (seg)','sum')
        ).reset_index().sort_values(['Eventos','Duracion_total_s'], ascending=False).head(10)
        stats["Top10_Salientes"] = g

    # ===== Top10 VOZ ENTRANTE (A←B)
    ent = df[df['Tipo'] == 'VOZ ENTRANTE'].copy()
    if not ent.empty:
        g = ent.groupby(['Número A','Número B'], dropna=False).agg(
            Eventos=('Teléfono','count'),
            Duracion_total_s=('Duración (seg)','sum')
        ).reset_index().sort_values(['Eventos','Duracion_total_s'], ascending=False).head(10)
        stats["Top10_Entrantes"] = g

    # ===== Top10 MENSAJES 2 VÍAS (A↔B)
    msg = df[df['Tipo'] == 'MENSAJES 2 VÍAS'].copy()
    if not msg.empty:
        g = msg.groupby(['Número A','Número B'], dropna=False).size().reset_index(name='Eventos') \
              .sort_values('Eventos', ascending=False).head(10)
        stats["Top10_Mensajes"] = g

    # ===== Top10 DATOS por Número A (conteo y suma de duración)
    datos = df[df['Tipo'] == 'DATOS'].copy()
    if not datos.empty:
        g = datos.groupby('Número A', dropna=False).agg(
            Eventos=('Teléfono','count'),
            Duracion_total_s=('Duración (seg)','sum')
        ).reset_index().sort_values(['Eventos','Duracion_total_s'], ascending=False).head(10)
        stats["Top10_Datos_porA"] = g

    # ===== Top10 IMEI
    if 'IMEI' in df.columns:
        t_imei = df.groupby('IMEI', dropna=False).size().reset_index(name='Eventos').sort_values('Eventos', ascending=False).head(10)
        stats["Top10_IMEI"] = t_imei

    # ===== Top PLUS_CODE_NOMBRE (si existe)
    if 'PLUS_CODE_NOMBRE' in df.columns and df['PLUS_CODE_NOMBRE'].notna().any():
        pc = df.groupby('PLUS_CODE_NOMBRE', dropna=True).size().reset_index(name='Eventos').sort_values('Eventos', ascending=False).head(10)
        stats["Top10_PlusCodeNom"] = pc

    # ===== Eventos por hora y Tipo (pivot 0..23)
    if df['Hora_int'].notna().any():
        hora = df.pivot_table(index='Hora_int', columns='Tipo', values='Teléfono', aggfunc='count', fill_value=0)
        hora = hora.sort_index().reset_index().rename(columns={'Hora_int':'Hora'})
        stats["Horas_x_Tipo"] = hora

    # ===== Eventos por día y Tipo
    if df['Fecha_dia'].notna().any():
        dia = df.pivot_table(index='Fecha_dia', columns='Tipo', values='Teléfono', aggfunc='count', fill_value=0)
        dia = dia.sort_index().reset_index().rename(columns={'Fecha_dia':'Fecha'})
        stats["Dias_x_Tipo"] = dia

    # ===== Duración total por Tipo
    if 'Duración (seg)' in df.columns:
        dur = df.groupby('Tipo', dropna=False)['Duración (seg)'].sum().reset_index().rename(columns={'Duración (seg)':'Duracion_total_s'}) \
                .sort_values('Duracion_total_s', ascending=False)
        stats["Duracion_x_Tipo"] = dur

    return stats

# ===== Sidebar
st.sidebar.header("Parámetros")
telefono_obj = st.sidebar.text_input("Fijar columna 'Teléfono' (opcional)", value="", help="Déjalo vacío para usar NUM_A.")
show_preview = st.sidebar.checkbox("Mostrar preview de Datos_Limpios", value=True)
show_stats = st.sidebar.checkbox("Mostrar estadísticos en pantalla", value=True)

files = st.file_uploader(
    "Sube 1 o varias sábanas AT&T (XLS/XLSX/CSV/TXT)",
    type=["xlsx","xls","csv","txt"],
    accept_multiple_files=True,
)

col1, col2 = st.columns(2)
go = col1.button("🧩 Convertir a Datos_Limpios + Stats", type="primary")
clear = col2.button("🗑️ Limpiar sesión")

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

                # Leer y unir todas las sábanas
                frames, logs = [], []
                for p in tmp_paths:
                    df = read_any_with_sniff(p)
                    logs.append({
                        "Archivo": os.path.basename(p),
                        "Filas leídas": len(df),
                        "Encabezados detectados": ", ".join(map(str, df.columns))
                    })
                    frames.append(df)
                raw_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

                # Transformar al formato de 20 columnas
                tel_fijo = telefono_obj.strip() or None
                limpio = transform_att_to_limpio(raw_all, telefono_fijo=tel_fijo)

                st.success(f"✅ Hecho: {len(limpio):,} filas en 'Datos_Limpios' (20 columnas)")

                if show_preview:
                    st.subheader("Preview — Datos_Limpios (20 columnas)")
                    st.dataframe(limpio.head(500), use_container_width=True)

                st.subheader("📜 LOG")
                st.dataframe(pd.DataFrame(logs), use_container_width=True)

                # ===== Estadísticos
                stats = build_stats(limpio)

                if show_stats and stats:
                    st.subheader("📊 Estadísticos (como Limpieza)")
                    # Resumen + tablas principales
                    if "Resumen" in stats:
                        st.markdown("**Resumen**")
                        st.dataframe(stats["Resumen"], use_container_width=True)
                    if "Distrib_Tipo" in stats:
                        st.markdown("**Distribución por Tipo**")
                        st.dataframe(stats["Distrib_Tipo"], use_container_width=True)
                    # Top tablas
                    for key in ["Top10_Salientes","Top10_Entrantes","Top10_Mensajes","Top10_Datos_porA","Top10_IMEI","Top10_PlusCodeNom"]:
                        if key in stats and not stats[key].empty:
                            st.markdown(f"**{key.replace('_',' ')}**")
                            st.dataframe(stats[key], use_container_width=True)
                    # Temporales
                    for key in ["Horas_x_Tipo","Dias_x_Tipo","Duracion_x_Tipo"]:
                        if key in stats and not stats[key].empty:
                            st.markdown(f"**{key.replace('_',' ')}**")
                            st.dataframe(stats[key], use_container_width=True)
                elif show_stats:
                    st.info("No se generaron estadísticos (dataset vacío o sin columnas clave).")

                # ===== Descargar Excel con stats en hojas extra
                def to_excel_with_stats(df: pd.DataFrame, stats: Dict[str, pd.DataFrame]) -> bytes:
                    bio = io.BytesIO()
                    with pd.ExcelWriter(bio, engine="xlsxwriter") as xw:
                        df.to_excel(xw, index=False, sheet_name="Datos_Limpios")
                        # Hojas de estadísticos
                        for name, sdf in stats.items():
                            sheet = str(name)[:31]  # límite Excel
                            try:
                                sdf.to_excel(xw, index=False, sheet_name=sheet)
                            except Exception:
                                # fallback por si alguna columna es problemática
                                sdf.reset_index(drop=True).to_excel(xw, index=False, sheet_name=sheet)
                    bio.seek(0)
                    return bio.getvalue()

                xlsx = to_excel_with_stats(limpio, stats)
                st.download_button(
                    "⬇️ Descargar Excel (Datos_Limpios + Estadísticos)",
                    xlsx,
                    file_name="ATT_Datos_Limpios_y_Estadisticos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        except Exception as e:
            st.error("Ocurrió un error.")
            st.exception(e)
