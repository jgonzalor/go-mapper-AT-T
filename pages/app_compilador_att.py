import streamlit as st
import pandas as pd
import io
import tempfile
from typing import List, Dict

st.set_page_config(page_title="Go Mapper — Compilador AT&T (independiente)", layout="wide")

st.title("📞 Go Mapper — Compilador AT&T (independiente)")
st.write(
    "Sube sábanas de AT&T (XLS/XLSX/CSV/TXT), unifícalas al formato del *Limpieza*, y descarga un Excel con *Datos_Limpios*, *LOG_Compilación*, *Duplicados* y hojas de estadísticas."
)

# ===== Import del módulo (asegúrate de que la ruta existe en tu repo) =====
try:
    from modules.att_compiler import compile_att_sabanas
except Exception as e:
    st.error("No se pudo importar modules.att_compiler. Verifica que el archivo exista y no tenga errores.")
    st.exception(e)
    st.stop()

# ===== Sidebar =====
st.sidebar.header("Parámetros")
tz = st.sidebar.text_input("Zona horaria", value="America/Mazatlan")
show_preview = st.sidebar.checkbox("Mostrar preview de datos", value=True)

st.sidebar.markdown("---")
st.sidebar.caption("Este módulo NO hace geocoding ni llamadas externas. Opcionalmente genera PLUS_CODE si tu DataFrame trae Latitud/Longitud y tienes `openlocationcode` instalado en el entorno.")

# ===== Carga de archivos =====
files = st.file_uploader(
    "Arrastra y suelta archivos de AT&T (puedes subir varios)",
    type=["xlsx", "xls", "csv", "txt"],
    accept_multiple_files=True,
)

col_btn1, col_btn2 = st.columns([1,1])

compile_clicked = col_btn1.button("🧩 Compilar sábanas AT&T", type="primary")
clear_clicked = col_btn2.button("🗑️ Limpiar sesión")

if clear_clicked:
    st.experimental_rerun()

if compile_clicked:
    if not files:
        st.warning("Primero sube al menos un archivo.")
        st.stop()

    tmp_paths: List[str] = []
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            for f in files:
                # Guardar con la extensión original
                suffix = ("." + f.name.split(".")[-1].lower()) if "." in f.name else ""
                tmp = tempfile.NamedTemporaryFile(delete=False, dir=tmpdir, suffix=suffix)
                tmp.write(f.read())
                tmp.flush()
                tmp_paths.append(tmp.name)

            with st.spinner("Compilando y normalizando…"):
                res = compile_att_sabanas(tmp_paths, tz=tz, out_xlsx=None)

            st.success(f"✅ Compilado: {len(res.df):,} filas | Archivos procesados: {len(files)}")

            # ===== Preview =====
            if show_preview:
                st.subheader("Preview — Datos_Limpios")
                st.dataframe(res.df.head(500), use_container_width=True)

            # ===== Resumen rápido =====
            st.subheader("Resumen rápido")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Filas totales", f"{len(res.df):,}")
            with c2:
                tipos = res.df["Tipo"].value_counts(dropna=False).rename_axis("Tipo").reset_index(name="Conteo") if not res.df.empty else pd.DataFrame()
                st.write("**Distribución por Tipo**")
                if not tipos.empty:
                    st.dataframe(tipos, use_container_width=True)
                else:
                    st.caption("Sin datos")
            with c3:
                st.write("**Duplicados (DATOS/min) removidos**")
                st.metric("Filas duplicadas", f"{len(res.dupes):,}")

            # ===== LOG =====
            with st.expander("📜 LOG de compilación"):
                st.dataframe(res.log, use_container_width=True)

            # ===== Estadísticas =====
            if res.stats:
                st.subheader("📊 ESTADÍSTICAS")
                for name, sdf in res.stats.items():
                    st.markdown(f"**{name}**")
                    st.dataframe(sdf, use_container_width=True)
            else:
                st.caption("No se generaron estadísticas (dataset vacío o columnas clave ausentes).")

            # ===== Descargar Excel =====
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

# ===== Ayuda rápida =====
st.markdown(
    """
---
### Salida estandarizada (igual que *Limpieza*)
- **Datos_Limpios** con columnas: Operador, Tipo, Dirección del tráfico (VOZ), Número A, Número B, Datetime, Duración (seg), IMEI, IMSI, Tecnología, LAC_TAC, CI_ECI, Celda, Azimuth_deg, Latitud, Longitud, PLUS_CODE, PLUS_CODE_NOMBRE.
- **LOG_Compilación**: archivo, filas, columnas detectadas, columnas origen, y errores si aplican.
- **Duplicados**: registros de *DATOS* removidos por regla (misma pareja A/B dentro del mismo minuto, se conserva la mayor duración).
- **Estadísticas**: Top10 Salientes/Entrantes, Top IMEI, Antenas TOP por tipo (si existen columnas de celda).

> Este módulo es independiente. Cuando quede estabilizado, podemos integrarlo como `pages/app_compilador_att.py` dentro de tu suite principal y conectarlo con los siguientes módulos (KMZ, azimuth, mapas, etc.).
"""
)
