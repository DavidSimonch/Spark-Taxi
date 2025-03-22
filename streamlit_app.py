import streamlit as st
import pandas as pd
import requests

st.title("🚕 NYC Taxi Data Dashboard")

# --- Cargar y mostrar los datasets si existen ---
try:
    # Cargar los archivos JSON
    with open("results/data.json") as f:
        data = pd.read_json(f)
    with open("results/summary.json") as f:
        summary = pd.read_json(f)

    # --- Convertir los timestamps a datetime ---
    data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"], unit='ms')
    data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"], unit='ms')

    # --- Verificar las fechas de los datos ---
    st.write("Primeras fechas de pickup:")
    st.write(data["tpep_pickup_datetime"].head())  # Mostrar las primeras fechas para verificar

    # --- Asegurarse de que la columna 'tpep_pickup_datetime' sea tipo datetime ---
    data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"], errors='coerce')

    # Eliminar filas con fechas nulas
    data = data.dropna(subset=["tpep_pickup_datetime"])

    # Convertir las fechas a tipo `date` para el filtro
    min_date = data["tpep_pickup_datetime"].min().date()
    max_date = data["tpep_pickup_datetime"].max().date()

    st.write(f"Rango de fechas disponibles: {min_date} - {max_date}")  # Verificar el rango de fechas

    # --- Filtro para el dataset 'data.json' ---
    st.subheader("📄 Muestra de viajes")

    # Filtro por fecha de pickup
    pickup_date = st.date_input("Selecciona la fecha de pickup", min_value=min_date, max_value=max_date)
    
    # Filtrar los datos según la fecha seleccionada
    filtered_data = data[data["tpep_pickup_datetime"].dt.date == pickup_date]

    st.dataframe(filtered_data)

    # --- Filtro para el resumen 'summary.json' ---
    st.subheader("📊 Resumen por hora")

    # Filtro por hora de pickup
    hour_filter = st.slider("Selecciona la hora de pickup", 0, 23, 0)
    filtered_summary = summary[summary["pickup_hour"] == hour_filter]

    st.dataframe(filtered_summary)

except FileNotFoundError:
    st.warning("⚠️ No se han generado los archivos aún. Usa el botón de abajo.")

# --- Botón para lanzar el workflow de GitHub ---
st.subheader("⚙️ Ejecutar procesamiento con GitHub Actions")

if st.button("🔁 Generar nuevos datos"):
    # Reemplaza con tu GitHub token y el usuario/repositorio
    token = st.secrets["github_token"]  # Usando Streamlit Secrets
    usuario = "DavidSimonch"  # Tu usuario de GitHub
    repo = "Spark-Taxi"  # Tu repositorio de GitHub

    url = f"https://api.github.com/repos/{usuario}/{repo}/dispatches"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }
    payload = {
        "event_type": "spark"
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 204:
        st.success("✅ GitHub Action lanzado con éxito. Espera 1-2 min y recarga.")
    else:
        st.error(f"❌ Error al lanzar workflow: {response.status_code}")