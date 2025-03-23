import streamlit as st
import pandas as pd
import requests
import pymongo
import json
from bson import ObjectId

# Ajustar el layout para que todo sea más ancho
st.set_page_config(layout="wide")

# Título de la aplicación
st.title("🚕 NYC Taxi Data Dashboard")

# --- Estilos CSS para que las pestañas sean más grandes ---
st.markdown("""
    <style>
        .stRadio>div>label {
            font-size: 20px;
            padding: 15px;
            margin-right: 10px;
        }
        .stRadio>div>label:hover {
            background-color: #ddd;
        }
        .stButton>button {
            font-size: 18px;
            padding: 12px;
            margin: 10px;
        }
        .stTextInput>div>div>input {
            font-size: 18px;
        }
    </style>
""", unsafe_allow_html=True)

# --- Menú de navegación con pestañas ---
tab = st.radio("Selecciona una opción:", 
               ["Lanzar GitHub Action", "Consultar Resultados", "Conexión MongoDB y PostgreSQL"],
               index=0, 
               horizontal=True)

# --- Página 1: Lanzar GitHub Action ---
if tab == "Lanzar GitHub Action":
    st.subheader("⚙️ Ejecutar procesamiento con GitHub Actions")
    if st.button("🔁 Generar nuevos datos"):
        # Reemplaza con tu GitHub token y el usuario/repositorio
        token = st.secrets["github_token"]  # Usando Streamlit Secrets
        usuario = "DavidSimonch"  # Tu usuario de GitHub
        repo = "Spark-Taxi"      # Tu repositorio de GitHub

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

# --- Página 2: Consultar Resultados ---
elif tab == "Consultar Resultados":
    st.header("📊 Consultar Resultados")

    # Cargar los archivos JSON de resultados
    try:
        with open("results/data.json") as f:
            data = pd.read_json(f)
        with open("results/summary.json") as f:
            summary = pd.read_json(f)

        # --- Convertir los timestamps a datetime ---
        data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"], unit='ms')
        data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"], unit='ms')

        # Filtro por fecha de pickup
        st.subheader("📄 Filtro de Datos de Viajes")
        min_date = data["tpep_pickup_datetime"].min().date()
        max_date = data["tpep_pickup_datetime"].max().date()
        pickup_date = st.date_input("Selecciona la fecha de pickup", min_value=min_date, max_value=max_date)

        filtered_data = data[data["tpep_pickup_datetime"].dt.date == pickup_date]
        st.dataframe(filtered_data, use_container_width=True, height=600)

        # Filtro para el resumen 'summary.json'
        st.subheader("📊 Resumen por hora")
        hour_filter = st.slider("Selecciona la hora de pickup", 0, 23, 0)
        filtered_summary = summary[summary["pickup_hour"] == hour_filter]
        #st.dataframe(filtered_summary, use_container_width=True, height=600)
        st.dataframe(filtered_summary)
       

    except FileNotFoundError:
        st.warning("⚠️ No se han generado los archivos aún. Usa el botón de Generar.")

# --- Página 3: Conexión y consulta a MongoDB y PostgreSQL en paralelo ---
elif tab == "Conexión MongoDB y PostgreSQL":
    st.header("🔗 Conexión a MongoDB y PostgreSQL")

    # Crear dos columnas para mostrar ambas secciones al mismo tiempo
    col1, col2 = st.columns(2)

    # --- Conexión a MongoDB ---
    with col1:
        st.subheader("📚 Conexión a MongoDB")

        @st.cache_resource
        def init_mongo_connection():
            return pymongo.MongoClient(**st.secrets["mongo"])

        client = init_mongo_connection()

        @st.cache_data(ttl=600)
        def get_mongo_data():
            db = client.summary         # Acceder a la base de datos 'summary'
            items = db.summary.find()   # Colección 'summary'
            return list(items)          # Convertir el cursor a lista

        def convert_object_to_str(item):
            for key, value in item.items():
                if isinstance(value, ObjectId):
                    item[key] = str(value)
            return item

    # --- Conexión a PostgreSQL ---
    with col2:
        st.subheader("📊 Conexión a PostgreSQL")

        def check_postgres_connection():
            try:
                # Establecer la conexión usando st.secrets
                conn = st.connection("postgresql", type="sql")
                query = "SELECT * FROM taxi_data LIMIT 50;"  # Ajusta la tabla/consulta a tu caso
                df = conn.query(query, ttl="10m")
                return df
            except Exception as e:
                st.error(f"Error en la conexión a PostgreSQL: {e}")
                return None

    # --- Botón único para consultar a MongoDB y PostgreSQL ---
    if st.button("🔄 Verificar Conexión a MongoDB y PostgreSQL"):
        # --- Consultar MongoDB ---
        result_mongo = get_mongo_data()
        if result_mongo:
            # Convertir ObjectId a string
            result_mongo = [convert_object_to_str(item) for item in result_mongo]

            # Extraer el campo 'data' de cada documento y unirlo en un solo DataFrame
            all_data = []
            for item in result_mongo:
                all_data.extend(item['data'])
            df_mongo = pd.DataFrame(all_data)

            with col1:
                st.subheader("Datos de MongoDB como tabla:")
                st.dataframe(df_mongo, use_container_width=True, height=600)
        else:
            with col1:
                st.warning("No se obtuvieron datos de MongoDB")

        # --- Consultar PostgreSQL ---
        result_postgres = check_postgres_connection()
        if result_postgres is not None:
            with col2:
                st.subheader("Datos de PostgreSQL:")
                st.dataframe(result_postgres, use_container_width=True, height=600)
        else:
            with col2:
                st.warning("No se obtuvieron datos de PostgreSQL")