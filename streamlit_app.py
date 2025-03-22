import streamlit as st
import pandas as pd
import json


st.title("🚕 NYC Taxi Data Dashboard")

# Cargar archivos
with open("results/data.json") as f:
    data = pd.read_json(f)

with open("results/summary.json") as f:
    summary = pd.read_json(f)

# Mostrar tabla de muestra
st.subheader("📄 Muestra de viajes")
st.dataframe(data)

# Mostrar resumen por hora
st.subheader("📊 Resumen por hora")
st.bar_chart(summary.set_index("pickup_hour")[["avg_distance", "avg_amount"]])
st.line_chart(summary.set_index("pickup_hour")["total_trips"])