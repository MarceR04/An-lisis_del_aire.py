import pandas as pd
import sqlite3
import requests

def ej_1_cargar_datos_demograficos() -> pd.DataFrame:
    url = "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
    data = pd.read_csv(url, sep=';')
    
    # Realizar la limpieza de datos
    data.drop(columns=['Race', 'Count', 'Number of Veterans'], inplace=True)
    data.drop_duplicates(inplace=True)
    
    return data

def ej_2_cargar_calidad_aire(ciudades: set) -> None:
    api_url = "https://api-ninjas.com/api/airquality"
    
    ciudades_data = []
    
    for ciudad in ciudades:
        response = requests.get(f"{api_url}/{ciudad}")
        data = response.json()
        
        if data:
            # Tomar el elemento concentration
            concentration = data.get("concentration")
            ciudades_data.append({
                'city': ciudad,
                'concentration': concentration
            })
    
    # Crear un DataFrame con los datos de calidad del aire
    calidad_aire_df = pd.DataFrame(ciudades_data)
    
    # Crear una base de datos SQLite y cargar las tablas
    conn = sqlite3.connect("datos.db")
    ej_1_cargar_datos_demograficos().to_sql("demografia", conn, if_exists="replace", index=False)
    calidad_aire_df.to_sql("calidad_aire", conn, if_exists="replace", index=False)
    conn.close()

# Llamamos a las funciones para cargar y procesar los datos
data_demografica = ej_1_cargar_datos_demograficos()
ciudades_set = set(data_demografica["City"].tolist())
ej_2_cargar_calidad_aire(ciudades_set)
