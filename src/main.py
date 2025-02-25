# Cargar librerias
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime
from datetime import date
from dotenv import load_dotenv
import os
from src import convertir_fecha, convertir_fechaeventos, scrapear_hoteles, extraccion_api, insertar_datos_automatico, conexion_postgres
import psycopg2

# Cargar fichero y crear copia
df = pd.read_parquet("../datos/reservas_hoteles.parquet", engine='fastparquet')
dfcopia = df.copy()
load_dotenv()

# Extraer datos API
urlapi = os.getenv("urlapi")
dfapilimpio = extraccion_api(urlapi)

# Extraer datos webscrapping
url_scrap = os.getenv("url_scrap")
dictio_scrap = {}
dfscrap = scrapear_hoteles (url_scrap, dictio_scrap)

# Elminar duplicados
dfcopia.drop_duplicates(inplace=True)

# Asignar id único a cada hotel (Estaban mal asignados)
hoteles_unicos = dfcopia.groupby('nombre_hotel')['id_hotel'].unique().reset_index()
hoteles_unicos
diccionario_hoteles = {"hotel": [], "id": []}
contador = 0
for hotel in hoteles_unicos["nombre_hotel"]:
    if hotel == "":
        pass
    else:
        diccionario_hoteles["hotel"].append(hotel)
        contador = contador + 1
        diccionario_hoteles["id"].append(contador)
df_dicthoteles = pd.DataFrame(diccionario_hoteles)
mapa_hoteles = dict(zip(df_dicthoteles["hotel"], df_dicthoteles["id"]))
dfcopia["id_hotel"] = dfcopia["nombre_hotel"].map(mapa_hoteles).fillna(df["id_hotel"])
dfcopia["id_hotel"] = dfcopia["id_hotel"].astype(int)

# Cambiar ID cliente (había mas IDs que clientes)
lista_mails = dfcopia["mail"].unique().tolist()
lista_idmails = []
contador = 0
for mail in lista_mails:
    contador = contador + 1
    lista_idmails.append(contador)
diccionario_mails = dict(zip(lista_mails, lista_idmails))
dfcopia["id_cliente"] = dfcopia["mail"].map(diccionario_mails)
dfcopia["id_cliente"] = dfcopia["id_cliente"].astype(int)

# Cambiar nulos inicio y final de estancia (siempre es la misma fecha)
dfcopia["inicio_estancia"] = dfcopia["inicio_estancia"].fillna("2025-03-01")
dfcopia["final_estancia"] = dfcopia["final_estancia"].fillna("2025-03-02")

# Cambiar precio noche de los hoteles no competencia por media de precios del hotel
medias = dfcopia.groupby("id_hotel")["precio_noche"].mean().round(2)
medias = pd.DataFrame(medias).reset_index()
diccionario_medias = dict(zip(medias["id_hotel"], medias["precio_noche"]))
dfcopia['precio_noche'] = dfcopia['precio_noche'].fillna(dfcopia['id_hotel'].map(diccionario_medias))

# Rellenar columna ciudad con Madrid (siempre es Madrid)
dfcopia["ciudad"] = "Madrid"

# Crear df con solo hoteles competencia y df solo hoteles propios
dfpropio = dfcopia[dfcopia["competencia"] == False]
dfcompetencia = dfcopia[dfcopia["competencia"] == True]

# Modificar las valoraciones de los hoteles propios (tienen distintas valoraciones y se rellena con la media)
medias_valoraciones = dfpropio.groupby("id_hotel")["estrellas"].mean().round(2).reset_index()
diccionario_valoraciones = dict(zip(medias_valoraciones["id_hotel"], medias_valoraciones["estrellas"]))
dfpropio["estrellas"] = dfpropio["id_hotel"].map(diccionario_valoraciones)

# Unir dfcompetencia con el df webscrapping
dictio_mergeo = dict(zip(dfscrap["nombre_hotel"], dfcompetencia["id_hotel"].unique().tolist()))
dfscrap["id_hotel"] = dfscrap["nombre_hotel"].map(dictio_mergeo)
dfcompetencia_mergeado = dfcompetencia.merge(dfscrap, on="id_hotel")
dfcompetencia_mergeado["fecha_reserva"] = dfcompetencia_mergeado["fecha"]
dfcompetencia_mergeado["precio_noche"] = dfcompetencia_mergeado["precio"]
dfcompetencia_mergeado["nombre_hotel_x"] = dfcompetencia_mergeado["nombre_hotel_y"]
dfcompetencia_mergeado = dfcompetencia_mergeado.drop(columns= ["nombre_hotel_y", "precio","fecha", "estrellas"])
dfcompetencia_mergeado = dfcompetencia_mergeado.rename(columns={"nombre_hotel_x":"nombre_hotel"})

# Limpieza fichero API
convertir_fecha(dfapilimpio)
dfapilimpio1 = dfapilimpio[(dfapilimpio["dtstart"] == "2025-03-01") | (dfapilimpio["dtend"] == "2025-03-02")]
dfapilimpio1["codigo-postal"] = dfapilimpio1["address"].apply(lambda x: x.get("area", {}).get("postal-code") if pd.notna(x) else None)
dfapilimpio1["direccion"] = dfapilimpio1["address"].apply(lambda x: x.get("area", {}).get("street-address") if pd.notna(x) else None)
dfapilimpio1["organizacion"] = dfapilimpio1["organization"].apply(lambda x: x.get("organization-name", {})if pd.notna(x) else None)
dfapilimpio1 = dfapilimpio1.drop(columns = ["address", "organization"], axis = 1)
dfapilimpio1["time"] = dfapilimpio1["time"].replace('', None)

# Crear tabla unida
df_mergeado = pd.concat([dfpropio, dfcompetencia_mergeado], axis=0, ignore_index = True)

# Configurar la conexión
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
host = os.getenv("host")
port = os.getenv("port")
(conn, cur) = conexion_postgres(dbname, user, password, host, port)

# Tabla ciudad
listaciudad = dfpropio["ciudad"].unique().tolist()
df_ciudad = pd.DataFrame(listaciudad, columns=["nombre_ciudad"])
insertar_datos_automatico(df_ciudad, "ciudad", cur)
df_eventos = dfapilimpio1.copy()
df_eventos["id"] = range(1, len(df_eventos["id"]) + 1)
df_eventos = df_eventos.rename(columns={"id": "id_evento",
                                        "title":"nombre_evento",
                                        "link": "url_evento",
                                        "codigo-postal": "codigo_postal",
                                        "time": "horario",
                                        "dtstart":"fecha_inicio",
                                        "dtend":"fecha_fin"})
def convertir_fechaeventos(dataframe):
    for col in dataframe.filter(like="fecha", axis=1):
        dataframe[col] = pd.to_datetime(dataframe[col])
    return dataframe
convertir_fechaeventos(df_eventos)
df_eventos["ciudad"] = "Madrid"
cur.execute("SELECT nombre_ciudad, id_ciudad FROM ciudad")
ciudad_dict = dict(cur.fetchall())
ciudad_dict
df_eventos["id_ciudad"] = df_eventos["ciudad"].map(ciudad_dict)
df_eventos.drop(columns = ["ciudad"], inplace = True)
df_eventos["codigo_postal"]= df_eventos["codigo_postal"].replace("No Disponible", None)
df_eventos["codigo_postal"] = df_eventos["codigo_postal"].fillna(0).astype(int)
df_eventos["codigo_postal"]= df_eventos["codigo_postal"].replace(0, None)
df_eventos["fecha_inicio"] = pd.to_datetime(df_eventos["fecha_inicio"]).dt.strftime('%Y-%m-%d')
df_eventos["fecha_fin"] = pd.to_datetime(df_eventos["fecha_fin"]).dt.strftime('%Y-%m-%d')
insertar_datos_automatico(df_eventos, "eventos", cur)

# Tabla hoteles
df_hoteles = df_mergeado[["id_hotel", "nombre_hotel", "competencia", "estrellas", "ciudad"]]
cur.execute("SELECT nombre_ciudad, id_ciudad FROM ciudad")
ciudad_dict = dict(cur.fetchall())
ciudad_dict
df_hoteles["id_ciudad"] = df_hoteles["ciudad"].map(ciudad_dict)
df_hoteles.drop(columns = ["ciudad"], inplace = True)
df_hoteles["estrellas"] = df_hoteles["estrellas"].astype(float)
df_hoteles.rename(columns = {"estrellas":"valoracion"}, inplace=True)
df_hoteles.drop_duplicates(inplace=True)
insertar_datos_automatico(df_hoteles, "hoteles", cur)

# Tabla clientes
df_clientes = df_mergeado[["id_cliente", "nombre", "apellido", "mail"]]
df_clientes.drop_duplicates(inplace=True)
insertar_datos_automatico(df_clientes, "clientes", cur)

# Tabla reservas
df_reservas = df_mergeado[['id_reserva', 'fecha_reserva', 'inicio_estancia', 'final_estancia',
       'precio_noche', 'mail','nombre_hotel']]
df_reservas["precio_noche"] = df_reservas["precio_noche"].astype(float)
col_fechas = ["fecha_reserva", "inicio_estancia", "final_estancia"]
for col in col_fechas:
    df_reservas[col] = pd.to_datetime(df_reservas[col])
cur.execute("SELECT mail, id_cliente FROM clientes")
clientes_dict = dict(cur.fetchall())
cur.execute("SELECT nombre_hotel, id_hotel FROM hoteles")
hoteles_dict = dict(cur.fetchall())
df_reservas["id_cliente"] = df_reservas["mail"].map(clientes_dict)
df_reservas.drop(columns = ["mail"], inplace = True)
df_reservas["id_hotel"] = df_reservas["nombre_hotel"].map(hoteles_dict)
df_reservas.drop(columns = ["nombre_hotel"], inplace = True)
insertar_datos_automatico(df_reservas, "reservas", cur)

cur.close()
conn.close()

if __name__ == "__main__":
    print('Ejecutando proceso ETL')
    print("¡Terminado!")