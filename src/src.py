import pandas as pd
import requests
import numpy as np
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
import psycopg2


def convertir_fecha(dataframe):

    """
    Convierte todas las columnas de un DataFrame cuyo nombre contenga "dt" en formato de fecha (%Y-%m-%d).

    Parámetros:

    dataframe (df): DataFrame que contiene las columnas a convertir.

    Retorno:

    DataFrame: DataFrame con las fechas convertidas en formato YYYY-MM-DD.

    Uso: df = convertir_fecha(df)

    """
    for col in dataframe.filter(like="dt", axis=1):
        dataframe[col] = pd.to_datetime(dataframe[col]).dt.strftime('%Y-%m-%d')
    return dataframe

def convertir_fechaeventos(dataframe):
    """
    Convierte las columnas que contengan la palabra "fecha" en objetos de fecha (datetime).

    Parámetros:

    dataframe (df): DataFrame con las fechas a convertir.

    Retorno:

    dataframe: DataFrame con fechas convertidas a objetos datetime.
    
    Uso: df = convertir_fechaeventos(df)

    """
    for col in dataframe.filter(like="fecha", axis=1):
        dataframe[col] = pd.to_datetime(dataframe[col])
    return dataframe

def scrapear_hoteles (url_base, dict):
    """
    Realiza web scraping de datos de hoteles desde la página especificada.

    Parámetros:

    url_base (str): URL de la página web a scrapear.

    dict (dict): Diccionario para almacenar los datos extraídos.

    Retorno:

    pd.DataFrame: DataFrame con los datos extraídos (nombre del hotel, valoraciones, precio y fecha).

    Uso: df = scrapear_hoteles (url, dict)

    """
    if "nombre_hotel" not in dict:
        dict["nombre_hotel"] = []
    if "valoraciones" not in dict:
        dict["valoraciones"] = []
    if "precio" not in dict:
        dict["precio"] = []
    if "fecha" not in dict:
        dict["fecha"] = []
    service = Service(ChromeDriverManager().install())
    options = Options()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url_base)
    cookie_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept')]")
    cookie_button2 = driver.find_element(By.XPATH, "//button[contains(text(), 'A')]")
    try:
        cookie_button.click()
    except:
        cookie_button2.click()
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span.booking-price__number.mcp-price-number")))
    info = driver.find_elements(By.CSS_SELECTOR, ".hotelblock__content")
    for item in info:
        precio = item.find_element(By.CSS_SELECTOR, "span.booking-price__number.mcp-price-number").text
        nombre = item.find_element(By.CSS_SELECTOR, ".title__link").text
        nombre_hotel1 = nombre
        nombre_hotel1 = nombre_hotel1.split()
        nombre_hotel = " ".join(nombre_hotel1[:-2])
        valoraciones = item.find_element(By.CSS_SELECTOR, ".ratings__score").text
        valoraciones1 = valoraciones.split("/")[0]
        fecha = datetime.now()
        fecha = fecha.strftime("%Y-%m-%d")
        dict["nombre_hotel"].append(nombre_hotel)
        dict["valoraciones"].append(valoraciones1)
        dict["precio"].append(precio)
        dict["fecha"].append(fecha)
    driver.quit()
    dfscrap = pd.DataFrame(dict)
    dfscrap.to_csv("../datos/datos_webscrapping.csv", index=False)
    return dfscrap

def extraccion_api(url):
    """
    Extrae datos desde una API pública y los convierte en un DataFrame.

    Parámetros:

    url (str): URL de la API a consultar.

    Retorno:

    pd.DataFrame: DataFrame filtrado con columnas relevantes.

    Uso: df = extraccion_api(url)

    """
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json() # Convertir la respuesta a un diccionario de Python
        print(data)
    else:
        print(f"Error en la solicitud: {response.status_code}")
    eventos = data["@graph"] # seleccionar datos del diccionario
    lista_eventos = [] # Crear lista con todos los eventos
    for evento in eventos:
        lista_eventos.append(evento)
    dfapi = pd.DataFrame(eventos)
    dfapi.to_csv("../datos/datos_api.csv", index=False)
    # Crear DF con las columnas que nos interesan
    dfapilimpio = dfapi[["id", "title", "link", "address", "time", "dtstart", "dtend", "organization"]]
    return dfapilimpio

def insertar_datos_automatico(df, tabla, cursor):
    """
    Inserta automáticamente los datos de un DataFrame en una tabla PostgreSQL.

    Parámetros:

    df (pd.DataFrame): DataFrame con los datos a insertar.

    tabla (str): Nombre de la tabla de destino.

    cursor (psycopg2.cursor): Cursor de la base de datos.

    Uso: insertar_datos_automatico(df, tabla, cursor)
    
    """
    columnas = df.columns.tolist()  # Extraer nombres de columnas
    columnas_str = ", ".join(columnas) # Poner comas para separar los nombres de las columnas
    placeholders = ", ".join(["%s"] * len(columnas))  # Generar los %s en función del nº de columnas

    # Generar la query automáticamente
    insert_query = f"""
        INSERT INTO {tabla} ({columnas_str})
        VALUES ({placeholders})
    """

    # Convertir los datos del DataFrame en una lista de listas
    data_to_insert = df.values.tolist()

    # Ejecutar la inserción
    cursor.executemany(insert_query, data_to_insert)
    conn.commit()

def conexion_postgres(dbname, user, password, host, port):
    """
    Establece una conexión con la base de datos PostgreSQL.

    Parámetros:

    dbname (str): Nombre de la base de datos.

    user (str): Usuario de la base de datos.

    password (str): Contraseña del usuario.

    host (str): Dirección del servidor.

    port (int): Puerto de conexión.

    Retorno:

    conn: Objeto de conexión.

    cur: Cursor de la base de datos.

    Uso: conexion_postgres(dbname, user, password, host, port)

    """
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cur = conn.cursor()
# Confirmar conexion
    cur.execute("SELECT version();")
    print(cur.fetchone())
    return conn, cur