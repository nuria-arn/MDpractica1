# Uso de la IA ChatGPT-6 para la lista de los tickers

from src.modelo.ejercicios.ejercicio1 import *
from src.modelo.ejercicios.ejercicio2 import *
from src.modelo.ejercicios.ejercicio3 import *
from src.modelo.ejercicios.ejercicio4 import *
from src.modelo.ejercicios.ejercicio5 import *
from src.modelo.conexion.SparkSession import *

def ejecutar():

    df, spark = crear_sesion()

    # Ejercicio 1b
    # Descargar ultimos dos años de todas las empresas de ibex
    # guardarlo en crudo en la capa de bronce
    
    empresas = [
        "ACS.MC", "ACX.MC", "AENA.MC", "AMS.MC", "ANA.MC", "BBVA.MC","BKT.MC", "CABK.MC","CLNX.MC", "COL.MC", "ELE.MC", "ENG.MC", "FDR.MC",
        "FER.MC", "GRF.MC", "IAG.MC", "IBE.MC", "IDR.MC","ITX.MC", "LOG.MC", "MAP.MC", "MRL.MC", "MTS.MC", "NTGY.MC", "RED.MC", "REP.MC",
        "ROVI.MC", "SAB.MC", "SAN.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC", "VIS.MC", "ANE.MC"
    ]
    '''
    df_ej_1b = None

    for empresa in empresas:
        df_empresa = ejercicio1a(spark, empresa, "2024-10-01", "2026-10-01")

        if df_ej_1b is None:
            df_ej_1b = df_empresa
        else:
            df_ej_1b = df_ej_1b.union(df_empresa)
    
    df_ej_1b.write.mode("overwrite").parquet("data/lake/bronze/ibex")
    '''
    # Ejercicio 1c
    print("Ejercicio 1c:")
    df_ej_1c = ejercicio1c(spark)

    # Ejercicio 2a
    ejercicio2a(df)

    # Ejercicio 2b: guardar df en capa bronze
    df_sent = ejercicio2b(df, spark)
    print(df_sent.count())

    # Ejercicio 2c: ejb pero con append
    df_sent2 = ejercicio2c(df, spark)
    print(df_sent2.count())

    # Ejercicio 3: convierte carpetas parquet en delta y muestra historial
    ejercicio3(spark)
    print("Se ha creado una carpeta _delta_log en ambas carpetas ibex y sentimiento que contiene los metadatos de las operaciones que se han hecho sobre las tablas. ")

    # Ejercicio 4: hacer merge sobre Ticker y Fecha con los precios de Ibex hasta hoy
    '''ejercicio4(spark, empresas)'''
    print("Al ejecutarlo por primera vez aparecían 805 filas insertadas, al ejecutarlo por segunda vez aparecen 0 filas insertadas.")

    # Ejercicio 5a: columna nueva close mas5 