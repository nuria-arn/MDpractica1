# Uso de la IA ChatGPT-6 para la lista de los tickers

from src.modelo.ejercicios.ejercicio1 import *
from src.modelo.ejercicios.ejercicio2 import *
from src.modelo.ejercicios.ejercicio3 import *
from src.modelo.ejercicios.ejercicio4 import *
from src.modelo.ejercicios.ejercicio5 import *
from src.modelo.ejercicios.ejercicio6 import *
from src.modelo.ejercicios.ejercicio8 import *
from src.modelo.ejercicios.ejercicio9 import *
from src.modelo.ejercicios.ejercicio10 import *
from src.modelo.ejercicios.funciones10 import *
from src.modelo.conexion.SparkSession import *

from delta.tables import DeltaTable

def ejecutar():

    df, spark = crear_sesion()
    df = spark.read.format("delta").load("data/lake/bronze/ibex")

    print("Número de filas:", df.count())
    df.printSchema()

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
    '''
    # Ejercicio 2a
    ejercicio2a(df)

    # Ejercicio 2b: guardar df en capa bronze
    df_sent = ejercicio2b(df, spark)
    print(df_sent.count())

    # Ejercicio 2c: ejb pero con append
    df_sent2 = ejercicio2c(df, spark)
    print(df_sent2.count())'''

    # Ejercicio 3: convierte carpetas parquet en delta y muestra historial
    '''ejercicio3(spark)'''
    print("Se ha creado una carpeta _delta_log en ambas carpetas ibex y sentimiento que contiene los metadatos de las operaciones que se han hecho sobre las tablas. ")

    # Ejercicio 4: hacer merge sobre Ticker y Fecha con los precios de Ibex hasta hoy
    '''ejercicio4(spark, empresas)'''
    print("Al ejecutarlo por primera vez aparecían 805 filas insertadas, al ejecutarlo por segunda vez aparecen 0 filas insertadas.")

    # Ejercicio 5a: columna nueva close mas5 con append
    '''ejercicio5a(spark)'''

    # Ejercicio 5b: mismo que el 5a pero añadiendo mergeSchema
    '''ejercicio5b(spark)'''
    print("Ya no salta error añadiendo mergeSchema")

    # Ejercicio 5c: eliminar las filas del ticker BBVA
    '''ejercicio5c(spark)'''

    # Ejercicio 5d: muestra history() y la version de antes de 5a
    '''ejercicio5d(spark)'''

    # Ejercicio 6: Deja la tabla realmente como estaba en el EJ4
    '''ejercicio6(spark)'''
    print("Hasta aqui el ejerciccio 6")

    # Ejercicio 8: construir la capa silver
    ejercicio8(spark)

    # Ejercicio 9: 
    ejercicio9(spark)

    # Ejercicio 10: integrar las fuentes y guardar en gold
    # Dimensión empresa: obtener información de Yahoo Finance
    datos_empresas = obtener_info_empresas(empresas)

    # Guardar información original en Bronze
    df_dim_empresa = guardar_dim_empresa_bronze(spark, datos_empresas)
    df_dim_empresa_silver = limpiar_dim_empresa(spark)

    # Crear dimensión fecha
    dim_fecha = crear_dim_fecha(spark)

    # Crear tabla de hechos
    fact = crear_fact_cotizaciones(spark)
