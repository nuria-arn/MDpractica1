from src.modelo.ejercicios.ejercicio1 import *
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

    df_ej_1b = None


    for empresa in empresas:
        df_empresa = ejercicio1a(spark, empresa, "2024-10-01", "2026-10-01")

        if df_ej_1b is None:
            df_ej_1b = df_empresa
        else:
            df_ej_1b = df_ej_1b.union(df_empresa)
    
    df_ej_1b.write.mode("overwrite").parquet("data/lake/bronze/ibex")