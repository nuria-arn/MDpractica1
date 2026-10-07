from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

def crear_sesion():

    path = r"lib/mysql-connector-j-26.7.0.jar"

    spark_session = (SparkSession.builder.appName("IBEX35").config("spark.driver.extraClassPath", path).getOrCreate())
    df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("sentimiento_ibex.csv")
    return df, spark_session