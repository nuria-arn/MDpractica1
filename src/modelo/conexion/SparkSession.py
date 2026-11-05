from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def crear_sesion():
    builder = (SparkSession.builder.appName("IBEX35")
               .master("local[*]")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

    df = spark_session.read.option("header", True).option("sep", ";").option("dateFormat", "dd/MM/yyyy").csv("sentimiento_ibex.csv")
    return df, spark_session