
from pyspark.sql.functions import *

def crear_fact_cotizaciones(spark):

    # Leer las tablas Silver
    ibex = spark.read.format("delta").load(
        "data/lake/silver/ibex"
    )

    sentimiento = spark.read.format("delta").load(
        "data/lake/silver/sentimiento"
    )

    empresa = spark.read.format("delta").load(
        "data/lake/silver/dim_empresa"
    )

    fecha = spark.read.format("delta").load(
        "data/lake/gold/dim_fecha"
    )

    # Normalizar sectores para poder cruzarlos
    sentimiento = sentimiento.withColumn(
        "Sector", lower(trim(col("Sector")))
    )

    # Unir IBEX con DIM_EMPRESA
    fact = ibex.join(
        empresa,
        on="Ticker",
        how="inner"
    )

    # Unir con DIM_FECHA
    fact = fact.join(
        fecha,
        to_date(fact["Date"]) == fecha["Fecha"],
        "inner"
    )

    # Unir con sentimiento por fecha y sector
    fact = fact.join(
        sentimiento,
        (to_date(fact["Date"]) == sentimiento["Fecha"]) &
        (fact["Sector"] == sentimiento["Sector"]),
        "left"
    )

    # Seleccionar columnas de la tabla de hechos
    fact = fact.select(
        ibex["Ticker"],
        to_date(ibex["Date"]).alias("Fecha"),
        ibex["Open"],
        ibex["High"],
        ibex["Low"],
        ibex["Close"],
        ibex["Adj_Close"],
        ibex["Volume"],
        ibex["Dividends"],
        ibex["Stock_Splits"],
        empresa["Sector"],
        sentimiento["Sentimiento"]
    )

    # Guardar tabla de hechos
    fact.write.format("delta").mode("overwrite").save(
        "data/lake/gold/fact_cotizaciones"
    )

    print("Filas FACT_COTIZACIONES:", fact.count())
    fact.show(5, truncate=False)

    return fact
