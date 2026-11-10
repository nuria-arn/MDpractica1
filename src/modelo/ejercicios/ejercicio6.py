from delta.tables import DeltaTable
from pyspark.sql.functions import *

def ejercicio6(spark):
    print("Ejercicio 6:")

    destino = DeltaTable.forPath(spark, "data/lake/bronze/ibex")
    destino.restoreToVersion(1)