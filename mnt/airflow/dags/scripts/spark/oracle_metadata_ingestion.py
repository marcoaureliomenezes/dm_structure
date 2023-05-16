from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Forex processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.driver.memory", "10G") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')



# Read the file forex_rates.json from the HDFS
df_oracles_metadata = spark.read.parquet('hdfs://namenode:9000/oracles/metadata/')
df_oracles_metadata.printSchema()

df_oracles_metadata.write.mode("overwrite").insertInto("oracles.metadata_pricefeeds", overwrite=True)



