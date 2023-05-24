
import pyspark
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()
print(spark)
# df=spark.read.format("csv").option("infrascheme","true").load("C:\\dta")

file_path = "C:\\Users\\iramp\\Downloads\\MyD"
file_format = "json"  # or "csv" or "avro"


def read_file(file_path, file_format):
    if file_format == "csv" or file_format == "txt":
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    if file_format == "json":
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


df = read_file(file_path, file_format)
df.show(5)
