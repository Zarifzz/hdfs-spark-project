
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Submitted").getOrCreate()

df = spark.read.format("csv").option("header", True).option("separator", ",").load("hdfs:///cyberlogs.csv")

df_subset = df.select(df.columns[:5])
df_subset.show(n=10)
