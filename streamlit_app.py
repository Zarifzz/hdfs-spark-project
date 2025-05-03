import streamlit as st
import findspark
findspark.init("/home/zarif/Desktop/hdfs-spark-project/spark-3.5.5-bin-hadoop3")  

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Cyberlog Streamlit App") \
    .getOrCreate()

# Load CSV from HDFS
hdfs_path = "hdfs://localhost:9000/cyberlogs.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)


#-----------------------Seperator---------------------------------------------



# Convert to Pandas for Streamlit display
pandas_df = df.limit(1000).toPandas()  # Limit rows for performance

# Streamlit UI
st.title("Cybersecurity Log Viewer")
st.write("Showing first 1000 rows from cyberlogs.csv:")
st.dataframe(pandas_df)
