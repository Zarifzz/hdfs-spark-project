


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




#--------------------------------------------------------------------------------



import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Streamlit Application Layout
st.set_page_config(page_title="Security Incident Monitoring", layout="wide")
st.title("Monitor and Analyze Security Incidents Across Your Network")

# Sidebar - Settings
st.sidebar.header("Log Analysis Settings")
# dataset_url = st.sidebar.text_input("Enter Dataset URL (HDFS or Local Path)", value="hdfs://localhost:9000/cyberlogs.csv")

# Loading Data
if st.sidebar.button("Load Data"):
try:
df_spark = df
st.success("Data loaded successfully.")

# Displaying Overview
st.subheader("Overview")
total_incidents = df_spark.count()
alert_triggered = df_spark.filter(df_spark['Alerts'] == 'Alert Triggered').count()
high_severity = df_spark.filter(df_spark['Anomaly Score'] > 70).count()

st.metric("Total Incidents", total_incidents)
st.metric("Alerts Triggered", alert_triggered)
st.metric("High Severity", high_severity)

# Displaying Data
st.subheader("Log Data")
pandas_df = df_spark.limit(1000).toPandas()
st.dataframe(pandas_df)

# Data Manipulation Example
st.subheader("Data Manipulation")
http_df = df_spark.filter(df_spark['Traffic Type'] == 'HTTP')
pandas_http_df = http_df.toPandas()
st.dataframe(pandas_http_df)

# Display Charts
st.subheader("Log Analysis")
st.write("Distribution of Anomaly Scores")
plt.figure(figsize=(10, 5))
sns.histplot(pandas_df['Anomaly Score'], kde=True)
st.pyplot(plt)

st.write("Alerts Triggered by Attack Type")
attack_counts = pandas_df['Attack Type'].value_counts()
st.bar_chart(attack_counts)

except Exception as e:
st.error(f"Failed to load or process data: {str(e)}")
