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


# import streamlit as st
# from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, avg, count

from pyspark.sql.functions import col, count, to_date
import pandas as pd
import altair as alt

# -------------------- PySpark Analysis (All Up Front) --------------------

# 1. Top 10 Attack Types


# Filter to valid attack types and remove nulls
filtered_attack_types_df = (
    df.filter(
        col("Attack Type").isNotNull() &
        col("Attack Type").isin("Malware", "DDoS", "Intrusion")
    )
    .groupBy("Attack Type")
    .agg(count("*").alias("Count"))
)


# 2. Attack Frequency Over Time

attack_freq_monthly_df = (
    df
    .filter(col("Timestamp").isNotNull())
    .withColumn("Timestamp", to_timestamp("Timestamp"))
    .withColumn("Month", date_format("Timestamp", "yyyy-MM"))
    .groupBy("Month")
    .agg(count("*").alias("Event Count"))
    .orderBy("Month")
)



# 3. Action Taken Distribution


# Remove nulls and count action types
action_taken_df = (
    df.filter(col("Action Taken").isNotNull())
    .groupBy("Action Taken")
    .agg(count("*").alias("Count"))
    .orderBy("Count", ascending=False)
)


# 4. Anomaly Score Distribution


anomaly_monthly_df = (
    df
    .filter(col("Anomaly Scores").isNotNull() & col("Timestamp").isNotNull())
    .withColumn("Anomaly Scores", col("Anomaly Scores").cast("float"))
    .withColumn("Timestamp", to_timestamp("Timestamp"))
    .withColumn("Month", date_format("Timestamp", "yyyy-MM"))
    .groupBy("Month")
    .agg(avg("Anomaly Scores").alias("Average Anomaly Score"))
    .orderBy("Month")
)



# -------------------- Convert to Pandas for Display --------------------
filtered_attack_types_pd = filtered_attack_types_df.toPandas()
attack_freq_monthly_pd = attack_freq_monthly_df.toPandas()
action_taken_pd = action_taken_df.toPandas()
anomaly_monthly_pd = anomaly_monthly_df.toPandas()


# -------------------- Streamlit UI & Charts --------------------

st.title("Cybersecurity Log Analysis Dashboard")

st.subheader("📄 Raw Data Sample")
st.dataframe(df.limit(10).toPandas())

# Top Attack Types
st.subheader("🛡️  Top Attack Types")

if not filtered_attack_types_pd.empty:
    pie_chart = alt.Chart(filtered_attack_types_pd).mark_arc().encode(
        theta=alt.Theta(field="Count", type="quantitative"),
        color=alt.Color(field="Attack Type", type="nominal"),
        tooltip=["Attack Type", "Count"]
    ).properties(width=400, height=400)

    st.altair_chart(pie_chart, use_container_width=True)
else:
    st.warning("No valid attack type data to display.")


# Attack Frequency Over Time
st.subheader("📅 Attack Frequency Per Month")

if not attack_freq_monthly_pd.empty:
    attack_line_chart = alt.Chart(attack_freq_monthly_pd).mark_line(point=True).encode(
        x=alt.X("Month:T", title="Month"),
        y=alt.Y("Event Count:Q", title="Number of Attacks", scale=alt.Scale(domain=[0, 1200])),
        tooltip=["Month", "Event Count"]
    ).properties(width=600, height=300)

    st.altair_chart(attack_line_chart, use_container_width=True)
else:
    st.warning("No valid timestamp data available to show monthly attack frequency.")




# Action Taken Distribution
st.subheader("🚨 Actions Taken by Security System")

if not action_taken_pd.empty:
    action_bar_chart = alt.Chart(action_taken_pd).mark_bar().encode(
        x=alt.X("Action Taken:N", sort='-y'),
        y=alt.Y("Count:Q"),
        tooltip=["Action Taken", "Count"]
    ).properties(width=600, height=400)

    st.altair_chart(action_bar_chart, use_container_width=True)
else:
    st.warning("No valid action taken data to display.")


# Anomaly Score Distribution
st.subheader("📈 Average Anomaly Score Per Month")

if not anomaly_monthly_pd.empty:
    score_line_chart = alt.Chart(anomaly_monthly_pd).mark_line(point=True).encode(
        x=alt.X("Month:T", title="Month"),
        y=alt.Y("Average Anomaly Score:Q", title="Avg Anomaly Score"),
        tooltip=["Month", "Average Anomaly Score"]
    ).properties(width=600, height=300)

    st.altair_chart(score_line_chart, use_container_width=True)
else:
    st.warning("No valid monthly anomaly score data available to display.")



