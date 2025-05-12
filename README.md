# Cyberlog Analyzer: HDFS + PySpark + Streamlit

This project demonstrates an end-to-end data analysis pipeline using Hadoop HDFS, YARN, and Apache Spark to process cybersecurity log data (`cyberlogs.csv`). The processed data is visualized using a lightweight Streamlit web application.

---

## 📁 Project Structure

```
.
├── hadoop-3.4.1/etc/hadoop            # Contains etc/hadoop/ configuration files
├── spark-3.5.5-bin-hadoop3/conf       # Contains conf/ configuration files
├── hdfs                               # Contains NameNode and DataNode
├── streamlit_app.py                   # Streamlit + PySpark application
├── cyberlogs.csv                      # Cybersecurity logs dataset
├── requirements.txt                   # Python dependencies
└── README.md
```

---

## 🚀 Deployment Instructions

### 1. Prerequisites

Install the following on your Linux system:

* Java (OpenJDK 11 only)
* Hadoop (with HDFS + YARN)
* Apache Spark
* Python 3.8+ with pip
* Passwordless SSH on own user

### 2. Clone the Repository

```bash
git clone https://github.com/yourusername/cyberlog-analyzer.git
cd cyberlog-analyzer
```

> Replace `yourusername` with your actual GitHub username.

### 3. Replace Configuration Files

Overwrite your local Hadoop and Spark configuration files with the ones in this repository:

```bash
cp -r hadoop/etc/hadoop/* $HADOOP_HOME/etc/hadoop/
cp -r spark/conf/* $SPARK_HOME/conf/
```

Adjust `$HADOOP_HOME` and `$SPARK_HOME` based on where you've installed Hadoop and Spark.
Adjust the Config paths (Java bin, hdfs dirs, etc.) content to the correct path on your local machine (dirs, files and bins vary on system to system):
* `$HADOOP_HOME/etc/hadoop/hdfs-site.xml` (namenode & datanode dir) - (prob need to format it again `$HADOOP_HOME/bin/hdfs namenode -format`)
* `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` (Java 11 bin)
* `$SPARK_HOME/conf/spark-env.sh` (hadoop & yarn conf, python)

---

## ⚙️ Instructions to Running the Project

### Step 1: Start HDFS and YARN

navigate to `$HADOOP_HOME/sbin/`
```bash
./start-dfs.sh
./start-yarn.sh
```

### Step 2: Put the Dataset into HDFS

navigate to `$HADOOP_HOME/bin/`
```bash
./hdfs dfs -mkdir -p /
./hdfs dfs -put cyberlogs.csv /
```

### Step 3: Install Python Dependencies

We recommend using a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 4: Run the Streamlit Application

```bash
streamlit run app.py
```
or 
```bash
python3 -m streamlit run app.py
```

Once started, navigate to [Localhost 8501](http://localhost:8501) in your browser.

---

## ✅ Features

* Distributed storage and processing using HDFS, YARN, and Spark
* Interactive visualization of log data using Streamlit
* Unified PySpark + Streamlit pipeline in a single script

---

## 📌 Notes

* This project was built and tested on a local single-node Linux environment.
* Make sure your Hadoop and Spark installations match the configurations used in this repo.
* Performance is optimized for small-to-medium datasets (\~40,000 records).

---

