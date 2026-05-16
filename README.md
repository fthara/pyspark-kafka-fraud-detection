# 🕵️‍♂️ PySpark & Kafka: Real-Time Fraud Detection

This project is a streaming data processing pipeline (*Structured Streaming*) built with **Apache Kafka** and **Apache Spark (PySpark)**. 

It simulates a real-time credit card transaction flow and uses a **Sliding Window** in Spark to detect fraudulent activity (e.g., the same user making purchases in different countries within a 2-minute interval).

## 🚀 Architecture

* **Messaging:** Apache Kafka (running via KRaft, without Zookeeper).
* **Processing:** PySpark (running in `local[*]` mode inside the container).
* **Language:** Python 3 (managed locally by `uv`).
* **Infrastructure:** Docker and Docker Compose.

---

## 🛠️ Prerequisites

Make sure you have the following installed on your machine:
* [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
* [Python 3.12+](https://www.python.org/downloads/)
* [uv](https://docs.astral.sh/uv/) (Ultra-fast Python package manager)

Install Python dependencies (for the producer):
```bash
uv add confluent-kafka faker
```

## 🚀 How to Run the Project

You will need two open terminals running simultaneously to execute the producer and the consumer.
Step 1: Start the Infrastructure (Kafka + App Container)

In your terminal, at the root of the project, start the containers in the background:

```bash
docker compose up -d
```

Step 2: Start the Transaction Generator (Producer)
In Terminal 1, start the script that generates legitimate transactions and injects random frauds into Kafka:

```bash
uv run producer.py
```
Step 3: Start the Detection Engine (PySpark Consumer)

In Terminal 2, instruct the Spark container to execute the streaming script.

Note: The command below redirects the Java cache to the /tmp folder to avoid permission issues when downloading the Kafka connector.

```bash
docker compose exec app /opt/spark/bin/spark-submit \
  --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  consumer.py
```

Once Spark loads, it will start analyzing 2-minute windows every 10 seconds. When a fraud occurs in Terminal 1, it will be printed as an alert in Terminal 2, displaying the user_id and the incompatible countries_list.

Cleaning Up the Environment

When you are finished testing, tear down the containers and clean up the volumes (Kafka cache) to avoid leaving orphaned data on your machine:

```bash
docker compose down -v
```

Main File Structure

* docker-compose.yml: Definition of the Kafka services and the Spark environment.
* producer.py: Python script using confluent-kafka that simulates purchases and sends them to the transactions topic.
* consumer.py: PySpark Structured Streaming job containing the time-based aggregation logic (window, collect_set) and fraud rules.