import pandas as pd
import mysql.connector
import json
import os
from datetime import datetime, timedelta
import random

# ------------------ LOGGING -----------------
def log(message):
    os.makedirs("logs", exist_ok=True)
    with open("logs/log.txt", "a") as f:
        f.write(f"{datetime.now()} - {message}\n")


# ------------------ DB CONNECTION---------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root@123"
)

cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS shipping_pipeline")
cursor.execute("USE shipping_pipeline")


# ================== CREATE TABLES ==================
def create_tables():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bronze (
        id INT AUTO_INCREMENT PRIMARY KEY,
        raw_data TEXT,
        ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS silver (
        tracking_id VARCHAR(50),
        carrier VARCHAR(50),
        shipment_date DATETIME,
        delivery_date DATETIME,
        days INT,
        status VARCHAR(50),
        overdue BOOLEAN,
        exception_category VARCHAR(50)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS gold (
        carrier VARCHAR(50),
        avg_days FLOAT,
        delayed_percent FLOAT,
        exception_rate FLOAT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS status_history (
        tracking_id VARCHAR(50),
        status VARCHAR(50),
        timestamp DATETIME
    )
    """)

    conn.commit()


# ================== EXTRACT ==================
def extract_csv(filepath):
    try:
        df = pd.read_csv(filepath)
        print("File read:", filepath)
        log(f"File read: {filepath}")
        return df
    except Exception as e:
        log(f"Error reading file {filepath}: {e}")
        return None


# ================== FAKE API ==================
def generate_shipment_data(df):
    all_data = []

    for _, row in df.iterrows():
        tid = row["tracking_id"]

        # Invalid ID check
        if not str(tid).startswith("T"):
            log(f"Invalid Tracking ID: {tid}")
            continue

        ship_date = datetime.now() - timedelta(days=random.randint(2, 10))

        if random.choice([True, False]):
            delivery_date = ship_date + timedelta(days=random.randint(2, 5))
            status = "Delivered"
        else:
            delivery_date = None
            status = "In Transit"

        all_data.append({
            "tracking_id": tid,
            "carrier": random.choice(["DHL", "FedEx"]),
            "shipment_date": ship_date,
            "delivery_date": delivery_date,
            "status": status
        })

    return pd.DataFrame(all_data)


# ================== LOAD BRONZE ==================
def load_bronze(df):
    query = "INSERT INTO bronze (raw_data) VALUES (%s)"

    data = [
        (json.dumps(row.to_dict(), default=str),)
        for _, row in df.iterrows()
    ]

    cursor.executemany(query, data)
    conn.commit()


# ================== TRANSFORM ==================
def clean_for_silver(df):
    df["shipment_date"] = pd.to_datetime(df["shipment_date"])
    df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")

    df["days"] = (df["delivery_date"] - df["shipment_date"]).dt.days
    df["days"] = df["days"].fillna((pd.Timestamp.now() - df["shipment_date"]).dt.days)

    df["overdue"] = df["days"] > 5

    # Exception category
    def get_category(row):
        if row["status"] == "Delivered":
            return "Delivered"
        elif row["overdue"]:
            return "Delayed"
        else:
            return "In Transit"

    df["exception_category"] = df.apply(get_category, axis=1)

    # Validation
    for _, row in df.iterrows():
        if pd.isnull(row["shipment_date"]):
            log("Missing shipment date")

    return df


# ================== LOAD SILVER ==================
def load_silver(df):
    query = """
    INSERT INTO silver
    (tracking_id, carrier, shipment_date, delivery_date, days, status, overdue, exception_category)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    data = [
        (
            row["tracking_id"],
            row["carrier"],
            row["shipment_date"].strftime('%Y-%m-%d %H:%M:%S'),
            row["delivery_date"].strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row["delivery_date"]) else None,
            int(row["days"]),
            row["status"],
            int(row["overdue"]),
            row["exception_category"]
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany(query, data)
    conn.commit()


# ================== STATUS HISTORY ==================
def save_status_history(df):
    query = "INSERT INTO status_history VALUES (%s, %s, %s)"

    data = [
        (row["tracking_id"], row["status"], datetime.now())
        for _, row in df.iterrows()
    ]

    cursor.executemany(query, data)
    conn.commit()


# ================== GOLD ==================
def generate_gold():
    cursor.execute("DELETE FROM gold")

    cursor.execute("""
    INSERT INTO gold
    SELECT carrier,
           AVG(days),
           SUM(CASE WHEN overdue = 1 THEN 1 ELSE 0 END)*100.0/COUNT(*),
           SUM(CASE WHEN exception_category != 'Delivered' THEN 1 ELSE 0 END)*100.0/COUNT(*)
    FROM silver
    GROUP BY carrier
    """)

    conn.commit()


# ================== PROCESS FILE ==================
def process_file(filepath):
    try:
        log(f"Processing file: {filepath}")

        df = extract_csv(filepath)

        if df is None or df.empty:
            log("Empty file")
            return

        df_api = generate_shipment_data(df)

        load_bronze(df_api)

        df_clean = clean_for_silver(df_api)

        load_silver(df_clean)

        save_status_history(df_api)

        os.makedirs("output", exist_ok=True)
        df_clean.to_csv(f"output/clean_{os.path.basename(filepath)}", index=False)

        log(f"File processed successfully: {filepath}")

    except Exception as e:
        log(f"Error processing file {filepath}: {e}")


# ================== RUN PIPELINE ==================
def run_pipeline():
    log("Pipeline started")

    create_tables()

    for file in os.listdir("data"):
        if file.endswith(".csv"):
            filepath = os.path.join("data", file)
            process_file(filepath)

    generate_gold()

    log("Pipeline completed")
    print("✅ Pipeline Completed 🚀")


# ================== START ==================
if __name__ == "__main__":
    run_pipeline()