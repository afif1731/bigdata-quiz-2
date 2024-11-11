from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
import json
import pandas as pd
import logging

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumerSpark")

# Konfigurasi Kafka
BOOTSTRAP_SERVER = "localhost:9092"
GROUP_ID = "group"
TOPIC = "quiz-2"

# Inisialisasi Kafka Consumer
try:
    consumer = KafkaConsumer(
        TOPIC,
        group_id=GROUP_ID,
        bootstrap_servers=BOOTSTRAP_SERVER,
        auto_offset_reset='earliest',  # Mulai dari offset paling awal jika tidak ada commit offset
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info("Kafka Consumer connected successfully.")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    raise SystemExit(e)

# Inisialisasi Spark Session
spark = SparkSession.builder.appName("BigDataModelTraining").getOrCreate()

# Variabel untuk menyimpan batch data
batch_size = 10
batch_data = []

def train_als_model(data, i, rank=10, max_iter=5, reg_param=0.1):
    if not data:
        logger.warning("No data provided for training.")
        return

    # Konversi ke Pandas DataFrame
    df = pd.DataFrame(data)
    
    # Validasi apakah kolom yang diperlukan ada
    required_columns = {"user_id", "product_id", "price"}
    if not required_columns.issubset(df.columns):
        logger.error(f"Data missing required columns: {required_columns}")
        return

    # Konversi Pandas DataFrame ke Spark DataFrame
    try:
        spark_df = spark.createDataFrame(df)
        als = ALS(
            userCol="user_id",
            itemCol="product_id",
            ratingCol="price",
            rank=rank,
            maxIter=max_iter,
            regParam=reg_param,
            coldStartStrategy="drop"  # Mengatasi NaN pada prediksi
        )
        model = als.fit(spark_df)
        model.save(f"./models/model_{i}")
        logger.info(f"Model {i} saved successfully.")
    except Exception as e:
        logger.error(f"Failed to train ALS model: {e}")

# Loop untuk mengonsumsi data dari Kafka
i = 0

try:
    for message in consumer:
        # Tambahkan data pesan yang diterima ke dalam batch
        batch_data.append(message.value)
        
        # Debug: Print ukuran batch yang telah terisi
        logger.info(f"Batch size: {len(batch_data)} (Waiting for {batch_size - len(batch_data)} more records to reach {batch_size}).")
        
        # Jika sudah mencapai ukuran batch yang diinginkan
        if len(batch_data) >= batch_size:
            i += 1
            # Proses batch data
            logger.info(f"Processing batch of {batch_size} records...")

            # Debug: Tampilkan contoh data yang akan diproses
            logger.debug(f"Example data from the batch: {batch_data[0]}")

            # Training Data
            train_als_model(data=batch_data, i=i)

            # Kosongkan batch_data setelah disimpan
            batch_data = []

        # Hentikan setelah 3 batch
        if i == 3:
            logger.info("Processed 3 batches. Stopping...")
            break
finally:
    # Hentikan Spark Session
    spark.stop()
    logger.info("Spark session stopped.")
    consumer.close()
    logger.info("Kafka consumer closed.")
