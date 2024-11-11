from kafka import KafkaProducer
import pandas as pd
import time
import random
import sys

BOOTSTRAP_SERVER="localhost:9092"
DATASET_FILE="../data/2020-Jan.csv"
TOPIC="quiz-2"

isSilent=False

if len(sys.argv) > 1 and sys.argv[1] == "--silent":
    isSilent=True

# Membaca file CSV
df = pd.read_csv(DATASET_FILE)

# Inisialisasi Kafka Producer
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

# Print info tentang inisialisasi
print(f"Kafka Producer initialized and connected to {BOOTSTRAP_SERVER}.")

# Kirim data ke Kafka
print("sending datas...")
for index, row in df.iterrows():
    # Konversi row menjadi JSON dan encode sebagai bytes
    message = row.to_json().encode('utf-8')

    # Mengirimkan pesan ke topik
    producer.send(TOPIC, message)

    # Debug: Berikan informasi bahwa pesan telah dikirim
    if not isSilent:
        print(f"Data #{index + 1} sent to Kafka.")

    # Sleep random untuk simulasi streaming
    sleep_time = random.uniform(0.5, 2)
    time.sleep(sleep_time)

# Menutup koneksi producer setelah pengiriman selesai
producer.close()
print("Kafka Producer closed.")