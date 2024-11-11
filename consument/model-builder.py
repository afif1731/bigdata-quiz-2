from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS

# Inisialisasi Spark Session
spark = SparkSession.builder.appName("BigDataModelTraining").getOrCreate()

# Membaca data dari CSV (asumsikan data sudah disimpan dalam file)
df = spark.read.csv('batch_data.csv', header=True, inferSchema=True)

# Skema (a) Membagi Data ke 3 bagian berdasarkan waktu atau jumlah data
total_rows = df.count()
split1 = total_rows // 3
split2 = 2 * (total_rows // 3)

# Membagi data menjadi 3 bagian
df_part1 = df.limit(split1)  # Model 1: Data 1/3 pertama
df_part2 = df.limit(split2).subtract(df_part1)  # Model 2: Data 1/3 kedua
df_part3 = df.subtract(df_part1).subtract(df_part2)  # Model 3: Data 1/3 terakhir

# Fungsi untuk melatih model menggunakan ALS
def train_als_model(data, rank=10, max_iter=5, reg_param=0.1):
    als = ALS(userCol="user_id", itemCol="product_id", ratingCol="price", rank=rank, maxIter=max_iter, regParam=reg_param)
    model = als.fit(data)
    return model

# Latih model untuk masing-masing bagian
model1 = train_als_model(df_part1)
model2 = train_als_model(df_part1.union(df_part2))
model3 = train_als_model(df_part1.union(df_part2).union(df_part3))

# Simpan model
model1.save("model1")
model2.save("model2")
model3.save("model3")

# Stop Spark session
spark.stop()