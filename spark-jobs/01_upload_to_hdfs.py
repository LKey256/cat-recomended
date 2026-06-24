from spark_config import get_spark_session, print_spark_info
import os

print("🚀 Загрузка данных в HDFS как один файл...")

spark = get_spark_session("UploadToHDFS")
print_spark_info(spark)

# Читаем локальный CSV
local_path = "file:///home/student/Project/petfinder_processed/train.csv"
print(f"📂 Читаем файл: {local_path}")

df = spark.read.csv(local_path, header=True, inferSchema=True)
print(f"✅ Загружено записей: {df.count()}")

# Сохраняем как один файл в HDFS
hdfs_path = "hdfs://localhost:9000/user/student/petfinder/train.csv"
print(f"📤 Сохраняем в HDFS: {hdfs_path}")

df.coalesce(1).write.mode("overwrite").option("header", "true").csv(hdfs_path)
print("✅ Данные загружены как один CSV файл")

spark.stop()
print("✅ Готово!")