from spark_config import get_spark_session, print_spark_info
import os

print("🚀 Создание Hive-таблицы с правильной схемой...")

spark = get_spark_session("CreateHiveTable")
print_spark_info(spark)

# ============================================
# УДАЛЯЕМ СТАРУЮ ТАБЛИЦУ
# ============================================
try:
    spark.sql("DROP TABLE IF EXISTS cats_raw")
    print("✅ Старая таблица удалена")
except:
    pass

# ============================================
# СОЗДАЁМ ТАБЛИЦУ С ПРАВИЛЬНОЙ СХЕМОЙ
# ============================================
HDFS_PATH = "/user/student/petfinder/train.csv"
print(f"📂 HDFS путь: {HDFS_PATH}")

spark.sql(f"""
CREATE EXTERNAL TABLE cats_raw (
    idx INT,
    Type INT,
    Name STRING,
    Age INT,
    Breed1 INT,
    Breed2 INT,
    Gender INT,
    Color1 INT,
    Color2 INT,
    Color3 INT,
    MaturitySize INT,
    FurLength INT,
    Vaccinated INT,
    Dewormed INT,
    Sterilized INT,
    Health INT,
    Quantity INT,
    Fee INT,
    State INT,
    RescuerID STRING,
    VideoAmt INT,
    Description STRING,
    PetID STRING,
    PhotoAmt INT,
    AdoptionSpeed INT,
    Images STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{HDFS_PATH}'
TBLPROPERTIES ("skip.header.line.count"="1")
""")

print("✅ Таблица cats_raw создана с правильной схемой")

# ============================================
# ПРОВЕРЯЕМ
# ============================================
print("\n📊 Проверка...")
result = spark.sql("SELECT COUNT(*) FROM cats_raw").collect()
print(f"📊 Записей в таблице: {result[0][0]}")

print("\n📊 Первые 5 записей (правильные колонки):")
spark.sql("SELECT Name, Age, Breed1, Type FROM cats_raw LIMIT 5").show()

age_check = spark.sql("SELECT COUNT(*) FROM cats_raw WHERE Age IS NOT NULL").collect()
print(f"📊 Записей с Age NOT NULL: {age_check[0][0]}")

spark.stop()
print("✅ Готово!")