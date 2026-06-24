from spark_config import get_spark_session, print_spark_info
from pyspark.sql.functions import col, when, concat, lit

print("🚀 Запуск ETL через Hive с фильтрацией NULL...")

spark = get_spark_session("CatETL")
print_spark_info(spark)

# ============================================
# УДАЛЯЕМ СТАРУЮ ТАБЛИЦУ
# ============================================
spark.sql("DROP TABLE IF EXISTS cats_enriched")
print("✅ Старая таблица удалена")

# ============================================
# СОЗДАЁМ НОВУЮ ТАБЛИЦУ ТОЛЬКО С КОТАМИ И НЕ-NULL ВОЗРАСТОМ
# ============================================
spark.sql("""
CREATE TABLE cats_enriched AS
SELECT 
    Type,
    Name,
    Age,
    Breed1,
    Breed2,
    Gender,
    Color1,
    Color2,
    Color3,
    MaturitySize,
    FurLength,
    Vaccinated,
    Dewormed,
    Sterilized,
    Health,
    Quantity,
    Fee,
    State,
    RescuerID,
    VideoAmt,
    Description,
    PetID,
    PhotoAmt,
    AdoptionSpeed,
    Images,
    CASE 
        WHEN Age < 12 THEN 'Kitten'
        WHEN Age < 48 THEN 'Adult'
        ELSE 'Senior'
    END AS AgeCategory,
    CONCAT(Name, ' (', Age, ' мес.)') AS DisplayName
FROM cats_raw
WHERE Type = 2
AND Age IS NOT NULL
AND Age > 0
""")

print("✅ Таблица cats_enriched создана (только коты с возрастом)")

# ============================================
# СТАТИСТИКА
# ============================================
stats = spark.sql("""
SELECT 
    COUNT(*) AS total_cats,
    AVG(Age) AS avg_age,
    COUNT(DISTINCT Breed1) AS distinct_breeds
FROM cats_enriched
""").collect()

total = stats[0]['total_cats'] if stats[0]['total_cats'] is not None else 0
avg_age = stats[0]['avg_age'] if stats[0]['avg_age'] is not None else 0
breeds = stats[0]['distinct_breeds'] if stats[0]['distinct_breeds'] is not None else 0

print(f"📊 Котов с возрастом: {total}")
print(f"📊 Средний возраст: {avg_age:.1f} мес." if avg_age > 0 else "📊 Средний возраст: N/A")
print(f"📊 Уникальных пород: {breeds}")

# ============================================
# ПРИМЕР ДАННЫХ
# ============================================
print("\n📊 Пример данных (коты с возрастом):")
spark.sql("SELECT Name, Age, AgeCategory FROM cats_enriched LIMIT 10").show()

# ============================================
# РАСПРЕДЕЛЕНИЕ ПО ВОЗРАСТНЫМ КАТЕГОРИЯМ
# ============================================
print("\n📊 Распределение по возрастным категориям:")
spark.sql("""
SELECT AgeCategory, COUNT(*) as count
FROM cats_enriched
GROUP BY AgeCategory
ORDER BY AgeCategory
""").show()

spark.stop()
print("✅ Готово!")