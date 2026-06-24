from spark_config import get_spark_session, print_spark_info
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import rand, when, col, explode

print("🚀 Запуск рекомендательной системы...")

spark = get_spark_session("CatRecommender")
print_spark_info(spark)

# ============================================
# ЗАГРУЖАЕМ ДАННЫЕ
# ============================================
df = spark.sql("SELECT PetID, cluster FROM cats_clustered WHERE cluster IS NOT NULL")

count = df.count()
print(f"📊 Записей для рекомендаций: {count}")

if count == 0:
    print("⚠️ Нет данных для рекомендаций!")
    spark.stop()
    exit()

# ============================================
# ПРЕОБРАЗУЕМ PetID В ЧИСЛОВОЙ ИНДЕКС
# ============================================
indexer = StringIndexer(inputCol="PetID", outputCol="PetID_index")
df_indexed = indexer.fit(df).transform(df)

# Сохраняем маппинг PetID -> индекс для обратного преобразования
mapping_df = df_indexed.select("PetID", "PetID_index").distinct()
print("✅ PetID преобразован в числовой индекс")

# ============================================
# ГЕНЕРИРУЕМ СИНТЕТИЧЕСКИХ ПОЛЬЗОВАТЕЛЕЙ
# ============================================
users = spark.range(1, 501).toDF("userId")

df_with_cluster = df_indexed.withColumn("cluster", col("cluster"))
users_with_cluster = users.crossJoin(df_with_cluster.select("cluster").distinct())

# Генерируем оценки
ratings = users_with_cluster.withColumn("rating",
    when(col("userId") % 3 == col("cluster"), rand() * 4 + 1)
    .otherwise(rand() * 2)
).filter(col("rating") > 0.5)

ratings_with_pets = ratings.join(
    df_indexed.select("cluster", "PetID_index"), 
    on="cluster"
)

# ============================================
# ALS МОДЕЛЬ
# ============================================
(train, test) = ratings_with_pets.randomSplit([0.8, 0.2], seed=42)

als = ALS(
    maxIter=5,
    regParam=0.1,
    userCol="userId",
    itemCol="PetID_index",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)

print("🚀 Обучение ALS модели...")
model = als.fit(train)
model.write().overwrite().save("../models/als_recommender")
print("✅ Модель сохранена")

# ============================================
# РЕКОМЕНДАЦИИ
# ============================================
user_recs = model.recommendForAllUsers(5)

# Разворачиваем структуру recommendations
user_recs_exploded = user_recs.select(
    col("userId"),
    explode("recommendations").alias("rec")
).select(
    col("userId"),
    col("rec.PetID_index").alias("PetID_index"),
    col("rec.rating").alias("rating")
)

# Присоединяем оригинальные PetID
user_recs_with_petid = user_recs_exploded.join(
    mapping_df,
    on="PetID_index"
).select("userId", "PetID", "rating")

# Сохраняем
user_recs_with_petid.write.mode("overwrite").saveAsTable("cat_recommendations")
print("✅ Рекомендации сохранены в таблицу cat_recommendations")

print("\n📊 Пример рекомендаций для пользователя 1:")
user_recs_with_petid.filter(col("userId") == 1).show(5, truncate=False)

print(f"\n📊 Всего рекомендаций: {user_recs_with_petid.count()}")

spark.stop()
print("✅ Готово!")