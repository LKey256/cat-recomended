from spark_config import get_spark_session, print_spark_info
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, when
from functools import reduce

print("🚀 Запуск кластеризации...")

spark = get_spark_session("CatClustering")
print_spark_info(spark)

# ============================================
# ЗАГРУЖАЕМ ДАННЫЕ
# ============================================
df = spark.sql("SELECT * FROM cats_enriched")

print(f"📊 Записей до очистки: {df.count()}")

# ============================================
# ОЧИСТКА: ЗАМЕНЯЕМ NULL НА 0
# ============================================
categorical_cols = ["MaturitySize", "FurLength", "Vaccinated", "Sterilized", "Health", "AgeCategory"]
numerical_cols = ["Age", "Fee", "PhotoAmt"]

# Заменяем NULL в числовых колонках на 0
for col_name in numerical_cols:
    df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))

# Заменяем NULL в категориальных на "Unknown"
for col_name in categorical_cols:
    df = df.withColumn(col_name, when(col(col_name).isNull(), "Unknown").otherwise(col(col_name)))

print(f"📊 Записей после очистки: {df.count()}")

# ============================================
# ИНДЕКСАЦИЯ КАТЕГОРИАЛЬНЫХ ПРИЗНАКОВ
# ============================================
indexers = [StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="keep") 
            for col in categorical_cols]

df_indexed = reduce(lambda df, idx: idx.fit(df).transform(df), indexers, df)

# ============================================
# ВЕКТОРИЗАЦИЯ
# ============================================
assembler_inputs = [col + "_idx" for col in categorical_cols] + numerical_cols
assembler = VectorAssembler(
    inputCols=assembler_inputs,
    outputCol="features_raw",
    handleInvalid="keep"
)
df_features = assembler.transform(df_indexed)

# ============================================
# МАСШТАБИРОВАНИЕ
# ============================================
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)

# ============================================
# KMEANS КЛАСТЕРИЗАЦИЯ
# ============================================
kmeans = KMeans().setK(5).setSeed(1).setFeaturesCol("features").setPredictionCol("cluster")
model = kmeans.fit(df_scaled)
clustered_df = model.transform(df_scaled)

# ============================================
# СОХРАНЯЕМ
# ============================================
clustered_df.select("PetID", "Name", "Age", "cluster").write.mode("overwrite").saveAsTable("cats_clustered")
print("✅ Кластеризация сохранена в таблицу cats_clustered")

# ============================================
# РАСПРЕДЕЛЕНИЕ ПО КЛАСТЕРАМ
# ============================================
print("\n📊 Распределение по кластерам:")
spark.sql("""
SELECT cluster, COUNT(*) as count
FROM cats_clustered
GROUP BY cluster
ORDER BY cluster
""").show()

spark.stop()
print("✅ Готово!")