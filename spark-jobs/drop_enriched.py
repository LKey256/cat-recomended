from spark_config import get_spark_session, print_spark_info

print("🚀 Удаление таблицы cats_enriched...")

spark = get_spark_session("DropTable")
print_spark_info(spark)

spark.sql("DROP TABLE IF EXISTS cats_enriched")
print("✅ Таблица cats_enriched удалена")

spark.stop()
print("✅ Готово!")