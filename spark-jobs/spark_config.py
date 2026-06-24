# spark_config.py
import os

# ============================================
# НАСТРОЙКА SPARK ДЛЯ VIRTUALBOX
# ============================================
YOUR_IP = "192.168.1.77"

# Устанавливаем переменные окружения
os.environ["SPARK_LOCAL_IP"] = YOUR_IP
os.environ["SPARK_LOCAL_HOSTNAME"] = YOUR_IP

def get_spark_session(app_name):
    """Создаёт SparkSession с правильными настройками"""
    from pyspark.sql import SparkSession
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.master", "local[*]") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.host", YOUR_IP) \
        .config("spark.driver.port", "5001") \
        .config("spark.blockManager.port", "5002") \
        .config("spark.sql.warehouse.dir", "/home/student/hive/warehouse") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .enableHiveSupport() \
        .getOrCreate()

def print_spark_info(spark):
    """Выводит информацию о Spark"""
    print(f"✅ Spark версия: {spark.version}")
    print(f"✅ Spark работает на IP: {YOUR_IP}")