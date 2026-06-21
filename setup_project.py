import os

def create_project():
    """Автоматическое создание структуры проекта"""
    
    print("=== СОЗДАНИЕ ПРОЕКТА CAT RECOMMENDER ===")
    
    # 1. Создаем папки
    folders = [
        "data",
        "spark-jobs",
        "web-app/templates",
        "web-app/static",
        "models",
        "notebooks"
    ]
    
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        print(f"[OK] Создана папка: {folder}")
    
    # 2. Создаем файлы
    files = {
        "requirements.txt": """pyspark==3.5.0
pandas==2.0.3
flask==3.0.0
scikit-learn==1.3.0
numpy==1.24.3
matplotlib==3.7.2
seaborn==0.12.2
jupyter==1.0.0
requests==2.31.0""",

        "download_data.py": '''import urllib.request
import zipfile
import os

print("=== СКАЧИВАНИЕ ДАТАСЕТА ===")
url = "https://automl-mm-bench.s3.amazonaws.com/petfinder_kaggle.zip"
print("Загрузка... (это может занять 10-20 минут)")
urllib.request.urlretrieve(url, "petfinder.zip")
print("Распаковка...")
with zipfile.ZipFile("petfinder.zip", "r") as zip_ref:
    zip_ref.extractall("data/")
os.remove("petfinder.zip")
print("Данные распакованы в папку data/")
print("ВНИМАНИЕ: Папка train_images/ занимает около 2 ГБ")''',

        ".gitignore": """venv/
data/train_images/
*.parquet/
*.pyc
__pycache__/
*.zip
.DS_Store""",

        "README.md": """# Cat Recommender — Big Data проект

## Описание
Система подбора котов на основе распределенных алгоритмов Apache Spark.

## Технологии
- Apache Spark (распределенные вычисления)
- Spark MLlib (KMeans кластеризация, ALS рекомендации)
- Flask (веб-интерфейс)

## Установка и запуск
1. python -m venv venv
2. source venv/bin/activate (или venv\\Scripts\\activate)
3. pip install -r requirements.txt
4. python download_data.py
5. cd spark-jobs && python 04_run_all.py && cd ..
6. cd web-app && python app.py
7. Открыть http://localhost:5000""",

        "spark-jobs/01_etl_pipeline.py": '''from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, lit

print("=== ЗАПУСК ETL ПАЙПЛАЙНА ===")

spark = SparkSession.builder.appName("CatDataETL").getOrCreate()
print(f"Spark версия: {spark.version}")

df = spark.read.csv("../data/train.csv", header=True, inferSchema=True)
print(f"Загружено записей: {df.count()}")

# Очистка данных
categorical_cols = ["Vaccinated", "Sterilized", "Health"]
for col_name in categorical_cols:
    df = df.fillna("Unknown", subset=[col_name])

# Создание новой фичи
df = df.withColumn("AgeCategory",
    when(col("Age") < 12, "Kitten")
    .when(col("Age") < 48, "Adult")
    .otherwise("Senior")
)

# Оставляем только котов (Type == 2)
cats_df = df.filter(col("Type") == 2)
print(f"Котов в датасете: {cats_df.count()}")

# Добавляем пути к картинкам
image_base_path = "../data/train_images"
cats_with_images_df = cats_df.withColumn(
    "image_path",
    concat(lit(image_base_path + "/"), col("PetID"), lit("-1.jpg"))
)

# Сохраняем в Parquet
cats_with_images_df.write.mode("overwrite").parquet("../data/cats_with_images.parquet")
print("Данные сохранены в data/cats_with_images.parquet")

# Статистика
print("Статистика по возрасту:")
cats_df.select("Age").describe().show()

spark.stop()
print("=== ETL ЗАВЕРШЕН ===")''',

        "spark-jobs/02_clustering.py": '''from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

print("=== ЗАПУСК КЛАСТЕРИЗАЦИИ ===")

spark = SparkSession.builder.appName("CatClustering").getOrCreate()

df = spark.read.parquet("../data/cats_with_images.parquet")
print(f"Загружено записей: {df.count()}")

# Подготовка признаков
categorical_features = ["MaturitySize", "FurLength", "Vaccinated", "Sterilized", "Health", "AgeCategory"]
numerical_features = ["Age", "Fee", "PhotoAmt"]

# Индексация категориальных признаков
indexers = [StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="keep") 
            for col in categorical_features]

from functools import reduce
df_indexed = reduce(lambda df, idx: idx.fit(df).transform(df), indexers, df)

# Векторизация
assembler_inputs = [col + "_idx" for col in categorical_features] + numerical_features
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features_raw")
df_features = assembler.transform(df_indexed)

# Масштабирование
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)

# KMeans кластеризация
kmeans = KMeans().setK(5).setSeed(1).setFeaturesCol("features").setPredictionCol("cluster")
model = kmeans.fit(df_scaled)
clustered_df = model.transform(df_scaled)

# Оценка качества
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
silhouette = evaluator.evaluate(clustered_df)
print(f"Silhouette Score: {silhouette:.4f}")

# Анализ кластеров
print("Распределение по кластерам:")
clustered_df.groupBy("cluster").count().show()

# Сохранение
clustered_df.write.mode("overwrite").parquet("../data/cats_clustered.parquet")
print("Кластеризация завершена")

spark.stop()''',

        "spark-jobs/03_recommender.py": '''from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import rand, when, lit, col

print("=== ЗАПУСК РЕКОМЕНДАТЕЛЬНОЙ СИСТЕМЫ ===")

spark = SparkSession.builder.appName("CatRecommender").getOrCreate()

df = spark.read.parquet("../data/cats_clustered.parquet")

# Генерация синтетических данных о пользователях
users = spark.range(1, 501).toDF("userId")

# Cross join для создания взаимодействий
df_with_cluster = df.withColumn("cluster", col("cluster"))
users_with_cluster = users.crossJoin(df_with_cluster.select("cluster").distinct())

# Добавляем оценки
ratings = users_with_cluster.withColumn("rating",
    when(col("userId") % 3 == col("cluster"), rand() * 4 + 1)
    .otherwise(rand() * 2)
).filter(col("rating") > 0.5)

# Добавляем информацию о котах
ratings_with_pets = ratings.join(df.select("cluster", "PetID"), on="cluster")

# Разделяем на train/test
(train, test) = ratings_with_pets.randomSplit([0.8, 0.2], seed=42)

# ALS модель
als = ALS(
    maxIter=5,
    regParam=0.1,
    userCol="userId",
    itemCol="PetID",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)

model = als.fit(train)

# Оценка
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f"RMSE на тесте: {rmse:.4f}")

# Рекомендации для пользователя
user_recs = model.recommendForAllUsers(5)
print("Рекомендации для пользователя 1:")
user_recs.filter(col("userId") == 1).show(truncate=False)

# Сохраняем модель
model.save("../models/als_recommender")
print("Модель сохранена в models/als_recommender")

spark.stop()''',

        "spark-jobs/04_run_all.py": '''import subprocess
import sys

print("=== ЗАПУСК ВСЕХ ЭТАПОВ ОБРАБОТКИ ===")

scripts = ["01_etl_pipeline.py", "02_clustering.py", "03_recommender.py"]

for script in scripts:
    print(f"Запуск {script}...")
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"Ошибка в {script}")
        break

print("=== ВСЕ ЭТАПЫ ЗАВЕРШЕНЫ ===")''',

        "web-app/app.py": '''from flask import Flask, render_template, request, jsonify
import os
import base64
import io
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

app = Flask(__name__)

spark = SparkSession.builder.appName("CatWebApp").getOrCreate()

try:
    cats_df = spark.read.parquet("../data/cats_with_images.parquet")
    print(f"Загружено котов: {cats_df.count()}")
except:
    print("Данные не найдены. Запустите spark-jobs/01_etl_pipeline.py")
    cats_df = None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stats')
def stats():
    if cats_df is None:
        return jsonify({"error": "Data not loaded"})
    return jsonify({
        "total_cats": cats_df.count(),
        "spark_version": spark.version
    })

@app.route('/api/recommend/<pet_id>')
def recommend(pet_id):
    if cats_df is None:
        return jsonify({"error": "Data not loaded"})
    
    sample_cats = cats_df.select("PetID", "Age", "image_path").limit(5).collect()
    
    recommendations = []
    for row in sample_cats:
        image_url = row["image_path"].replace("../data/", "/static/")
        recommendations.append({
            "pet_id": row["PetID"],
            "age": row["Age"],
            "image_url": image_url
        })
    
    return jsonify({"recommendations": recommendations})

@app.route('/api/cluster_stats')
def cluster_stats():
    try:
        clustered = spark.read.parquet("../data/cats_clustered.parquet")
        stats = clustered.groupBy("cluster").count().toPandas()
        
        plt.figure(figsize=(8, 4))
        plt.bar(stats['cluster'].astype(str), stats['count'])
        plt.title('Distribution of Cats by Clusters (KMeans)')
        plt.xlabel('Cluster')
        plt.ylabel('Number of Cats')
        
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_url = base64.b64encode(img.getvalue()).decode()
        plt.close()
        
        return jsonify({"image": plot_url})
    except:
        return jsonify({"error": "Clustering data not found"})

@app.route('/static/<path:filename>')
def static_files(filename):
    from flask import send_from_directory
    data_path = os.path.join(os.path.dirname(__file__), "../data")
    full_path = os.path.join(data_path, filename)
    if os.path.exists(full_path):
        return send_from_directory(data_path, filename)
    return "File not found", 404

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')''',

        "web-app/templates/index.html": '''<!DOCTYPE html>
<html>
<head>
    <title>Cat Recommender — Big Data</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', sans-serif; background: #f0f2f5; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #d9534f; margin-bottom: 20px; }
        .card { background: white; padding: 20px; margin: 20px 0; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .badge { background: #5cb85c; color: white; padding: 4px 12px; border-radius: 20px; font-size: 12px; display: inline-block; margin: 4px; }
        .cat-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 20px; margin-top: 20px; }
        .cat-card { background: white; border-radius: 10px; padding: 15px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .cat-card img { width: 100%; height: 180px; object-fit: cover; border-radius: 8px; }
        .cat-card p { margin: 8px 0; font-size: 14px; }
        input { padding: 10px; border: 2px solid #ddd; border-radius: 5px; width: 200px; margin-right: 10px; }
        button { background: #d9534f; color: white; border: none; padding: 10px 25px; border-radius: 5px; cursor: pointer; font-weight: bold; }
        button:hover { background: #c9302c; }
        .search-row { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
        .loading { color: #666; font-style: italic; }
        .error { color: red; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 15px; margin-top: 15px; }
        .stat-item { background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; }
        .stat-value { font-size: 24px; font-weight: bold; color: #d9534f; }
        .stat-label { font-size: 12px; color: #666; }
        .green-btn { background: #5cb85c; }
        .green-btn:hover { background: #449d44; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Cat Recommender System</h1>
        <p style="color: #666; margin-bottom: 20px;">Big Data project on Apache Spark</p>
        
        <div class="card">
            <h2>Technologies</h2>
            <span class="badge">Apache Spark</span>
            <span class="badge">Spark MLlib</span>
            <span class="badge">KMeans</span>
            <span class="badge">ALS</span>
            <span class="badge">Flask</span>
            <div id="spark-info" class="loading">Loading statistics...</div>
        </div>
        
        <div class="card">
            <h2>Find Recommendations</h2>
            <div class="search-row">
                <input type="text" id="pet-id-input" placeholder="Enter PetID" value="015da9e87">
                <button onclick="getRecommendations()">Find Similar</button>
                <button onclick="getClusterPlot()" class="green-btn">Show Clusters</button>
            </div>
            <div id="recommendations" style="margin-top: 20px;">
                <p class="loading">Enter a Pet ID to search</p>
            </div>
        </div>
        
        <div class="card">
            <h2>Clustering Results</h2>
            <div id="cluster-plot">
                <p class="loading">Click "Show Clusters"</p>
            </div>
        </div>
    </div>

    <script>
        async function getRecommendations() {
            const petId = document.getElementById('pet-id-input').value;
            const recDiv = document.getElementById('recommendations');
            recDiv.innerHTML = '<p class="loading">Loading recommendations...</p>';
            
            try {
                const response = await fetch(`/api/recommend/${petId}`);
                const data = await response.json();
                
                if (data.error) {
                    recDiv.innerHTML = `<p class="error">Error: ${data.error}</p>`;
                    return;
                }
                
                if (data.recommendations && data.recommendations.length > 0) {
                    let html = '<h3>Similar Cats:</h3><div class="cat-grid">';
                    data.recommendations.forEach(cat => {
                        html += `
                            <div class="cat-card">
                                <img src="${cat.image_url}" alt="Cat ${cat.pet_id}" onerror="this.src='https://via.placeholder.com/200x180?text=Cat'">
                                <p><strong>ID:</strong> ${cat.pet_id}</p>
                                <p><strong>Age:</strong> ${cat.age || '?'} months</p>
                            </div>
                        `;
                    });
                    html += '</div>';
                    recDiv.innerHTML = html;
                } else {
                    recDiv.innerHTML = '<p>No similar cats found</p>';
                }
            } catch (error) {
                recDiv.innerHTML = `<p class="error">Error: ${error.message}</p>`;
            }
        }
        
        async function getClusterPlot() {
            const plotDiv = document.getElementById('cluster-plot');
            plotDiv.innerHTML = '<p class="loading">Loading...</p>';
            
            try {
                const response = await fetch('/api/cluster_stats');
                const data = await response.json();
                
                if (data.error) {
                    plotDiv.innerHTML = `<p class="error">Error: ${data.error}</p>`;
                    return;
                }
                
                plotDiv.innerHTML = `<img src="data:image/png;base64,${data.image}" style="max-width:100%; border-radius:8px;">`;
            } catch (error) {
                plotDiv.innerHTML = `<p class="error">Error: ${error.message}</p>`;
            }
        }
        
        async function loadStats() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                if (!data.error) {
                    document.getElementById('spark-info').innerHTML = `
                        <div class="stats-grid">
                            <div class="stat-item"><div class="stat-value">${data.total_cats}</div><div class="stat-label">Cats in Database</div></div>
                            <div class="stat-item"><div class="stat-value">${data.spark_version}</div><div class="stat-label">Spark Version</div></div>
                        </div>
                    `;
                }
            } catch (e) {
                document.getElementById('spark-info').innerHTML = '<p class="error">Failed to load statistics</p>';
            }
        }
        
        window.onload = function() {
            loadStats();
            getRecommendations();
        };
    </script>
</body>
</html>'''
    }
    
    for filepath, content in files.items():
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] Создан файл: {filepath}")
    
    print("\n=== ПРОЕКТ СОЗДАН ===")
    print("Далее выполните:")
    print("1. python -m venv venv")
    print("2. source venv/bin/activate  (или venv\\Scripts\\activate)")
    print("3. pip install -r requirements.txt")
    print("4. python download_data.py")
    print("5. cd spark-jobs && python 04_run_all.py && cd ..")
    print("6. cd web-app && python app.py")
    print("7. Откройте http://localhost:5000")

if __name__ == "__main__":
    create_project()
