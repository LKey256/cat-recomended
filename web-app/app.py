from flask import Flask, render_template, request, jsonify, send_from_directory
import os
import sys

# ============================================
# ДОБАВЛЯЕМ ПУТЬ К SPARK_JOBS ДЛЯ ИМПОРТА
# ============================================
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../spark-jobs'))

from spark_config import get_spark_session, print_spark_info, YOUR_IP

app = Flask(__name__)

# ============================================
# ПУТЬ К КАРТИНКАМ
# ============================================
IMAGES_DIR = "/home/student/Project/petfinder_processed/train_images"

print("🚀 Запуск веб-приложения...")

# ============================================
# ЗАПУСК SPARK С ПРАВИЛЬНЫМИ НАСТРОЙКАМИ
# ============================================
spark = get_spark_session("CatWebApp")
print_spark_info(spark)

print(f"📂 Картинки: {IMAGES_DIR}")

# ============================================
# ЗАГРУЗКА ДАННЫХ ИЗ HIVE
# ============================================
try:
    cats_df = spark.sql("""
        SELECT PetID, Name, Age, Breed1, Color1, Images
        FROM cats_enriched
    """)
    total_cats = cats_df.count()
    print(f"✅ Загружено котов: {total_cats}")
except Exception as e:
    print(f"⚠️ Ошибка загрузки данных: {e}")
    print("⚠️ Убедитесь, что сначала запущен ETL (03_etl_pipeline.py)")
    cats_df = None
    total_cats = 0

# ============================================
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ КАРТИНОК
# ============================================
def get_image_url(images_str):
    """Извлекает имя первого файла из строки с картинками"""
    if not images_str:
        return ""
    first = images_str.split(';')[0].strip()
    filename = os.path.basename(first)
    return f"/static/images/{filename}"

# ============================================
# ГЛАВНАЯ СТРАНИЦА
# ============================================
@app.route('/')
def index():
    return render_template('index.html')

# ============================================
# API: СТАТИСТИКА
# ============================================
@app.route('/api/stats')
def stats():
    if cats_df is None:
        return jsonify({"error": "Данные не загружены"})
    
    try:
        breed_stats = spark.sql("""
            SELECT Breed1, COUNT(*) AS count
            FROM cats_enriched
            GROUP BY Breed1
            ORDER BY count DESC
            LIMIT 5
        """).collect()
        
        top_breeds = [{"breed": str(row["Breed1"]), "count": row["count"]} for row in breed_stats]
        
        return jsonify({
            "total_cats": total_cats,
            "spark_version": spark.version,
            "top_breeds": top_breeds
        })
    except Exception as e:
        return jsonify({"total_cats": total_cats, "spark_version": spark.version})

# ============================================
# API: ПОИСК ПО ИМЕНИ
# ============================================
@app.route('/api/search')
def search():
    query = request.args.get('q', '')
    age_min = request.args.get('age_min', type=int, default=0)
    age_max = request.args.get('age_max', type=int, default=999)
    
    if cats_df is None:
        return jsonify({"error": "Данные не загружены"})
    
    try:
        query_sql = f"""
            SELECT PetID, Name, Age, Breed1, Color1, Images, AdoptionSpeed
            FROM cats_enriched
            WHERE Name LIKE '%{query}%'
            AND Age BETWEEN {age_min} AND {age_max}
            ORDER BY Age
            LIMIT 20
        """
        
        results = spark.sql(query_sql).collect()
        
        recommendations = []
        for row in results:
            image_url = get_image_url(row["Images"])
            
            recommendations.append({
                "pet_id": row["PetID"],
                "name": row["Name"] if row["Name"] else "Без имени",
                "age": row["Age"],
                "breed": row["Breed1"],
                "color": row["Color1"],
                "image_url": image_url,
                "adoption_speed": row["AdoptionSpeed"]
            })
        
        return jsonify({"recommendations": recommendations})
    except Exception as e:
        return jsonify({"error": str(e)})

# ============================================
# API: РЕКОМЕНДАЦИИ ПО PETID
# ============================================
@app.route('/api/recommend/<pet_id>')
def recommend(pet_id):
    if cats_df is None:
        return jsonify({"error": "Данные не загружены"})
    
    try:
        cat_query = f"""
            SELECT Name, Age, Breed1, Color1, Images
            FROM cats_enriched
            WHERE PetID = '{pet_id}'
            LIMIT 1
        """
        cat = spark.sql(cat_query).collect()
        
        if not cat:
            return jsonify({"error": "Кот не найден"})
        
        similar_query = f"""
            SELECT PetID, Name, Age, Breed1, Color1, Images
            FROM cats_enriched
            WHERE Breed1 = {cat[0]["Breed1"]}
            AND ABS(Age - {cat[0]["Age"]}) < 12
            AND PetID != '{pet_id}'
            LIMIT 5
        """
        
        similar = spark.sql(similar_query).collect()
        
        recommendations = []
        for row in similar:
            image_url = get_image_url(row["Images"])
            recommendations.append({
                "pet_id": row["PetID"],
                "name": row["Name"] if row["Name"] else "Без имени",
                "age": row["Age"],
                "breed": row["Breed1"],
                "color": row["Color1"],
                "image_url": image_url
            })
        
        return jsonify({"recommendations": recommendations})
    except Exception as e:
        return jsonify({"error": str(e)})

# ============================================
# РАЗДАЧА КАРТИНОК
# ============================================
@app.route('/static/images/<path:filename>')
def serve_image(filename):
    """Отдаёт картинки из папки train_images"""
    try:
        full_path = os.path.join(IMAGES_DIR, filename)
        if os.path.exists(full_path):
            return send_from_directory(IMAGES_DIR, filename)
        else:
            # Ищем файл без суффикса
            base = filename.split('-')[0] if '-' in filename else filename
            for f in os.listdir(IMAGES_DIR):
                if f.startswith(base):
                    return send_from_directory(IMAGES_DIR, f)
            return "", 404
    except Exception as e:
        print(f"Ошибка при загрузке картинки: {e}")
        return "", 404

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    if os.path.exists(IMAGES_DIR):
        print(f"✅ Картинки доступны по пути: {IMAGES_DIR}")
    else:
        print(f"⚠️ ВНИМАНИЕ: Папка с картинками не найдена: {IMAGES_DIR}")
    
    app.run(debug=True, port=5000, host='0.0.0.0')