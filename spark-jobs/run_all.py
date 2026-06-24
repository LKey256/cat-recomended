import subprocess
import sys
import os

print("🐱 Запуск всех этапов...\n")

os.chdir(os.path.dirname(os.path.abspath(__file__)))

scripts = [
    "01_upload_to_hdfs.py",
    "02_create_hive_table.py",
    "03_etl_pipeline.py",
    "04_clustering.py",
    "05_recommender.py"
]

for script in scripts:
    print(f"\n{'='*50}")
    print(f"▶️  Запуск {script}...")
    print('='*50)
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"❌ Ошибка в {script}")
        break

print("\n✅ Все этапы завершены!")