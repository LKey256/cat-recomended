 
-- ============================================
-- 1. БАЗОВАЯ СТАТИСТИКА
-- ============================================
-- Сколько всего котов в базе
SELECT COUNT(*) AS total_cats FROM cats_enriched;

-- Распределение по возрастным категориям
SELECT AgeCategory, COUNT(*) AS count, AVG(Age) AS avg_age
FROM cats_enriched
GROUP BY AgeCategory
ORDER BY AgeCategory;

-- ============================================
-- 2. АНАЛИЗ ПОРОД
-- ============================================
-- Топ-10 самых популярных пород
SELECT Breed1, COUNT(*) AS count
FROM cats_enriched
GROUP BY Breed1
ORDER BY count DESC
LIMIT 10;

-- ============================================
-- 3. АНАЛИЗ ПОИСКА (для рекомендаций)
-- ============================================
-- Поиск по имени (например, "Shin Wa")
SELECT PetID, Name, Age, Breed1, Color1 
FROM cats_enriched 
WHERE Name LIKE '%Shin%';

-- Поиск по возрасту (котята до 12 месяцев)
SELECT PetID, Name, Age, Breed1 
FROM cats_enriched 
WHERE Age < 12 
ORDER BY Age 
LIMIT 10;

-- ============================================
-- 4. КЛАСТЕРИЗАЦИЯ
-- ============================================
-- Распределение по кластерам
SELECT cluster, COUNT(*) AS count
FROM cats_clustered
GROUP BY cluster
ORDER BY cluster;

-- Характеристики кластеров (средний возраст)
SELECT c.cluster, AVG(e.Age) AS avg_age
FROM cats_clustered c
JOIN cats_enriched e ON c.PetID = e.PetID
GROUP BY c.cluster
ORDER BY c.cluster;

-- ============================================
-- 5. РЕКОМЕНДАЦИИ
-- ============================================
-- Рекомендации для пользователя 1
SELECT * FROM cat_recommendations WHERE userId = 1;

-- ============================================
-- 6. СЛОЖНЫЙ ЗАПРОС (JOIN + GROUP BY)
-- ============================================
-- Влияние количества фото на скорость усыновления
SELECT 
    CASE 
        WHEN PhotoAmt <= 2 THEN 'Мало фото'
        WHEN PhotoAmt <= 5 THEN 'Средне фото'
        ELSE 'Много фото'
    END AS photo_group,
    AVG(AdoptionSpeed) AS avg_speed,
    COUNT(*) AS count
FROM cats_enriched
GROUP BY 
    CASE 
        WHEN PhotoAmt <= 2 THEN 'Мало фото'
        WHEN PhotoAmt <= 5 THEN 'Средне фото'
        ELSE 'Много фото'
    END;