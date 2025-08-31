-- =====================================================
-- ANALYSE EXPLORATOIRE DES DONNÉES EMPLOYÉS AVEC PIG
-- =====================================================
-- Script Pig pour analyser les données des employés
-- Objectif : Statistiques descriptives et analyses par département

-- Configuration pour un meilleur débogage
set job.name 'Employee_Data_Analysis';
set default_parallel 3;

-- =====================================================
-- 1. CHARGEMENT DES DONNÉES
-- =====================================================

-- Charger les données des employés depuis HDFS
employee_data = LOAD '/data/input/sample_data.csv' 
    USING PigStorage(',') 
    AS (
        id:int,
        name:chararray,
        age:int,
        city:chararray,
        salary:double,
        department:chararray,
        hire_date:chararray,
        status:chararray
    );

-- Supprimer la ligne d'en-tête
employee_clean = FILTER employee_data BY id IS NOT NULL AND id > 0;

-- Afficher un échantillon des données
DUMP (LIMIT employee_clean 5);

-- =====================================================
-- 2. STATISTIQUES GLOBALES
-- =====================================================

-- Compter le nombre total d'employés
total_employees = GROUP employee_clean ALL;
employee_count = FOREACH total_employees GENERATE COUNT(employee_clean) as total_count;

STORE employee_count INTO '/data/output/pig/total_employees' USING PigStorage(',');

-- =====================================================
-- 3. ANALYSE PAR DÉPARTEMENT
-- =====================================================

-- Grouper les employés par département
dept_grouped = GROUP employee_clean BY department;

-- Calculer les statistiques par département
dept_stats = FOREACH dept_grouped GENERATE 
    group AS department,
    COUNT(employee_clean) AS employee_count,
    AVG(employee_clean.salary) AS avg_salary,
    MIN(employee_clean.salary) AS min_salary,
    MAX(employee_clean.salary) AS max_salary,
    AVG(employee_clean.age) AS avg_age;

-- Trier par nombre d'employés décroissant
dept_stats_sorted = ORDER dept_stats BY employee_count DESC;

-- Afficher les résultats
DUMP dept_stats_sorted;

-- Sauvegarder les statistiques par département
STORE dept_stats_sorted INTO '/data/output/pig/department_statistics' USING PigStorage(',');

-- =====================================================
-- 4. ANALYSE PAR VILLE
-- =====================================================

-- Grouper par ville
city_grouped = GROUP employee_clean BY city;

-- Compter les employés par ville
city_counts = FOREACH city_grouped GENERATE 
    group AS city,
    COUNT(employee_clean) AS employee_count,
    AVG(employee_clean.salary) AS avg_salary_city;

-- Filtrer les villes avec plus d'un employé
popular_cities = FILTER city_counts BY employee_count > 1;

-- Trier par nombre d'employés
popular_cities_sorted = ORDER popular_cities BY employee_count DESC;

DUMP popular_cities_sorted;

STORE popular_cities_sorted INTO '/data/output/pig/city_analysis' USING PigStorage(',');

-- =====================================================
-- 5. ANALYSE DES TRANCHES D'ÂGE
-- =====================================================

-- Créer des tranches d'âge
employee_with_age_group = FOREACH employee_clean GENERATE 
    *,
    (age < 30 ? 'Young (< 30)' : 
     (age < 40 ? 'Middle (30-39)' : 'Senior (40+)')) AS age_group;

-- Grouper par tranche d'âge
age_group_grouped = GROUP employee_with_age_group BY age_group;

-- Statistiques par tranche d'âge
age_group_stats = FOREACH age_group_grouped GENERATE 
    group AS age_group,
    COUNT(employee_with_age_group) AS count,
    AVG(employee_with_age_group.salary) AS avg_salary,
    AVG(employee_with_age_group.age) AS avg_age;

-- Ordonner par âge moyen
age_group_stats_sorted = ORDER age_group_stats BY avg_age;

DUMP age_group_stats_sorted;

STORE age_group_stats_sorted INTO '/data/output/pig/age_group_analysis' USING PigStorage(',');

-- =====================================================
-- 6. ANALYSE DES SALAIRES
-- =====================================================

-- Créer des tranches de salaires
employee_with_salary_bracket = FOREACH employee_clean GENERATE 
    *,
    (salary < 60000 ? 'Low (< 60K)' : 
     (salary < 80000 ? 'Medium (60K-80K)' : 'High (80K+)')) AS salary_bracket;

-- Grouper par tranche de salaire
salary_bracket_grouped = GROUP employee_with_salary_bracket BY salary_bracket;

-- Statistiques par tranche de salaire
salary_bracket_stats = FOREACH salary_bracket_grouped GENERATE 
    group AS salary_bracket,
    COUNT(employee_with_salary_bracket) AS count,
    AVG(employee_with_salary_bracket.salary) AS avg_salary,
    AVG(employee_with_salary_bracket.age) AS avg_age;

DUMP salary_bracket_stats;

STORE salary_bracket_stats INTO '/data/output/pig/salary_bracket_analysis' USING PigStorage(',');

-- =====================================================
-- 7. ANALYSE DU STATUT (ACTIF/INACTIF)
-- =====================================================

-- Grouper par statut
status_grouped = GROUP employee_clean BY status;

-- Statistiques par statut
status_stats = FOREACH status_grouped GENERATE 
    group AS status,
    COUNT(employee_clean) AS count,
    AVG(employee_clean.salary) AS avg_salary,
    COUNT(employee_clean) * 100.0 / 50 AS percentage; -- 50 = nombre total d'employés

DUMP status_stats;

STORE status_stats INTO '/data/output/pig/status_analysis' USING PigStorage(',');

-- =====================================================
-- 8. TOP 10 DES EMPLOYÉS LES MIEUX PAYÉS
-- =====================================================

-- Ordonner par salaire décroissant
top_earners = ORDER employee_clean BY salary DESC;

-- Prendre les 10 premiers
top_10_earners = LIMIT top_earners 10;

-- Sélectionner les champs pertinents
top_10_formatted = FOREACH top_10_earners GENERATE 
    name,
    department,
    city,
    salary,
    age;

DUMP top_10_formatted;

STORE top_10_formatted INTO '/data/output/pig/top_10_earners' USING PigStorage(',');

-- =====================================================
-- 9. RAPPORT DE SYNTHÈSE
-- =====================================================

-- Créer un rapport consolidé par département avec moyennes
dept_summary = FOREACH dept_grouped GENERATE 
    CONCAT('Department: ', (chararray)group) AS summary_line,
    CONCAT('Employees: ', (chararray)COUNT(employee_clean)) AS employee_info,
    CONCAT('Avg Salary: $', (chararray)ROUND(AVG(employee_clean.salary))) AS salary_info,
    CONCAT('Avg Age: ', (chararray)ROUND(AVG(employee_clean.age))) AS age_info;

-- Aplatir pour créer un rapport linéaire
dept_report = FOREACH dept_summary GENERATE 
    CONCAT(summary_line, ' | ', employee_info, ' | ', salary_info, ' | ', age_info) AS report_line;

DUMP dept_report;

STORE dept_report INTO '/data/output/pig/department_summary_report' USING PigStorage('');

-- =====================================================
-- 10. NETTOYAGE ET VALIDATION DES DONNÉES
-- =====================================================

-- Identifier les données potentiellement incorrectes
anomalies = FILTER employee_clean BY 
    salary < 10000 OR salary > 200000 OR 
    age < 18 OR age > 70;

anomaly_count = FOREACH (GROUP anomalies ALL) GENERATE COUNT(anomalies) as anomaly_count;

DUMP anomaly_count;

STORE anomalies INTO '/data/output/pig/data_anomalies' USING PigStorage(',');

-- Message de fin
-- DUMP (FOREACH (GROUP employee_clean ALL) GENERATE 'ANALYSE TERMINÉE - RÉSULTATS DANS /data/output/pig/');

print 'Analyse Pig terminée avec succès!';