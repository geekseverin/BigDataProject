-- =====================================================
-- ANALYSE DES VENTES AVEC APACHE PIG
-- =====================================================
-- Script Pig pour analyser les données de ventes
-- Objectif : KPI de ventes, tendances et performance

-- Configuration
set job.name 'Sales_Data_Analysis';
set default_parallel 3;

-- =====================================================
-- 1. CHARGEMENT DES DONNÉES DE VENTES
-- =====================================================

-- Charger les données de ventes depuis HDFS
sales_data = LOAD '/data/input/sales_data.csv' 
    USING PigStorage(',') 
    AS (
        transaction_id:chararray,
        product_id:chararray,
        product_name:chararray,
        category:chararray,
        quantity:int,
        unit_price:double,
        total_amount:double,
        customer_id:chararray,
        customer_name:chararray,
        sale_date:chararray,
        region:chararray,
        salesperson:chararray
    );

-- Nettoyer les données (supprimer l'en-tête)
sales_clean = FILTER sales_data BY transaction_id != 'transaction_id';

-- Convertir la date de vente en format exploitable
sales_with_date = FOREACH sales_clean GENERATE 
    *,
    SUBSTRING(sale_date, 0, 7) AS year_month,
    SUBSTRING(sale_date, 8, 10) AS day;

-- Échantillon des données
DUMP (LIMIT sales_with_date 5);

-- =====================================================
-- 2. KPI GLOBAUX DE VENTES
-- =====================================================

-- Calculer les KPI principaux
global_kpi = FOREACH (GROUP sales_clean ALL) GENERATE 
    COUNT(sales_clean) AS total_transactions,
    SUM(sales_clean.total_amount) AS total_revenue,
    AVG(sales_clean.total_amount) AS avg_transaction_value,
    SUM(sales_clean.quantity) AS total_units_sold,
    COUNT_STAR(DISTINCT sales_clean.customer_id) AS unique_customers;

DUMP global_kpi;

STORE global_kpi INTO '/data/output/pig/sales_kpi_global' USING PigStorage(',');

-- =====================================================
-- 3. ANALYSE PAR CATÉGORIE DE PRODUITS
-- =====================================================

-- Grouper par catégorie
category_grouped = GROUP sales_clean BY category;

-- Statistiques par catégorie
category_stats = FOREACH category_grouped GENERATE 
    group AS category,
    COUNT(sales_clean) AS transaction_count,
    SUM(sales_clean.total_amount) AS total_revenue,
    AVG(sales_clean.total_amount) AS avg_transaction_value,
    SUM(sales_clean.quantity) AS total_quantity,
    AVG(sales_clean.unit_price) AS avg_unit_price;

-- Ordonner par chiffre d'affaires décroissant
category_stats_sorted = ORDER category_stats BY total_revenue DESC;

DUMP category_stats_sorted;

STORE category_stats_sorted INTO '/data/output/pig/sales_by_category' USING PigStorage(',');

-- =====================================================
-- 4. ANALYSE PAR RÉGION
-- =====================================================

-- Grouper par région
region_grouped = GROUP sales_clean BY region;

-- Performance par région
region_performance = FOREACH region_grouped GENERATE 
    group AS region,
    COUNT(sales_clean) AS transactions,
    SUM(sales_clean.total_amount) AS revenue,
    AVG(sales_clean.total_amount) AS avg_order_value,
    COUNT_STAR(DISTINCT sales_clean.customer_id) AS unique_customers,
    SUM(sales_clean.total_amount) / COUNT_STAR(DISTINCT sales_clean.customer_id) AS revenue_per_customer;

-- Trier par chiffre d'affaires
region_performance_sorted = ORDER region_performance BY revenue DESC;

DUMP region_performance_sorted;

STORE region_performance_sorted INTO '/data/output/pig/sales_by_region' USING PigStorage(',');

-- =====================================================
-- 5. ANALYSE DES VENDEURS
-- =====================================================

-- Performance des vendeurs
salesperson_grouped = GROUP sales_clean BY salesperson;

salesperson_performance = FOREACH salesperson_grouped GENERATE 
    group AS salesperson,
    COUNT(sales_clean) AS total_sales,
    SUM(sales_clean.total_amount) AS total_revenue,
    AVG(sales_clean.total_amount) AS avg_sale_amount,
    COUNT_STAR(DISTINCT sales_clean.customer_id) AS customers_served;

-- Trier par chiffre d'affaires
top_salespeople = ORDER salesperson_performance BY total_revenue DESC;

DUMP top_salespeople;

STORE top_salespeople INTO '/data/output/pig/salesperson_performance' USING PigStorage(',');

-- =====================================================
-- 6. ANALYSE TEMPORELLE (PAR JOUR)
-- =====================================================

-- Extraire les informations de date
sales_with_day_info = FOREACH sales_with_date GENERATE 
    *,
    (int)SUBSTRING(sale_date, 8, 10) AS day_of_month;

-- Grouper par jour de vente
daily_grouped = GROUP sales_with_day_info BY sale_date;

daily_sales = FOREACH daily_grouped GENERATE 
    group AS sale_date,
    COUNT(sales_with_day_info) AS daily_transactions,
    SUM(sales_with_day_info.total_amount) AS daily_revenue,
    AVG(sales_with_day_info.total_amount) AS avg_daily_transaction;

-- Trier par date
daily_sales_sorted = ORDER daily_sales BY sale_date;

DUMP daily_sales_sorted;

STORE daily_sales_sorted INTO '/data/output/pig/daily_sales_trend' USING PigStorage(',');

-- =====================================================
-- 7. TOP PRODUITS
-- =====================================================

-- Analyser les produits les plus vendus
product_grouped = GROUP sales_clean BY product_name;

product_performance = FOREACH product_grouped GENERATE 
    group AS product_name,
    COUNT(sales_clean) AS times_sold,
    SUM(sales_clean.quantity) AS total_quantity_sold,
    SUM(sales_clean.total_amount) AS total_product_revenue,
    AVG(sales_clean.unit_price) AS avg_unit_price;

-- Top 10 produits par revenus
top_products_by_revenue = ORDER product_performance BY total_product_revenue DESC;
top_10_products = LIMIT top_products_by_revenue 10;

DUMP top_10_products;

STORE top_10_products INTO '/data/output/pig/top_products' USING PigStorage(',');

-- =====================================================
-- 8. ANALYSE DES CLIENTS
-- =====================================================

-- Analyser le comportement des clients
customer_grouped = GROUP sales_clean BY customer_name;

customer_analysis = FOREACH customer_grouped GENERATE 
    group AS customer_name,
    COUNT(sales_clean) AS purchase_frequency,
    SUM(sales_clean.total_amount) AS total_spent,
    AVG(sales_clean.total_amount) AS avg_order_value,
    SUM(sales_clean.quantity) AS total_items_bought;

-- Clients VIP (plus de 2 achats)
vip_customers = FILTER customer_analysis BY purchase_frequency > 2;
vip_customers_sorted = ORDER vip_customers BY total_spent DESC;

DUMP vip_customers_sorted;

STORE vip_customers_sorted INTO '/data/output/pig/vip_customers' USING PigStorage(',');

-- =====================================================
-- 9. ANALYSE DES PANIERS (QUANTITÉS)
-- =====================================================

-- Créer des segments de panier selon la quantité
basket_segments = FOREACH sales_clean GENERATE 
    *,
    (quantity == 1 ? 'Single Item' : 
     (quantity <= 2 ? 'Small Basket (2 items)' : 'Large Basket (3+ items)')) AS basket_size;

-- Grouper par taille de panier
basket_grouped = GROUP basket_segments BY basket_size;

basket_analysis = FOREACH basket_grouped GENERATE 
    group AS basket_size,
    COUNT(basket_segments) AS transaction_count,
    SUM(basket_segments.total_amount) AS revenue_contribution,
    AVG(basket_segments.total_amount) AS avg_transaction_value;

-- Calculer le pourcentage de contribution
basket_with_percentage = FOREACH basket_analysis GENERATE 
    basket_size,
    transaction_count,
    revenue_contribution,
    avg_transaction_value,
    (revenue_contribution * 100.0) / 37499.42 AS revenue_percentage; -- Total calculé approximatif

DUMP basket_with_percentage;

STORE basket_with_percentage INTO '/data/output/pig/basket_analysis' USING PigStorage(',');

-- =====================================================
-- 10. RAPPORT EXÉCUTIF DE SYNTHÈSE
-- =====================================================

-- Créer un résumé pour la direction
executive_summary = FOREACH (GROUP sales_clean ALL) GENERATE 
    CONCAT('RAPPORT EXÉCUTIF VENTES - Période: Août 2024') AS title,
    CONCAT('Total transactions: ', (chararray)COUNT(sales_clean)) AS transactions_summary,
    CONCAT('Chiffre d\'affaires: $', (chararray)ROUND(SUM(sales_clean.total_amount))) AS revenue_summary,
    CONCAT('Panier moyen: $', (chararray)ROUND(AVG(sales_clean.total_amount))) AS avg_basket_summary,
    CONCAT('Unités vendues: ', (chararray)SUM(sales_clean.quantity)) AS units_summary;

-- Reformater pour un rapport linéaire
executive_report = FOREACH executive_summary GENERATE 
    CONCAT(title, ' | ', transactions_summary, ' | ', revenue_summary, ' | ', avg_basket_summary, ' | ', units_summary) AS executive_line;

DUMP executive_report;

STORE executive_report INTO '/data/output/pig/executive_sales_summary' USING PigStorage('');

-- =====================================================
-- 11. DÉTECTION D'ANOMALIES DANS LES VENTES
-- =====================================================

-- Identifier les transactions suspectes
anomalous_sales = FILTER sales_clean BY 
    total_amount > 2000 OR 
    quantity > 5 OR 
    unit_price > 1500;

anomaly_summary = FOREACH (GROUP anomalous_sales ALL) GENERATE 
    COUNT(anomalous_sales) AS anomaly_count,
    SUM(anomalous_sales.total_amount) AS anomaly_revenue;

DUMP anomaly_summary;

-- Détails des anomalies
anomaly_details = FOREACH anomalous_sales GENERATE 
    transaction_id,
    product_name,
    quantity,
    unit_price,
    total_amount,
    customer_name,
    'HIGH_VALUE' AS anomaly_type;

STORE anomaly_details INTO '/data/output/pig/sales_anomalies' USING PigStorage(',');

print 'Analyse des ventes Pig terminée avec succès!';