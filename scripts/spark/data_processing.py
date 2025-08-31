#!/usr/bin/env python3
"""
====================================================
TRAITEMENT DE DONNÉES AVEC APACHE SPARK
====================================================
Script PySpark pour traitement avancé des données
Objectif: ETL, transformations et analyses complexes
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Créer une session Spark configurée pour notre cluster
    """
    return SparkSession.builder \
        .appName("BigData_DataProcessing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def load_employee_data(spark):
    """
    Charger et nettoyer les données des employés
    """
    logger.info("Chargement des données employés...")
    
    # Schéma explicite pour de meilleures performances
    employee_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True),
        StructField("hire_date", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(employee_schema) \
        .csv("hdfs://hadoop-master:9000/data/input/sample_data.csv")
    
    # Nettoyage des données
    df_clean = df.filter(col("id").isNotNull() & (col("id") > 0))
    
    logger.info(f"Données chargées: {df_clean.count()} employés")
    return df_clean

def load_sales_data(spark):
    """
    Charger et nettoyer les données de ventes
    """
    logger.info("Chargement des données de ventes...")
    
    # Schéma pour les ventes
    sales_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("sale_date", DateType(), True),
        StructField("region", StringType(), True),
        StructField("salesperson", StringType(), True)
    ])
    
    df = spark.read \
        .option("header", "true") \
        .option("dateFormat", "yyyy-MM-dd") \
        .schema(sales_schema) \
        .csv("hdfs://hadoop-master:9000/data/input/sales_data.csv")
    
    # Ajouter des colonnes calculées
    df_enriched = df.withColumn("year_month", date_format(col("sale_date"), "yyyy-MM")) \
        .withColumn("day_of_week", date_format(col("sale_date"), "EEEE")) \
        .withColumn("profit_margin", col("total_amount") * 0.3) \
        .withColumn("revenue_per_unit", col("total_amount") / col("quantity"))
    
    logger.info(f"Données de ventes chargées: {df_enriched.count()} transactions")
    return df_enriched

def analyze_employee_performance(df_employees):
    """
    Analyses avancées des performances des employés
    """
    logger.info("Analyse des performances employés...")
    
    # 1. Statistiques par département avec window functions
    window_dept = Window.partitionBy("department")
    
    employee_analysis = df_employees.withColumn("dept_avg_salary", avg("salary").over(window_dept)) \
        .withColumn("salary_rank", rank().over(Window.partitionBy("department").orderBy(desc("salary")))) \
        .withColumn("salary_percentile", percent_rank().over(Window.partitionBy("department").orderBy("salary"))) \
        .withColumn("years_service", datediff(current_date(), to_date(col("hire_date"), "yyyy-MM-dd")) / 365.25) \
        .withColumn("salary_above_dept_avg", col("salary") > col("dept_avg_salary"))
    
    # 2. Créer des segments de performance
    performance_segments = employee_analysis.withColumn("performance_segment",
        when((col("salary_percentile") >= 0.8) & (col("years_service") >= 3), "High Performer")
        .when((col("salary_percentile") >= 0.6) & (col("years_service") >= 2), "Good Performer")
        .when(col("salary_percentile") >= 0.4, "Average Performer")
        .otherwise("Developing"))
    
    # 3. Analyse des tendances salariales
    salary_trends = performance_segments.groupBy("department", "performance_segment") \
        .agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary"),
            stddev("salary").alias("salary_std"),
            avg("years_service").alias("avg_tenure"),
            sum(when(col("status") == "Active", 1).otherwise(0)).alias("active_employees")
        )
    
    # Sauvegarder les résultats
    salary_trends.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/data/output/spark/employee_performance")
    
    # 4. Top performers par département
    top_performers = performance_segments.filter(col("performance_segment") == "High Performer") \
        .select("name", "department", "salary", "years_service", "salary_rank")
    
    top_performers.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/data/output/spark/top_performers")
    
    return performance_segments, salary_trends

def analyze_sales_advanced(df_sales):
    """
    Analyses avancées des ventes avec Spark SQL
    """
    logger.info("Analyses avancées des ventes...")
    
    # Créer une vue temporaire pour SQL
    df_sales.createOrReplaceTempView("sales")
    
    # 1. Analyse RFM (Recency, Frequency, Monetary)
    rfm_analysis = df_sales.groupBy("customer_name") \
        .agg(
            max("sale_date").alias("last_purchase_date"),
            count("transaction_id").alias("frequency"),
            sum("total_amount").alias("monetary_value"),
            avg("total_amount").alias("avg_order_value")
        ) \
        .withColumn("recency_days", datediff(current_date(), col("last_purchase_date"))) \
        .withColumn("rfm_score", 
            (when(col("recency_days") <= 7, 3)
             .when(col("recency_days") <= 14, 2)
             .otherwise(1)) +
            (when(col("frequency") >= 3, 3)
             .when(col("frequency") >= 2, 2)
             .otherwise(1)) +
            (when(col("monetary_value") >= 1000, 3)
             .when(col("monetary_value") >= 500, 2)
             .otherwise(1))
        ) \
        .withColumn("customer_segment",
            when(col("rfm_score") >= 8, "Champions")
            .when(col("rfm_score") >= 6, "Loyal Customers")
            .when(col("rfm_score") >= 4, "Potential Loyalists")
            .otherwise("New Customers"))
    
    # 2. Analyse des tendances avec window functions
    sales_with_trends = df_sales.withColumn("running_total",
        sum("total_amount").over(Window.orderBy("sale_date"))) \
        .withColumn("daily_rank",
        rank().over(Window.partitionBy("sale_date").orderBy(desc("total_amount"))))
    
    # 3. Cohort Analysis (analyse de cohortes)
    cohort_data = df_sales.withColumn("customer_cohort", 
        date_format(first("sale_date").over(Window.partitionBy("customer_name").orderBy("sale_date")), "yyyy-MM")) \
        .withColumn("period_number",
        months_between(col("sale_date"), 
            first("sale_date").over(Window.partitionBy("customer_name").orderBy("sale_date"))))
    
    # 4. Market Basket Analysis (produits souvent achetés ensemble)
    # Simplifiée car nous n'avons qu'un produit par transaction
    product_affinity = df_sales.groupBy("customer_name") \
        .agg(collect_list("product_name").alias("products_bought")) \
        .withColumn("product_count", size(col("products_bought")))
    
    # 5. Analyse géographique avancée
    geographic_analysis = df_sales.groupBy("region", "category") \
        .agg(
            sum("total_amount").alias("revenue"),
            count("*").alias("transactions"),
            countDistinct("customer_name").alias("unique_customers"),
            avg("total_amount").alias("avg_transaction")
        ) \
        .withColumn("market_share", 
            col("revenue") / sum("revenue").over(Window.partitionBy("region"))) \
        .withColumn("region_rank", 
            rank().over(Window.partitionBy("category").orderBy(desc("revenue"))))
    
    # Sauvegarder les analyses
    rfm_analysis.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/data/output/spark/customer_rfm_analysis")
    
    geographic_analysis.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/data/output/spark/geographic_analysis")
    
    cohort_data.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/data/output/spark/cohort_analysis")
    
    return rfm_analysis, geographic_analysis

def create_summary_report(spark, df_employees, df_sales):
    """
    Créer un rapport de synthèse consolidé
    """
    logger.info("Création du rapport de synthèse...")
    
    # Métriques employés
    emp_metrics = df_employees.agg(
        count("*").alias("total_employees"),
        avg("salary").alias("avg_salary"),
        countDistinct("department").alias("departments"),
        countDistinct("city").alias("cities")
    ).collect()[0]
    
    # Métriques ventes
    sales_metrics = df_sales.agg(
        count("*").alias("total_transactions"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_transaction"),
        countDistinct("customer_name").alias("unique_customers"),
        countDistinct("product_name").alias("unique_products")
    ).collect()[0]
    
    # Créer le rapport
    report_data = [
        ("Metric", "Value", "Type"),
        ("Total Employees", emp_metrics["total_employees"], "HR"),
        ("Average Salary", f"${emp_metrics['avg_salary']:.2f}", "HR"),
        ("Departments", emp_metrics["departments"], "HR"),
        ("Cities", emp_metrics["cities"], "HR"),
        ("Total Transactions", sales_metrics["total_transactions"], "Sales"),
        ("Total Revenue", f"${sales_metrics['total_revenue']:.2f}", "Sales"),
        ("Average Transaction", f"${sales_metrics['avg_transaction']:.2f}", "Sales"),
        ("Unique Customers", sales_metrics["unique_customers"], "Sales"),
        ("Unique Products", sales_metrics["unique_products"], "Sales")
    ]
    
    report_df = spark.createDataFrame(report_data, ["metric", "value", "type"])
    
    report_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/data/output/spark/executive_summary")
    
    return report_df

def main():
    """
    Fonction principale d'orchestration
    """
    logger.info("=== DÉMARRAGE DU TRAITEMENT SPARK ===")
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Charger les données
        df_employees = load_employee_data(spark)
        df_sales = load_sales_data(spark)
        
        # Afficher des statistiques de base
        logger.info(f"Employés: {df_employees.count()}")
        logger.info(f"Transactions: {df_sales.count()}")
        
        # Analyses avancées
        employee_performance, salary_trends = analyze_employee_performance(df_employees)
        rfm_analysis, geographic_analysis = analyze_sales_advanced(df_sales)
        
        # Rapport de synthèse
        summary_report = create_summary_report(spark, df_employees, df_sales)
        
        # Afficher quelques résultats
        print("\n=== APERÇU DES RÉSULTATS ===")
        print("Top 5 performeurs:")
        salary_trends.orderBy(desc("avg_salary")).show(5)
        
        print("Segments clients RFM:")
        rfm_analysis.groupBy("customer_segment").count().show()
        
        print("Résumé exécutif:")
        summary_report.show()
        
        logger.info("=== TRAITEMENT TERMINÉ AVEC SUCCÈS ===")
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()