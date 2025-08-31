#!/usr/bin/env python3
"""
====================================================
CONNECTEUR MONGODB POUR APACHE SPARK
====================================================
Script pour lire/écrire des données depuis/vers MongoDB
Objectif: Intégration MongoDB-Spark pour analyse Big Data
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from pymongo import MongoClient
import json
from datetime import datetime, timedelta

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Créer une session Spark avec le connecteur MongoDB
    """
    logger.info("Création de la session Spark avec connecteur MongoDB...")
    return SparkSession.builder \
        .appName("BigData_MongoDB_Integration") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.input.uri", "mongodb://admin:bigdata2025@mongodb:27017/bigdata.sales") \
        .config("spark.mongodb.output.uri", "mongodb://admin:bigdata2025@mongodb:27017/bigdata.results") \
        .getOrCreate()

def setup_mongodb_data():
    """
    Initialiser MongoDB avec des données d'exemple
    """
    logger.info("Initialisation des données MongoDB...")
    
    try:
        # Connexion à MongoDB
        client = MongoClient("mongodb://admin:bigdata2025@mongodb:27017/")
        db = client["bigdata"]
        
        # Collection sales - données transactionnelles
        sales_collection = db["sales"]
        
        # Supprimer les données existantes
        sales_collection.drop()
        
        # Données d'exemple pour MongoDB (format JSON)
        sample_sales = [
            {
                "_id": "MONGO_001",
                "transaction_id": "M001",
                "product": {
                    "id": "P105",
                    "name": "Smart Watch",
                    "category": "Electronics",
                    "brand": "TechCorp"
                },
                "sale_info": {
                    "quantity": 1,
                    "unit_price": 299.99,
                    "total_amount": 299.99,
                    "discount": 0.0,
                    "tax": 24.0
                },
                "customer": {
                    "id": "C101",
                    "name": "Maria Garcia",
                    "email": "maria@email.com",
                    "loyalty_tier": "Gold"
                },
                "sale_date": datetime(2024, 8, 26),
                "location": {
                    "region": "Europe",
                    "country": "Spain",
                    "city": "Madrid"
                },
                "sales_rep": "Isabella Rodriguez",
                "payment_method": "Credit Card",
                "status": "completed",
                "metadata": {
                    "source": "online",
                    "campaign": "summer_sale",
                    "created_at": datetime.now()
                }
            },
            {
                "_id": "MONGO_002",
                "transaction_id": "M002",
                "product": {
                    "id": "P220",
                    "name": "Laptop",
                    "category": "Electronics",
                    "brand": "Lenovo"
                },
                "sale_info": {
                    "quantity": 2,
                    "unit_price": 899.50,
                    "total_amount": 1799.00,
                    "discount": 50.0,
                    "tax": 144.0
                },
                "customer": {
                    "id": "C102",
                    "name": "John Smith",
                    "email": "john@email.com",
                    "loyalty_tier": "Silver"
                },
                "sale_date": datetime(2024, 9, 1),
                "location": {
                    "region": "North America",
                    "country": "USA",
                    "city": "New York"
                },
                "sales_rep": "David Johnson",
                "payment_method": "PayPal",
                "status": "completed",
                "metadata": {
                    "source": "store",
                    "campaign": "back_to_school",
                    "created_at": datetime.now()
                }
            }
        ]
        
        sales_collection.insert_many(sample_sales)
        logger.info("Données insérées dans MongoDB avec succès.")
    
    except Exception as e:
        logger.error(f"Erreur lors de l’initialisation de MongoDB: {e}")

def analyse_with_spark(spark):
    """
    Lire les données depuis MongoDB avec Spark et effectuer une analyse simple
    """
    logger.info("Lecture des données depuis MongoDB avec Spark...")
    df = spark.read.format("mongo").load()
    
    logger.info("Affichage du schéma des données :")
    df.printSchema()
    
    logger.info("Aperçu des données :")
    df.show(5, truncate=False)
    
    # Exemple d'analyse : ventes par catégorie de produit
    results = df.groupBy("product.category").agg(
        count("*").alias("total_transactions"),
        sum("sale_info.total_amount").alias("total_sales")
    )
    
    logger.info("Résultats de l'analyse (ventes par catégorie) :")
    results.show()
    
    # Sauvegarder les résultats dans MongoDB
    results.write.format("mongo").mode("overwrite").save()
    logger.info("Résultats sauvegardés dans MongoDB (collection results).")

if __name__ == "__main__":
    # Initialiser MongoDB avec des données factices
    setup_mongodb_data()
    
    # Créer la session Spark
    spark = create_spark_session()
    
    # Lancer l'analyse
    analyse_with_spark(spark)
    
    # Arrêter Spark
    spark.stop()
    logger.info("Fin du script.")
