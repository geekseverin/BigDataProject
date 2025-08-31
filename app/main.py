#!/usr/bin/env python3
"""
====================================================
APPLICATION WEB FLASK POUR VISUALISATION BIG DATA
====================================================
Interface web pour visualiser les résultats d'analyses
Hadoop, Spark et MongoDB
"""

import os
import json
import csv
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, flash, redirect, url_for
import pymongo
from pymongo import MongoClient
import pandas as pd
import logging
from io import StringIO
import subprocess
import requests

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration de l'application Flask
app = Flask(__name__)
app.secret_key = 'bigdata_ucao_2024_secret_key_change_in_production'

# Configuration des connexions
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:bigdata2025@mongodb:27017/bigdata?authSource=admin')
HADOOP_NAMENODE = os.getenv('HADOOP_NAMENODE', 'http://hadoop-master:9870')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')

class BigDataConnector:
    """Classe pour gérer les connexions aux différents services Big Data"""
    
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self._connect_mongodb()
    
    def _connect_mongodb(self):
        """Connexion à MongoDB"""
        try:
            self.mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            self.mongo_db = self.mongo_client['bigdata']
            # Test de connexion
            self.mongo_client.admin.command('ping')
            logger.info("✅ Connexion MongoDB établie")
        except Exception as e:
            logger.error(f"❌ Erreur connexion MongoDB: {str(e)}")
            self.mongo_client = None
            self.mongo_db = None
    
    def get_mongodb_data(self, collection_name, query=None, limit=100):
        """Récupérer des données depuis MongoDB"""
        if not self.mongo_db:
            return []
        
        try:
            collection = self.mongo_db[collection_name]
            if query:
                cursor = collection.find(query).limit(limit)
            else:
                cursor = collection.find().limit(limit)
            
            # Convertir ObjectId en string pour JSON
            data = []
            for doc in cursor:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                data.append(doc)
            
            return data
        except Exception as e:
            logger.error(f"Erreur récupération données MongoDB: {str(e)}")
            return []
    
    def get_hadoop_info(self):
        """Récupérer des informations depuis Hadoop NameNode"""
        try:
            # Informations de base du cluster
            response = requests.get(f"{HADOOP_NAMENODE}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem", timeout=10)
            if response.status_code == 200:
                data = response.json()
                beans = data.get('beans', [])
                if beans:
                    fs_info = beans[0]
                    return {
                        'total_capacity': fs_info.get('CapacityTotal', 0),
                        'used_capacity': fs_info.get('CapacityUsed', 0),
                        'available_capacity': fs_info.get('CapacityRemaining', 0),
                        'total_files': fs_info.get('FilesTotal', 0),
                        'total_blocks': fs_info.get('BlocksTotal', 0),
                        'live_nodes': fs_info.get('NumLiveDataNodes', 0),
                        'dead_nodes': fs_info.get('NumDeadDataNodes', 0)
                    }
        except Exception as e:
            logger.error(f"Erreur récupération info Hadoop: {str(e)}")
        
        return {}
    
    def get_spark_info(self):
        """Récupérer des informations depuis Spark Master"""
        try:
            # Utiliser l'API REST de Spark
            response = requests.get("http://spark-master:8080/api/v1/applications", timeout=10)
            if response.status_code == 200:
                apps = response.json()
                return {
                    'active_applications': len([app for app in apps if app.get('attempts', [{}])[-1].get('completed', True) == False]),
                    'completed_applications': len([app for app in apps if app.get('attempts', [{}])[-1].get('completed', True) == True]),
                    'total_applications': len(apps),
                    'applications': apps[:5]  # Les 5 dernières
                }
        except Exception as e:
            logger.error(f"Erreur récupération info Spark: {str(e)}")
        
        return {}
    
    def read_hdfs_results(self, path_pattern):
        """Lire les résultats depuis HDFS (simulation via requêtes HTTP)"""
        # Dans un environnement réel, on utiliserait hdfs3 ou pydoop
        # Ici on simule avec des données d'exemple
        sample_data = []
        
        if 'pig' in path_pattern:
            sample_data = [
                {'department': 'Engineering', 'employee_count': 15, 'avg_salary': 85000},
                {'department': 'Marketing', 'employee_count': 12, 'avg_salary': 72000},
                {'department': 'Finance', 'employee_count': 10, 'avg_salary': 68000},
                {'department': 'HR', 'employee_count': 8, 'avg_salary': 65000}
            ]
        elif 'spark' in path_pattern:
            sample_data = [
                {'region': 'North America', 'revenue': 125000, 'transactions': 45},
                {'region': 'Europe', 'revenue': 98000, 'transactions': 38},
                {'region': 'Asia', 'revenue': 87000, 'transactions': 32},
                {'region': 'South America', 'revenue': 23000, 'transactions': 8}
            ]
        
        return sample_data

# Initialiser le connecteur
db_connector = BigDataConnector()

@app.route('/')
def index():
    """Page d'accueil avec tableau de bord"""
    try:
        # Récupérer les statistiques générales
        mongodb_stats = {
            'sales_count': len(db_connector.get_mongodb_data('sales')),
            'customers_count': len(db_connector.get_mongodb_data('customers'))
        }
        
        hadoop_stats = db_connector.get_hadoop_info()
        spark_stats = db_connector.get_spark_info()
        
        # Données pour les graphiques
        sales_data = db_connector.get_mongodb_data('sales', limit=50)
        
        return render_template('index.html', 
                             mongodb_stats=mongodb_stats,
                             hadoop_stats=hadoop_stats,
                             spark_stats=spark_stats,
                             sales_data=sales_data)
    
    except Exception as e:
        logger.error(f"Erreur page d'accueil: {str(e)}")
        flash(f"Erreur lors du chargement: {str(e)}", 'error')
        return render_template('index.html', 
                             mongodb_stats={},
                             hadoop_stats={},
                             spark_stats={},
                             sales_data=[])

@app.route('/dashboard')
def dashboard():
    """Page de tableau de bord avancé"""
    try:
        # Analyses Pig (simulation)
        pig_results = {
            'department_stats': db_connector.read_hdfs_results('/data/output/pig/department_statistics'),
            'city_analysis': db_connector.read_hdfs_results('/data/output/pig/city_analysis'),
            'salary_trends': db_connector.read_hdfs_results('/data/output/pig/salary_bracket_analysis')
        }
        
        # Analyses Spark (simulation)
        spark_results = {
            'regional_analysis': db_connector.read_hdfs_results('/data/output/spark/geographic_analysis'),
            'customer_segments': db_connector.read_hdfs_results('/data/output/spark/customer_rfm_analysis'),
            'performance_metrics': db_connector.read_hdfs_results('/data/output/spark/employee_performance')
        }
        
        # Données MongoDB
        mongodb_data = {
            'recent_sales': db_connector.get_mongodb_data('sales', query={'sale_date': {'$gte': datetime.now() - timedelta(days=30)}}, limit=20),
            'top_customers': db_connector.get_mongodb_data('customers', limit=10)
        }
        
        return render_template('dashboard.html',
                             pig_results=pig_results,
                             spark_results=spark_results,
                             mongodb_data=mongodb_data)
    
    except Exception as e:
        logger.error(f"Erreur tableau de bord: {str(e)}")
        flash(f"Erreur lors du chargement du tableau de bord: {str(e)}", 'error')
        return render_template('dashboard.html',
                             pig_results={},
                             spark_results={},
                             mongodb_data={})

@app.route('/api/mongodb/collections')
def api_mongodb_collections():
    """API pour récupérer les collections MongoDB"""
    try:
        if not db_connector.mongo_db:
            return jsonify({'error': 'MongoDB non disponible'}), 503
        
        collections = db_connector.mongo_db.list_collection_names()
        result = {}
        
        for collection_name in collections:
            count = db_connector.mongo_db[collection_name].count_documents({})
            result[collection_name] = {
                'count': count,
                'sample': db_connector.get_mongodb_data(collection_name, limit=3)
            }
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sales/by-region')
def api_sales_by_region():
    """API pour les ventes par région"""
    try:
        sales_data = db_connector.get_mongodb_data('sales')
        
        # Agrégation par région
        region_stats = {}
        for sale in sales_data:
            region = sale.get('location', {}).get('region', 'Unknown')
            amount = sale.get('sale_info', {}).get('total_amount', 0)
            
            if region not in region_stats:
                region_stats[region] = {'total': 0, 'count': 0}
            
            region_stats[region]['total'] += amount
            region_stats[region]['count'] += 1
        
        # Formater pour les graphiques
        result = []
        for region, stats in region_stats.items():
            result.append({
                'region': region,
                'total_sales': stats['total'],
                'transactions': stats['count'],
                'avg_transaction': stats['total'] / stats['count'] if stats['count'] > 0 else 0
            })
        
        return jsonify(sorted(result, key=lambda x: x['total_sales'], reverse=True))
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sales/by-category')
def api_sales_by_category():
    """API pour les ventes par catégorie"""
    try:
        sales_data = db_connector.get_mongodb_data('sales')
        
        # Agrégation par catégorie
        category_stats = {}
        for sale in sales_data:
            category = sale.get('product', {}).get('category', 'Unknown')
            amount = sale.get('sale_info', {}).get('total_amount', 0)
            quantity = sale.get('sale_info', {}).get('quantity', 0)
            
            if category not in category_stats:
                category_stats[category] = {'total': 0, 'count': 0, 'quantity': 0}
            
            category_stats[category]['total'] += amount
            category_stats[category]['count'] += 1
            category_stats[category]['quantity'] += quantity
        
        # Formater pour les graphiques
        result = []
        for category, stats in category_stats.items():
            result.append({
                'category': category,
                'total_sales': stats['total'],
                'transactions': stats['count'],
                'total_quantity': stats['quantity'],
                'avg_price': stats['total'] / stats['quantity'] if stats['quantity'] > 0 else 0
            })
        
        return jsonify(sorted(result, key=lambda x: x['total_sales'], reverse=True))
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/status')
def api_system_status():
    """API pour le statut du système"""
    try:
        status = {
            'mongodb': {
                'status': 'connected' if db_connector.mongo_db else 'disconnected',
                'collections': db_connector.mongo_db.list_collection_names() if db_connector.mongo_db else []
            },
            'hadoop': {
                'status': 'unknown',
                'info': db_connector.get_hadoop_info()
            },
            'spark': {
                'status': 'unknown', 
                'info': db_connector.get_spark_info()
            }
        }
        
        # Tester la connectivité Hadoop
        try:
            response = requests.get(f"{HADOOP_NAMENODE}/dfshealth.html", timeout=5)
            status['hadoop']['status'] = 'connected' if response.status_code == 200 else 'disconnected'
        except:
            status['hadoop']['status'] = 'disconnected'
        
        # Tester la connectivité Spark
        try:
            response = requests.get("http://spark-master:8080", timeout=5)
            status['spark']['status'] = 'connected' if response.status_code == 200 else 'disconnected'
        except:
            status['spark']['status'] = 'disconnected'
        
        return jsonify(status)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/pig-results')
def api_pig_results():
    """API pour récupérer les résultats des analyses Pig"""
    try:
        results = {
            'department_analysis': [
                {'department': 'Engineering', 'employees': 15, 'avg_salary': 85000, 'total_salary': 1275000},
                {'department': 'Marketing', 'employees': 12, 'avg_salary': 72000, 'total_salary': 864000},
                {'department': 'Finance', 'employees': 10, 'avg_salary': 68000, 'total_salary': 680000},
                {'department': 'HR', 'employees': 8, 'avg_salary': 65000, 'total_salary': 520000}
            ],
            'age_group_analysis': [
                {'age_group': 'Young (< 30)', 'count': 18, 'avg_salary': 67000},
                {'age_group': 'Middle (30-39)', 'count': 22, 'avg_salary': 78000},
                {'age_group': 'Senior (40+)', 'count': 10, 'avg_salary': 89000}
            ],
            'city_analysis': [
                {'city': 'New York', 'employees': 3, 'avg_salary': 82000},
                {'city': 'London', 'employees': 2, 'avg_salary': 78500},
                {'city': 'Paris', 'employees': 2, 'avg_salary': 71000},
                {'city': 'Tokyo', 'employees': 2, 'avg_salary': 85000}
            ]
        }
        
        return jsonify(results)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/spark-results')
def api_spark_results():
    """API pour récupérer les résultats des analyses Spark"""
    try:
        results = {
            'customer_segments': [
                {'segment': 'Champions', 'count': 8, 'avg_value': 1200, 'total_revenue': 9600},
                {'segment': 'Loyal Customers', 'count': 15, 'avg_value': 800, 'total_revenue': 12000},
                {'segment': 'Potential Loyalists', 'count': 12, 'avg_value': 450, 'total_revenue': 5400},
                {'segment': 'New Customers', 'count': 20, 'avg_value': 200, 'total_revenue': 4000}
            ],
            'regional_performance': [
                {'region': 'North America', 'revenue': 125000, 'customers': 25, 'avg_order': 280},
                {'region': 'Europe', 'revenue': 98000, 'customers': 18, 'avg_order': 320},
                {'region': 'Asia', 'revenue': 87000, 'customers': 20, 'avg_order': 250},
                {'region': 'South America', 'revenue': 23000, 'customers': 8, 'avg_order': 190}
            ],
            'product_performance': [
                {'product': 'Laptop Pro', 'sales': 45000, 'units': 35, 'margin': 30},
                {'product': 'Smartphone X', 'sales': 38000, 'units': 42, 'margin': 25},
                {'product': 'Tablet Plus', 'sales': 28000, 'units': 56, 'margin': 22},
                {'product': 'Smart Watch', 'sales': 15000, 'units': 50, 'margin': 35}
            ]
        }
        
        return jsonify(results)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/csv/<data_type>')
def export_csv(data_type):
    """Exporter les données en CSV"""
    try:
        output = StringIO()
        writer = csv.writer(output)
        
        if data_type == 'sales':
            sales_data = db_connector.get_mongodb_data('sales')
            if sales_data:
                # En-têtes
                writer.writerow(['Transaction ID', 'Product', 'Category', 'Amount', 'Region', 'Date'])
                
                # Données
                for sale in sales_data:
                    writer.writerow([
                        sale.get('transaction_id', ''),
                        sale.get('product', {}).get('name', ''),
                        sale.get('product', {}).get('category', ''),
                        sale.get('sale_info', {}).get('total_amount', 0),
                        sale.get('location', {}).get('region', ''),
                        sale.get('sale_date', '')
                    ])
        
        elif data_type == 'departments':
            # Données d'exemple pour les départements
            writer.writerow(['Department', 'Employees', 'Avg Salary', 'Total Salary'])
            dept_data = [
                ['Engineering', 15, 85000, 1275000],
                ['Marketing', 12, 72000, 864000],
                ['Finance', 10, 68000, 680000],
                ['HR', 8, 65000, 520000]
            ]
            writer.writerows(dept_data)
        
        # Créer la réponse
        output.seek(0)
        from flask import Response
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={data_type}_export.csv'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def page_not_found(e):
    """Gestionnaire d'erreur 404"""
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    """Gestionnaire d'erreur 500"""
    logger.error(f"Erreur serveur: {str(e)}")
    return render_template('500.html'), 500

@app.context_processor
def utility_processor():
    """Processeur de contexte pour les utilitaires template"""
    def format_currency(amount):
        """Formater les montants en devise"""
        return f"${amount:,.2f}"
    
    def format_number(number):
        """Formater les nombres"""
        return f"{number:,}"
    
    def format_bytes(bytes_size):
        """Formater la taille en bytes"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.2f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.2f} PB"
    
    return dict(
        format_currency=format_currency,
        format_number=format_number,
        format_bytes=format_bytes
    )

if __name__ == '__main__':
    # Configuration pour développement
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True,
        threaded=True
    )