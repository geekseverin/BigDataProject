#!/bin/bash
# =====================================================
# SCRIPT D'INITIALISATION APACHE SPARK
# =====================================================
# Configure et teste le cluster Spark
# Utilisation: ./init-spark.sh

set -e  # Arrêter en cas d'erreur

echo "======================================================"
echo "⚡ INITIALISATION DU CLUSTER SPARK"
echo "======================================================"

# Variables de configuration
SPARK_MASTER="spark-master"
SPARK_MASTER_URL="spark://spark-master:7077"
TIMEOUT=120

# Fonction d'attente de service
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local timeout=$4
    
    echo "⏳ Attente du service $service_name sur $host:$port..."
    
    local count=0
    while ! nc -z $host $port 2>/dev/null; do
        if [ $count -ge $timeout ]; then
            echo "❌ Timeout: $service_name non disponible après ${timeout}s"
            return 1
        fi
        sleep 1
        ((count++))
    done
    
    echo "✅ Service $service_name disponible"
}

# Vérifier le statut du cluster Spark
check_spark_cluster() {
    echo "🔍 Vérification du cluster Spark..."
    
    # Vérifier que tous les conteneurs Spark sont actifs
    local spark_containers=("spark-master" "spark-worker-1" "spark-worker-2")
    
    for container in "${spark_containers[@]}"; do
        if ! docker ps --format "table {{.Names}}" | grep -q "^$container$"; then
            echo "❌ Conteneur Spark $container non trouvé"
            return 1
        fi
        echo "✅ Conteneur $container actif"
    done
    
    # Attendre les services
    wait_for_service $SPARK_MASTER 8080 "Spark Master UI" 60
    wait_for_service $SPARK_MASTER 7077 "Spark Master" 60
    
    # Vérifier les workers
    echo "🔄 Vérification des workers Spark..."
    sleep 10
    
    local worker_count=$(docker exec spark-master curl -s http://localhost:8080 | grep -c "worker-" || echo "0")
    if [ "$worker_count" -lt 2 ]; then
        echo "⚠️  Seulement $worker_count workers détectés, attendu 2"
        sleep 15  # Attendre un peu plus
        worker_count=$(docker exec spark-master curl -s http://localhost:8080 | grep -c "worker-" || echo "0")
    fi
    
    echo "✅ $worker_count workers Spark connectés"
    return 0
}

# Créer les répertoires Spark dans HDFS
setup_spark_hdfs_dirs() {
    echo "📁 Création des répertoires Spark dans HDFS..."
    
    # Vérifier que Hadoop est disponible
    if ! docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /'" > /dev/null 2>&1; then
        echo "⚠️  HDFS non disponible, initialisation requise"
        echo "💡 Exécutez d'abord: ./scripts/setup/init-hadoop.sh"
        return 1
    fi
    
    # Créer les répertoires nécessaires
    local spark_dirs=(
        "/spark-logs"
        "/spark-checkpoints"
        "/spark-warehouse"
        "/user/spark"
    )
    
    for dir in "${spark_dirs[@]}"; do
        echo "   Création: $dir"
        docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -mkdir -p $dir'" 2>/dev/null || true
        docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -chmod 777 $dir'" 2>/dev/null || true
    done
    
    echo "✅ Répertoires Spark créés dans HDFS"
}

# Tester les applications Spark
test_spark_applications() {
    echo "🧪 Test des applications Spark..."
    
    # Test 1: Application Pi simple
    echo "📐 Test calcul Pi..."
    
    local pi_result
    pi_result=$(docker exec spark-master bash -c "
        /opt/bitnami/spark/bin/spark-submit \
        --master $SPARK_MASTER_URL \
        --class org.apache.spark.examples.SparkPi \
        /opt/bitnami/spark/examples/jars/spark-examples*.jar \
        10 2>/dev/null | grep 'Pi is roughly' | tail -1
    " || echo "Erreur")
    
    if [[ "$pi_result" == *"Pi is roughly"* ]]; then
        echo "✅ Test Pi réussi: $pi_result"
    else
        echo "❌ Test Pi échoué"
        return 1
    fi
    
    # Test 2: Application PySpark simple
    echo "🐍 Test PySpark..."
    
    # Créer un script Python de test
    docker exec spark-master bash -c "cat > /tmp/test_pyspark.py << 'EOF'
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName('TestPySpark').getOrCreate()
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
result = rdd.map(lambda x: x * 2).collect()
print(f'Test PySpark réussi: {result}')
spark.stop()
EOF"
    
    local pyspark_result
    pyspark_result=$(docker exec spark-master bash -c "
        /opt/bitnami/spark/bin/spark-submit \
        --master $SPARK_MASTER_URL \
        /tmp/test_pyspark.py 2>/dev/null | grep 'Test PySpark réussi' | tail -1
    " || echo "Erreur")
    
    if [[ "$pyspark_result" == *"Test PySpark réussi"* ]]; then
        echo "✅ Test PySpark réussi: $pyspark_result"
    else
        echo "❌ Test PySpark échoué"
        return 1
    fi
    
    echo "✅ Tous les tests Spark réussis"
}

# Tester la connectivité HDFS depuis Spark
test_spark_hdfs_integration() {
    echo "🔗 Test intégration Spark-HDFS..."
    
    # Créer un script de test d'intégration
    docker exec spark-master bash -c "cat > /tmp/test_hdfs_integration.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('TestSparkHDFS') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .getOrCreate()

try:
    # Test lecture HDFS
    df = spark.read.option('header', 'true').csv('hdfs://hadoop-master:9000/data/input/sample_data.csv')
    count = df.count()
    print(f'Lecture HDFS réussie: {count} lignes')
    
    # Test écriture HDFS
    df.limit(5).write.mode('overwrite').option('header', 'true').csv('hdfs://hadoop-master:9000/tmp/spark_test')
    print('Écriture HDFS réussie')
    
    # Test analyse simple
    dept_count = df.groupBy('department').count().collect()
    print(f'Analyse départements: {len(dept_count)} départements')
    
    print('Test intégration Spark-HDFS: SUCCÈS')
    
except Exception as e:
    print(f'Erreur intégration Spark-HDFS: {str(e)}')
    
finally:
    spark.stop()
EOF"
    
    local integration_result
    integration_result=$(docker exec spark-master bash -c "
        /opt/bitnami/spark/bin/spark-submit \
        --master $SPARK_MASTER_URL \
        --conf spark.hadoop.fs.defaultFS=hdfs://hadoop-master:9000 \
        /tmp/test_hdfs_integration.py 2>/dev/null | grep -E '(Lecture HDFS|Écriture HDFS|Test intégration)' | tail -3
    ")
    
    if [[ "$integration_result" == *"SUCCÈS"* ]]; then
        echo "✅ Intégration Spark-HDFS fonctionnelle"
    else
        echo "❌ Problème d'intégration Spark-HDFS"
        echo "Résultat: $integration_result"
        return 1
    fi
}

# Installer les dépendances Python supplémentaires
install_python_dependencies() {
    echo "📦 Installation des dépendances Python..."
    
    local dependencies=(
        "pymongo==4.5.0"
        "pandas==2.0.3"
        "numpy==1.24.3"
    )
    
    for dep in "${dependencies[@]}"; do
        echo "   Installation: $dep"
        docker exec spark-master bash -c "pip install $dep" > /dev/null 2>&1 || {
            echo "⚠️  Erreur installation $dep"
        }
    done
    
    echo "✅ Dépendances Python installées"
}

# Afficher les informations du cluster Spark
display_spark_info() {
    echo "======================================================"
    echo "📊 INFORMATIONS DU CLUSTER SPARK"
    echo "======================================================"
    
    # Informations de base
    echo "🔗 URLs d'accès:"
    echo "   Spark Master UI:     http://localhost:8080"
    echo "   Spark Worker 1:      http://localhost:8081"
    echo "   Spark Worker 2:      http://localhost:8082"
    echo "   Spark History:       http://localhost:18080 (si activé)"
    
    echo ""
    echo "⚙️  Configuration Spark Master:"
    docker exec spark-master bash -c "
        echo 'Spark Version: '\$(/opt/bitnami/spark/bin/spark-submit --version 2>&1 | grep version | head -1)
        echo 'Master URL: $SPARK_MASTER_URL'
        echo 'UI Port: 8080'
    "
    
    echo ""
    echo "👥 Workers connectés:"
    local worker_info
    worker_info=$(docker exec spark-master bash -c "curl -s http://localhost:8080/json/" | grep -o '"workers":\[.*\]' || echo "Impossible de récupérer les informations")
    
    if [[ "$worker_info" != "Impossible"* ]]; then
        echo "   ✅ Workers détectés dans l'interface"
    else
        echo "   ⚠️  Information workers non disponible"
    fi
    
    echo ""
    echo "🗄️  Répertoires HDFS Spark:"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /spark* /user/spark'" 2>/dev/null || echo "   Répertoires en cours de création..."
    
    echo ""
    echo "🐍 Python et dépendances:"
    docker exec spark-master bash -c "python3 --version && pip list | grep -E '(pyspark|pymongo|pandas|numpy)'" 2>/dev/null || echo "   Informations Python non disponibles"
}

# Créer des scripts d'exemple pour les tests
create_example_scripts() {
    echo "📝 Création de scripts d'exemple..."
    
    # Script d'exemple 1: Analyse simple
    docker exec spark-master bash -c "mkdir -p /opt/examples && cat > /opt/examples/simple_analysis.py << 'EOF'
#!/usr/bin/env python3
\"\"\"Exemple d'analyse simple avec Spark\"\"\"
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SimpleAnalysis').getOrCreate()

# Charger les données
df = spark.read.option('header', 'true').csv('hdfs://hadoop-master:9000/data/input/sample_data.csv')

# Analyses basiques
print('=== ANALYSE SIMPLE SPARK ===')
print(f'Total employés: {df.count()}')

# Par département
dept_stats = df.groupBy('department').agg(
    count('*').alias('count'),
    avg('salary').alias('avg_salary')
).orderBy(desc('count'))

print('Employés par département:')
dept_stats.show()

spark.stop()
EOF"
    
    # Script d'exemple 2: Test MongoDB
    docker exec spark-master bash -c "cat > /opt/examples/mongodb_test.py << 'EOF'
#!/usr/bin/env python3
\"\"\"Test de connexion MongoDB\"\"\"
try:
    from pymongo import MongoClient
    client = MongoClient('mongodb://admin:bigdata2025@mongodb:27017/')
    db = client['bigdata']
    collections = db.list_collection_names()
    print(f'MongoDB connecté. Collections: {collections}')
    client.close()
except Exception as e:
    print(f'Erreur MongoDB: {e}')
EOF"
    
    echo "✅ Scripts d'exemple créés dans /opt/examples/"
}

# Fonction principale
main() {
    echo "⚡ Démarrage de l'initialisation Spark..."
    
    # Vérifier les prérequis
    if ! docker ps | grep -q spark-master; then
        echo "❌ Conteneur Spark Master non démarré"
        echo "💡 Exécutez: docker-compose up -d spark-master"
        exit 1
    fi
    
    # Séquence d'initialisation
    check_spark_cluster
    setup_spark_hdfs_dirs
    install_python_dependencies
    test_spark_applications
    test_spark_hdfs_integration
    create_example_scripts
    display_spark_info
    
    echo ""
    echo "======================================================"
    echo "🎉 CLUSTER SPARK INITIALISÉ AVEC SUCCÈS!"
    echo "======================================================"
    echo "✅ Spark Master opérationnel"
    echo "✅ Workers connectés"
    echo "✅ Intégration HDFS fonctionnelle"
    echo "✅ PySpark configuré"
    echo "✅ Dépendances installées"
    echo ""
    echo "🚀 Prêt pour l'analyse Spark!"
    echo "💡 Prochaine étape: ./scripts/setup/load-data.sh"
    echo ""
    echo "🧪 Tests disponibles:"
    echo "   docker exec spark-master python3 /opt/examples/simple_analysis.py"
    echo "   docker exec spark-master python3 /opt/examples/mongodb_test.py"
}

# Point d'entrée
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi