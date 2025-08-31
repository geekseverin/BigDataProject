#!/bin/bash
# =====================================================
# SCRIPT D'INITIALISATION HADOOP CLUSTER
# =====================================================
# Initialise le cluster Hadoop et configure HDFS
# Utilisation: ./init-hadoop.sh

set -e  # Arrêter en cas d'erreur

echo "======================================================"
echo "🚀 INITIALISATION DU CLUSTER HADOOP"
echo "======================================================"

# Variables de configuration
HADOOP_MASTER="hadoop-master"
HDFS_USER="hadoop"
TIMEOUT=300  # 5 minutes timeout

# Fonction pour attendre qu'un service soit disponible
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
        echo "   Tentative $((count + 1))/$timeout - Service $service_name pas encore prêt..."
        sleep 1
        ((count++))
    done
    
    echo "✅ Service $service_name disponible"
    return 0
}

# Fonction pour vérifier le statut HDFS
check_hdfs_status() {
    echo "🔍 Vérification du statut HDFS..."
    
    if docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfsadmin -report'" 2>/dev/null | grep -q "Live datanodes"; then
        echo "✅ HDFS opérationnel"
        return 0
    else
        echo "❌ HDFS non opérationnel"
        return 1
    fi
}

# Fonction principale d'initialisation
initialize_hadoop() {
    echo "🔧 Initialisation du cluster Hadoop..."
    
    # 1. Attendre que tous les conteneurs soient en cours d'exécution
    echo "📋 Vérification des conteneurs..."
    
    local containers=("hadoop-master" "hadoop-secondary" "hadoop-worker-1" "hadoop-worker-2" "hadoop-worker-3")
    for container in "${containers[@]}"; do
        if ! docker ps --format "table {{.Names}}" | grep -q "^$container$"; then
            echo "❌ Conteneur $container non trouvé ou non démarré"
            echo "💡 Exécutez: docker-compose up -d"
            exit 1
        fi
        echo "✅ Conteneur $container en cours d'exécution"
    done
    
    # 2. Attendre que les services Hadoop soient prêts
    echo "🔄 Attente des services Hadoop..."
    wait_for_service $HADOOP_MASTER 9870 "NameNode" 60
    wait_for_service $HADOOP_MASTER 8088 "ResourceManager" 60
    
    # Attendre un peu plus pour la stabilisation
    echo "⏱️  Stabilisation des services (30s)..."
    sleep 30
    
    # 3. Vérifier le statut HDFS
    if ! check_hdfs_status; then
        echo "⚠️  HDFS pas encore complètement initialisé, tentative de récupération..."
        
        # Redémarrer les services si nécessaire
        echo "🔄 Redémarrage des services HDFS..."
        docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/sbin/stop-dfs.sh'" || true
        sleep 5
        docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/sbin/start-dfs.sh'"
        
        # Attendre et vérifier à nouveau
        sleep 30
        if ! check_hdfs_status; then
            echo "❌ Impossible d'initialiser HDFS correctement"
            exit 1
        fi
    fi
    
    echo "✅ Cluster Hadoop initialisé avec succès"
}

# Fonction pour créer la structure de répertoires HDFS
setup_hdfs_directories() {
    echo "📁 Création de la structure de répertoires HDFS..."
    
    local directories=(
        "/user"
        "/user/hadoop"
        "/user/root"
        "/data"
        "/data/input"
        "/data/output"
        "/data/output/pig"
        "/data/output/spark"
        "/spark-logs"
        "/spark-checkpoints"
        "/tmp"
    )
    
    for dir in "${directories[@]}"; do
        echo "   Création du répertoire: $dir"
        docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -mkdir -p $dir'" || {
            echo "⚠️  Répertoire $dir existe déjà ou erreur de création"
        }
    done
    
    # Définir les permissions
    echo "🔐 Configuration des permissions HDFS..."
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -chmod 755 /user'"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -chmod 777 /data'"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -chmod 777 /data/input'"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -chmod 777 /data/output'"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -chmod 777 /tmp'"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -chmod 777 /spark-logs'"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -chmod 777 /spark-checkpoints'"
    
    echo "✅ Structure de répertoires HDFS créée"
}

# Fonction pour charger les données initiales
load_initial_data() {
    echo "📊 Chargement des données initiales dans HDFS..."
    
    # Vérifier la présence des fichiers de données
    if [ ! -f "./data/sample_data.csv" ] || [ ! -f "./data/sales_data.csv" ]; then
        echo "⚠️  Fichiers de données non trouvés dans ./data/"
        echo "💡 Assurez-vous que sample_data.csv et sales_data.csv sont présents"
        return 1
    fi
    
    # Copier les fichiers vers le conteneur master
    echo "📋 Copie des fichiers vers le conteneur master..."
    docker cp ./data/sample_data.csv hadoop-master:/tmp/
    docker cp ./data/sales_data.csv hadoop-master:/tmp/
    
    # Charger dans HDFS
    echo "⬆️  Upload vers HDFS..."
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -put -f /tmp/sample_data.csv /data/input/'"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -put -f /tmp/sales_data.csv /data/input/'"
    
    # Vérifier le chargement
    echo "🔍 Vérification des données dans HDFS..."
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /data/input/'"
    
    # Afficher les tailles des fichiers
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -du -h /data/input/'"
    
    echo "✅ Données initiales chargées avec succès"
}

# Fonction pour afficher les informations du cluster
display_cluster_info() {
    echo "======================================================"
    echo "📊 INFORMATIONS DU CLUSTER HADOOP"
    echo "======================================================"
    
    # Rapport HDFS
    echo "🗄️  Rapport HDFS:"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfsadmin -report'" | head -20
    
    echo ""
    echo "🔗 URLs d'accès:"
    echo "   NameNode Web UI:     http://localhost:9870"
    echo "   ResourceManager:     http://localhost:8088"
    echo "   JobHistory Server:   http://localhost:19888"
    echo "   Secondary NameNode:  http://localhost:9868"
    
    echo ""
    echo "📁 Contenu HDFS /data/input/:"
    docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /data/input/'" || echo "   Aucun fichier trouvé"
    
    echo ""
    echo "⚙️  Services Java actifs sur le master:"
    docker exec hadoop-master bash -c "jps"
    
    echo ""
    echo "🎯 Utilisation mémoire:"
    docker exec hadoop-master bash -c "free -h"
}

# Fonction de nettoyage en cas d'erreur
cleanup_on_error() {
    echo "🧹 Nettoyage en cas d'erreur..."
    echo "⚠️  Pour réinitialiser complètement:"
    echo "   docker-compose down -v"
    echo "   docker-compose up --build -d"
}

# Fonction principale
main() {
    echo "🚀 Démarrage de l'initialisation Hadoop..."
    
    # Vérifier que Docker Compose est en cours d'exécution
    if ! docker-compose ps | grep -q "Up"; then
        echo "❌ Les services ne semblent pas être démarrés"
        echo "💡 Exécutez d'abord: docker-compose up -d"
        exit 1
    fi
    
    # Trap pour nettoyage en cas d'erreur
    trap cleanup_on_error ERR
    
    # Séquence d'initialisation
    initialize_hadoop
    setup_hdfs_directories
    load_initial_data
    display_cluster_info
    
    echo ""
    echo "======================================================"
    echo "🎉 CLUSTER HADOOP INITIALISÉ AVEC SUCCÈS!"
    echo "======================================================"
    echo "✅ NameNode opérationnel"
    echo "✅ DataNodes connectés"
    echo "✅ ResourceManager actif"
    echo "✅ Données chargées dans HDFS"
    echo ""
    echo "🚀 Prêt pour l'analyse Big Data!"
    echo "💡 Prochaine étape: ./scripts/setup/init-spark.sh"
}

# Point d'entrée
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi