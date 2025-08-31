#!/bin/bash
# =====================================================
# SCRIPT PRINCIPAL D'ORCHESTRATION
# =====================================================
# Démarre et configure tout l'écosystème Big Data
# Utilisation: ./run_all.sh

set -e

echo "======================================================"
echo "🚀 DÉMARRAGE COMPLET DU PROJET BIG DATA"
echo "======================================================"

# Variables globales
PROJECT_NAME="bigdata-project"
LOG_FILE="./logs/run_all.log"
START_TIME=$(date +%s)

# Créer le répertoire de logs
mkdir -p ./logs

# Fonction de logging
log() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] $message" | tee -a "$LOG_FILE"
}

# Fonction d'affichage avec couleurs
print_step() {
    local step="$1"
    local description="$2"
    echo ""
    echo "======================================================"
    echo "📋 ÉTAPE $step: $description"
    echo "======================================================"
}

# Fonction de vérification des prérequis
check_requirements() {
    print_step "1" "VÉRIFICATION DES PRÉREQUIS"
    
    # Vérifier Docker
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker non installé"
        exit 1
    fi
    echo "✅ Docker disponible: $(docker --version)"
    
    # Vérifier Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose non installé"
        exit 1
    fi
    echo "✅ Docker Compose disponible: $(docker-compose --version)"
    
    # Vérifier l'espace disque (minimum 10GB)
    local available_space=$(df . | awk 'NR==2 {print $4}')
    if [ "$available_space" -lt 10485760 ]; then  # 10GB en KB
        echo "⚠️  Espace disque faible: $(($available_space / 1024 / 1024))GB disponibles"
        echo "💡 Recommandation: Libérer plus de 10GB d'espace"
    else
        echo "✅ Espace disque suffisant: $(($available_space / 1024 / 1024))GB disponibles"
    fi
    
    # Vérifier la mémoire
    local available_memory=$(free -m | awk 'NR==2{printf "%.0f", $7}')
    if [ "$available_memory" -lt 4096 ]; then
        echo "⚠️  Mémoire disponible faible: ${available_memory}MB"
        echo "💡 Recommandation: Fermer des applications pour libérer de la mémoire"
    else
        echo "✅ Mémoire suffisante: ${available_memory}MB disponibles"
    fi
    
    # Vérifier les fichiers de données
    if [ ! -f "./data/sample_data.csv" ] || [ ! -f "./data/sales_data.csv" ]; then
        echo "❌ Fichiers de données manquants dans ./data/"
        exit 1
    fi
    echo "✅ Fichiers de données présents"
    
    log "Prérequis vérifiés avec succès"
}

# Fonction de nettoyage (optionnel)
cleanup_previous() {
    print_step "2" "NETTOYAGE ENVIRONNEMENT PRÉCÉDENT"
    
    echo "🧹 Nettoyage des conteneurs précédents..."
    
    # Arrêter les conteneurs existants
    docker-compose down -v 2>/dev/null || true
    
    # Supprimer les images orphelines (optionnel)
    echo "🗑️  Nettoyage des ressources Docker..."
    docker system prune -f > /dev/null 2>&1 || true
    
    # Nettoyer les logs précédents
    rm -f ./logs/*.log 2>/dev/null || true
    
    echo "✅ Environnement nettoyé"
    log "Environnement nettoyé"
}

# Fonction de construction et démarrage
build_and_start() {
    print_step "3" "CONSTRUCTION ET DÉMARRAGE DES SERVICES"
    
    echo "🔨 Construction des images Docker..."
    docker-compose build --no-cache | tee -a "$LOG_FILE"
    
    echo "🚀 Démarrage des services..."
    docker-compose up -d | tee -a "$LOG_FILE"
    
    echo "⏳ Attente du démarrage des services (60s)..."
    sleep 60
    
    # Vérifier que les services sont démarrés
    echo "🔍 Vérification des services:"
    local services=("hadoop-master" "hadoop-secondary" "hadoop-worker-1" "hadoop-worker-2" "hadoop-worker-3" "spark-master" "spark-worker-1" "spark-worker-2" "mongodb")
    
    for service in "${services[@]}"; do
        if docker ps --format "table {{.Names}}" | grep -q "^$service$"; then
            echo "   ✅ $service: Démarré"
        else
            echo "   ❌ $service: Non démarré"
            echo "🔍 Logs du service $service:"
            docker logs "$service" --tail 20
        fi
    done
    
    log "Services démarrés"
}

# Fonction d'initialisation Hadoop
initialize_hadoop() {
    print_step "4" "INITIALISATION HADOOP"
    
    echo "📊 Lancement de l'initialisation Hadoop..."
    
    # Rendre le script exécutable
    chmod +x ./scripts/setup/init-hadoop.sh
    
    # Lancer l'initialisation
    ./scripts/setup/init-hadoop.sh | tee -a "$LOG_FILE"
    
    echo "✅ Hadoop initialisé"
    log "Hadoop initialisé avec succès"
}

# Fonction d'initialisation Spark
initialize_spark() {
    print_step "5" "INITIALISATION SPARK"
    
    echo "⚡ Lancement de l'initialisation Spark..."
    
    # Rendre le script exécutable
    chmod +x ./scripts/setup/init-spark.sh
    
    # Lancer l'initialisation
    ./scripts/setup/init-spark.sh | tee -a "$LOG_FILE"
    
    echo "✅ Spark initialisé"
    log "Spark initialisé avec succès"
}

# Fonction de chargement et traitement des données
load_and_process_data() {
    print_step "6" "CHARGEMENT ET TRAITEMENT DES DONNÉES"
    
    echo "📊 Lancement du chargement des données..."
    
    # Rendre le script exécutable
    chmod +x ./scripts/setup/load-data.sh
    
    # Lancer le chargement et traitement
    ./scripts/setup/load-data.sh | tee -a "$LOG_FILE"
    
    echo "✅ Données chargées et traitées"
    log "Données chargées et analyses terminées"
}

# Fonction de démarrage de l'application web
start_web_application() {
    print_step "7" "DÉMARRAGE APPLICATION WEB"
    
    echo "🌐 Démarrage de l'application web Flask..."
    
    # Démarrer l'application web
    docker-compose up -d web-app
    
    # Attendre le démarrage
    echo "⏳ Attente du démarrage de l'application (30s)..."
    sleep 30
    
    # Vérifier que l'application est accessible
    local count=0
    while ! curl -s http://localhost:5000 > /dev/null 2>&1; do
        if [ $count -ge 10 ]; then
            echo "⚠️  Application web non accessible après 60s"
            break
        fi
        echo "   Tentative de connexion $(($count + 1))/10..."
        sleep 6
        ((count++))
    done
    
    if curl -s http://localhost:5000 > /dev/null 2>&1; then
        echo "✅ Application web accessible"
    else
        echo "❌ Application web non accessible"
        echo "🔍 Logs de l'application:"
        docker logs bigdata-webapp --tail 20
    fi
    
    log "Application web démarrée"
}

# Fonction d'affichage du statut final
display_final_status() {
    print_step "8" "STATUT FINAL DU SYSTÈME"
    
    local end_time=$(date +%s)
    local duration=$(($end_time - $start_time))
    
    echo "⏱️  Temps total d'initialisation: $((duration / 60))m $((duration % 60))s"
    echo ""
    
    # Statut des conteneurs
    echo "📋 Statut des conteneurs:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(hadoop|spark|mongo|webapp)"
    
    echo ""
    echo "🔗 URLs d'accès:"
    echo "   📊 Application Web:      http://localhost:5000"
    echo "   🗄️  Hadoop NameNode:     http://localhost:9870"
    echo "   ⚡ Spark Master:         http://localhost:8080"
    echo "   🍃 MongoDB Express:      http://localhost:8090"
    echo "   📈 YARN ResourceManager: http://localhost:8088"
    
    echo ""
    echo "📁 Données et résultats:"
    echo "   📊 Données HDFS:         /data/input/ et /data/output/"
    echo "   🍃 Collections MongoDB:  bigdata.sales, bigdata.customers"
    echo "   📋 Logs système:         ./logs/"
    
    # Afficher l'utilisation des ressources
    echo ""
    echo "💻 Utilisation des ressources:"
    echo "   🐳 Conteneurs Docker:    $(docker ps | wc -l) actifs"
    echo "   💾 Utilisation mémoire:  $(docker stats --no-stream --format 'table {{.Container}}\t{{.MemUsage}}' | tail -n +2 | head -5)"
    
    log "Déploiement terminé - durée: ${duration}s"
}

# Fonction de test de santé du système
health_check() {
    echo ""
    echo "🏥 VÉRIFICATIONS DE SANTÉ:"
    
    # Test HDFS
    if docker exec hadoop-master bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /'" > /dev/null 2>&1; then
        echo "   ✅ HDFS: Opérationnel"
    else
        echo "   ❌ HDFS: Problème détecté"
    fi
    
    # Test Spark
    if curl -s http://localhost:8080 > /dev/null 2>&1; then
        echo "   ✅ Spark: Accessible"
    else
        echo "   ❌ Spark: Non accessible"
    fi
    
    # Test MongoDB
    if docker exec mongodb mongosh --eval "db.runCommand('ping')" > /dev/null 2>&1; then
        echo "   ✅ MongoDB: Opérationnel"
    else
        echo "   ❌ MongoDB: Problème détecté"
    fi
    
    # Test Application Web
    if curl -s http://localhost:5000 > /dev/null 2>&1; then
        echo "   ✅ Application Web: Accessible"
    else
        echo "   ❌ Application Web: Non accessible"
    fi
}

# Fonction de gestion des erreurs
handle_error() {
    local exit_code=$?
    local line_number=$1
    
    echo ""
    echo "❌ ERREUR DÉTECTÉE À LA LIGNE $line_number (CODE: $exit_code)"
    echo "📋 Vérification des logs:"
    
    # Afficher les derniers logs
    if [ -f "$LOG_FILE" ]; then
        echo "🔍 Dernières entrées du log:"
        tail -20 "$LOG_FILE"
    fi
    
    echo ""
    echo "🛠️  ACTIONS DE DÉPANNAGE:"
    echo "1. Vérifier les logs: docker-compose logs [service]"
    echo "2. Redémarrer un service: docker-compose restart [service]" 
    echo "3. Réinitialiser complètement: docker-compose down -v && ./run_all.sh"
    echo "4. Vérifier l'espace disque: df -h"
    echo "5. Vérifier la mémoire: free -h"
    
    log "Erreur détectée - ligne $line_number - code $exit_code"
    
    exit $exit_code
}

# Configuration du trap pour gestion d'erreurs
trap 'handle_error $LINENO' ERR

# Fonction principale
main() {
    echo "🚀 DÉMARRAGE DU PROJET BIG DATA UCAO 2024-2025"
    echo "📅 $(date)"
    echo "📁 Répertoire: $(pwd)"
    echo ""
    
    log "=== DÉMARRAGE DU DÉPLOIEMENT ==="
    
    # Vérifier si l'utilisateur veut un déploiement propre
    if [ "$1" == "--clean" ]; then
        cleanup_previous
    else
        echo "💡 Pour un déploiement propre: ./run_all.sh --clean"
        echo ""
    fi
    
    # Séquence de déploiement
    check_requirements
    build_and_start
    initialize_hadoop
    initialize_spark
    load_and_process_data
    start_web_application
    display_final_status
    health_check
    
    echo ""
    echo "======================================================"
    echo "🎉 DÉPLOIEMENT TERMINÉ AVEC SUCCÈS!"
    echo "======================================================"
    echo ""
    echo "🚀 Votre environnement Big Data est prêt!"
    echo "📊 Accédez à l'application: http://localhost:5000"
    echo "📋 Consultez les logs: cat ./logs/run_all.log"
    echo ""
    echo "📚 Commandes utiles:"
    echo "   • Arrêter:        docker-compose down"
    echo "   • Redémarrer:     docker-compose restart"
    echo "   • Logs:           docker-compose logs [service]"
    echo "   • Status:         docker-compose ps"
    echo ""
    echo "🎓 Bon apprentissage du Big Data!"
    
    log "=== DÉPLOIEMENT TERMINÉ AVEC SUCCÈS ==="
}

# Point d'entrée
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi