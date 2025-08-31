#!/bin/bash
# =====================================================
# SCRIPT DE CHARGEMENT DES DONNÉES
# =====================================================
# Charge les données dans HDFS et MongoDB
# Lance les analyses Pig et Spark
# Utilisation: ./load-data.sh

set -e

echo "======================================================"
echo "📊 CHARGEMENT ET TRAITEMENT DES DONNÉES"
echo "======================================================"

# Variables de configuration
MONGODB_CONTAINER="mongodb"
HADOOP_MASTER="hadoop-master"
SPARK_MASTER="spark-master"

# Fonction de vérification des prérequis
check_prerequisites() {
    echo "🔍 Vérification des prérequis..."
    
    # Vérifier les conteneurs
    local required_containers=("hadoop-master" "spark-master" "mongodb")
    
    for container in "${required_containers[@]}"; do
        if ! docker ps --format "table {{.Names}}" | grep -q "^$container$"; then
            echo "❌ Conteneur $container non disponible"
            exit 1
        fi
        echo "✅ $container disponible"
    done
    
    # Vérifier HDFS
    if ! docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /'" > /dev/null 2>&1; then
        echo "❌ HDFS non accessible"
        echo "💡 Exécutez: ./scripts/setup/init-hadoop.sh"
        exit 1
    fi
    
    echo "✅ Prérequis vérifiés"
}

# Fonction de rechargement des données dans HDFS
reload_hdfs_data() {
    echo "📁 Rechargement des données dans HDFS..."
    
    # Nettoyer les anciens données
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -rm -r -f /data/input/*'" 2>/dev/null || true
    
    # Vérifier la présence des fichiers locaux
    if [ ! -f "./data/sample_data.csv" ] || [ ! -f "./data/sales_data.csv" ]; then
        echo "❌ Fichiers de données manquants dans ./data/"
        exit 1
    fi
    
    # Copier vers le conteneur
    docker cp ./data/sample_data.csv $HADOOP_MASTER:/tmp/
    docker cp ./data/sales_data.csv $HADOOP_MASTER:/tmp/
    
    # Charger dans HDFS
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -put /tmp/sample_data.csv /data/input/'"
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -put /tmp/sales_data.csv /data/input/'"
    
    # Vérifier le chargement
    echo "🔍 Vérification des données dans HDFS:"
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls -h /data/input/'"
    
    echo "✅ Données rechargées dans HDFS"
}

# Fonction d'initialisation MongoDB
setup_mongodb_data() {
    echo "🍃 Initialisation des données MongoDB..."
    
    # Attendre que MongoDB soit prêt
    echo "⏳ Attente de MongoDB..."
    local count=0
    while ! docker exec $MONGODB_CONTAINER mongosh --eval "db.runCommand('ping')" > /dev/null 2>&1; do
        if [ $count -ge 30 ]; then
            echo "❌ MongoDB non disponible"
            exit 1
        fi
        sleep 2
        ((count++))
    done
    
    # Script d'initialisation MongoDB
    docker exec $MONGODB_CONTAINER bash -c "cat > /tmp/init_mongo.js << 'EOF'
// Script d'initialisation MongoDB pour le projet Big Data

use bigdata

// Supprimer les anciennes collections
db.sales.drop()
db.customers.drop()
db.products.drop()

// Insérer des données de ventes
db.sales.insertMany([
    {
        _id: ObjectId(),
        transaction_id: \"MONGO_T001\",
        product: {
            id: \"P201\",
            name: \"Laptop Gaming\",
            category: \"Electronics\",
            brand: \"GameTech\"
        },
        sale_info: {
            quantity: 1,
            unit_price: 1599.99,
            total_amount: 1599.99,
            discount: 100.0,
            tax: 128.0
        },
        customer: {
            id: \"C201\",
            name: \"Alexandre Dubois\",
            email: \"alex@email.fr\",
            loyalty_tier: \"Platinum\"
        },
        sale_date: new Date(\"2024-08-30\"),
        location: {
            region: \"Europe\",
            country: \"France\",
            city: \"Paris\"
        },
        sales_rep: \"Marie Martin\",
        payment_method: \"Credit Card\",
        status: \"completed\",
        metadata: {
            source: \"online\",
            campaign: \"gaming_promo\",
            created_at: new Date()
        }
    },
    {
        _id: ObjectId(),
        transaction_id: \"MONGO_T002\",
        product: {
            id: \"P202\",
            name: \"Mechanical Keyboard\",
            category: \"Electronics\",
            brand: \"KeyMaster\"
        },
        sale_info: {
            quantity: 2,
            unit_price: 189.99,
            total_amount: 379.98,
            discount: 20.0,
            tax: 28.8
        },
        customer: {
            id: \"C202\",
            name: \"Sofia Rodriguez\",
            email: \"sofia@email.es\",
            loyalty_tier: \"Gold\"
        },
        sale_date: new Date(\"2024-08-30\"),
        location: {
            region: \"Europe\",
            country: \"Spain\",
            city: \"Barcelona\"
        },
        sales_rep: \"Carlos Lopez\",
        payment_method: \"PayPal\",
        status: \"completed\",
        metadata: {
            source: \"mobile\",
            campaign: \"productivity_week\",
            created_at: new Date()
        }
    },
    {
        _id: ObjectId(),
        transaction_id: \"MONGO_T003\",
        product: {
            id: \"P203\",
            name: \"4K Monitor\",
            category: \"Electronics\",
            brand: \"PixelPro\"
        },
        sale_info: {
            quantity: 1,
            unit_price: 699.99,
            total_amount: 699.99,
            discount: 50.0,
            tax: 52.0
        },
        customer: {
            id: \"C203\",
            name: \"Raj Patel\",
            email: \"raj@email.in\",
            loyalty_tier: \"Silver\"
        },
        sale_date: new Date(\"2024-08-30\"),
        location: {
            region: \"Asia\",
            country: \"India\",
            city: \"Mumbai\"
        },
        sales_rep: \"Priya Sharma\",
        payment_method: \"UPI\",
        status: \"completed\",
        metadata: {
            source: \"store\",
            campaign: \"office_upgrade\",
            created_at: new Date()
        }
    }
])

// Insérer des données clients
db.customers.insertMany([
    {
        _id: ObjectId(),
        customer_id: \"C201\",
        name: \"Alexandre Dubois\",
        email: \"alex@email.fr\",
        phone: \"+33 1 42 86 83 45\",
        address: {
            street: \"15 rue de la Paix\",
            city: \"Paris\",
            country: \"France\",
            postal_code: \"75001\"
        },
        loyalty_tier: \"Platinum\",
        registration_date: new Date(\"2023-01-15\"),
        total_orders: 12,
        total_spent: 18500.50,
        last_order_date: new Date(\"2024-08-30\"),
        preferences: [\"Electronics\", \"Gaming\", \"Tech\"],
        marketing_consent: true
    },
    {
        _id: ObjectId(),
        customer_id: \"C202\",
        name: \"Sofia Rodriguez\",
        email: \"sofia@email.es\",
        phone: \"+34 91 123 45 67\",
        address: {
            street: \"Calle Gran Via 123\",
            city: \"Barcelona\",
            country: \"Spain\",
            postal_code: \"08001\"
        },
        loyalty_tier: \"Gold\",
        registration_date: new Date(\"2023-03-20\"),
        total_orders: 8,
        total_spent: 4250.75,
        last_order_date: new Date(\"2024-08-30\"),
        preferences: [\"Office\", \"Productivity\"],
        marketing_consent: true
    }
])

// Créer des index pour les performances
db.sales.createIndex({\"sale_date\": 1})
db.sales.createIndex({\"location.region\": 1})
db.sales.createIndex({\"product.category\": 1})
db.sales.createIndex({\"customer.loyalty_tier\": 1})

db.customers.createIndex({\"customer_id\": 1}, {unique: true})
db.customers.createIndex({\"email\": 1}, {unique: true})
db.customers.createIndex({\"loyalty_tier\": 1})

print(\"✅ Données MongoDB initialisées avec succès\")
print(\"📊 Collections créées:\")
print(\"   - sales: \" + db.sales.countDocuments() + \" documents\")
print(\"   - customers: \" + db.customers.countDocuments() + \" documents\")
EOF"
    
    # Exécuter le script d'initialisation
    docker exec $MONGODB_CONTAINER mongosh -u admin -p bigdata2025 --authenticationDatabase admin bigdata /tmp/init_mongo.js
    
    echo "✅ Données MongoDB initialisées"
}

# Fonction pour lancer l'analyse Pig
run_pig_analysis() {
    echo "🐷 Lancement des analyses Apache Pig..."
    
    # Nettoyer les anciens résultats
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -rm -r -f /data/output/pig/*'" 2>/dev/null || true
    
    # Copier les scripts Pig
    docker cp ./scripts/pig/data_analysis.pig $HADOOP_MASTER:/tmp/
    docker cp ./scripts/pig/sales_analysis.pig $HADOOP_MASTER:/tmp/
    
    # Lancer l'analyse des données employés
    echo "👥 Analyse des données employés..."
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c 'cd /tmp && $PIG_HOME/bin/pig -x mapreduce data_analysis.pig'" || {
        echo "⚠️  Erreur lors de l'analyse Pig des employés"
    }
    
    # Lancer l'analyse des ventes
    echo "💰 Analyse des données de ventes..."
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c 'cd /tmp && $PIG_HOME/bin/pig -x mapreduce sales_analysis.pig'" || {
        echo "⚠️  Erreur lors de l'analyse Pig des ventes"
    }
    
    # Vérifier les résultats
    echo "🔍 Vérification des résultats Pig:"
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /data/output/pig/'" || echo "   Aucun résultat Pig trouvé"
    
    echo "✅ Analyses Pig terminées"
}

# Fonction pour lancer l'analyse Spark
run_spark_analysis() {
    echo "⚡ Lancement des analyses Apache Spark..."
    
    # Nettoyer les anciens résultats
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -rm -r -f /data/output/spark/*'" 2>/dev/null || true
    
    # Copier les scripts Spark
    docker cp ./scripts/spark/data_processing.py $SPARK_MASTER:/tmp/
    docker cp ./scripts/spark/mongodb_reader.py $SPARK_MASTER:/tmp/
    
    # Lancer le traitement des données
    echo "📊 Traitement avancé des données..."
    docker exec $SPARK_MASTER bash -c "/opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.hadoop.fs.defaultFS=hdfs://hadoop-master:9000 \
        /tmp/data_processing.py" || {
        echo "⚠️  Erreur lors du traitement Spark"
    }
    
    # Lancer l'intégration MongoDB
    echo "🍃 Intégration MongoDB avec Spark..."
    docker exec $SPARK_MASTER bash -c "/opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
        --conf spark.mongodb.input.uri=mongodb://admin:bigdata2025@mongodb:27017/bigdata.sales \
        --conf spark.mongodb.output.uri=mongodb://admin:bigdata2025@mongodb:27017/bigdata.results \
        /tmp/mongodb_reader.py" || {
        echo "⚠️  Erreur lors de l'intégration MongoDB-Spark"
    }
    
    # Vérifier les résultats
    echo "🔍 Vérification des résultats Spark:"
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /data/output/spark/'" || echo "   Aucun résultat Spark trouvé"
    
    echo "✅ Analyses Spark terminées"
}

# Fonction pour afficher les résultats
display_results() {
    echo "======================================================"
    echo "📈 RÉSULTATS DES ANALYSES"
    echo "======================================================"
    
    # Résultats HDFS
    echo "🗄️  Résultats stockés dans HDFS:"
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls -R /data/output/'" 2>/dev/null || echo "   Aucun résultat trouvé"
    
    echo ""
    echo "📊 Aperçu des résultats Pig (si disponibles):"
    
    # Afficher quelques résultats Pig
    local pig_results=(
        "/data/output/pig/department_statistics"
        "/data/output/pig/sales_by_category"
        "/data/output/pig/top_10_earners"
    )
    
    for result in "${pig_results[@]}"; do
        echo "   📄 $result:"
        docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -cat $result/part-* 2>/dev/null | head -5'" || echo "     Fichier non trouvé"
        echo ""
    done
    
    echo "⚡ Aperçu des résultats Spark (si disponibles):"
    docker exec $HADOOP_MASTER bash -c "su - hadoop -c '$HADOOP_HOME/bin/hdfs dfs -ls /data/output/spark/'" 2>/dev/null || echo "   Résultats Spark non disponibles"
    
    echo ""
    echo "🍃 Données MongoDB:"
    docker exec $MONGODB_CONTAINER bash -c "mongosh -u admin -p bigdata2025 --authenticationDatabase admin --eval '
        use bigdata
        print(\"Collections:\")
        db.getCollectionNames().forEach(function(name) {
            print(\"  \" + name + \": \" + db[name].countDocuments() + \" documents\")
        })
    '" 2>/dev/null || echo "   MongoDB non accessible"
}

# Fonction principale
main() {
    echo "📊 Démarrage du chargement et traitement des données..."
    
    # Vérifications préalables
    check_prerequisites
    
    # Chargement des données
    reload_hdfs_data
    setup_mongodb_data
    
    # Analyses
    run_pig_analysis
    run_spark_analysis
    
    # Affichage des résultats
    display_results
    
    echo ""
    echo "======================================================"
    echo "🎉 CHARGEMENT ET ANALYSES TERMINÉS!"
    echo "======================================================"
    echo "✅ Données chargées dans HDFS"
    echo "✅ Données initialisées dans MongoDB"
    echo "✅ Analyses Pig exécutées"
    echo "✅ Analyses Spark exécutées"
    echo ""
    echo "🌐 Prochaine étape: Démarrer l'application web"
    echo "   docker-compose up web-app"
    echo "   Accès: http://localhost:5000"
}

# Point d'entrée
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi