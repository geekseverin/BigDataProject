# Hadoop Worker Node - DataNode + NodeManager
FROM bigdata/hadoop-base:3.3.4

LABEL maintainer="BigData UCAO 2025"
LABEL description="Hadoop Worker Node avec DataNode et NodeManager"

USER root

# Script d'initialisation pour les worker nodes
RUN echo '#!/bin/bash' > /opt/scripts/init-worker.sh && \
    echo 'set -e' >> /opt/scripts/init-worker.sh && \
    echo '' >> /opt/scripts/init-worker.sh && \
    echo 'echo "=== Initialisation Hadoop Worker $WORKER_ID ==="' >> /opt/scripts/init-worker.sh && \
    echo '' >> /opt/scripts/init-worker.sh && \
    echo '# Démarrer SSH' >> /opt/scripts/init-worker.sh && \
    echo 'sudo service ssh start' >> /opt/scripts/init-worker.sh && \
    echo '' >> /opt/scripts/init-worker.sh && \
    echo '# Attendre que le master soit prêt' >> /opt/scripts/init-worker.sh && \
    echo 'echo "Attente du master node..."' >> /opt/scripts/init-worker.sh && \
    echo 'while ! nc -z hadoop-master 9870; do' >> /opt/scripts/init-worker.sh && \
    echo '    echo "Master pas encore prêt, attente..."' >> /opt/scripts/init-worker.sh && \
    echo '    sleep 5' >> /opt/scripts/init-worker.sh && \
    echo 'done' >> /opt/scripts/init-worker.sh && \
    echo 'echo "Master détecté, démarrage des services..."' >> /opt/scripts/init-worker.sh && \
    echo '' >> /opt/scripts/init-worker.sh && \
    echo '# Attendre un peu plus pour la stabilité' >> /opt/scripts/init-worker.sh && \
    echo 'sleep 10' >> /opt/scripts/init-worker.sh && \
    echo '' >> /opt/scripts/init-worker.sh && \
    echo '# Démarrer DataNode' >> /opt/scripts/init-worker.sh && \
    echo 'echo "Démarrage DataNode..."' >> /opt/scripts/init-worker.sh && \
    echo '$HADOOP_HOME/bin/hdfs --daemon start datanode' >> /opt/scripts/init-worker.sh && \
    echo '' >> /opt/scripts/init-worker.sh && \
    echo '# Démarrer NodeManager' >> /opt/scripts/init-worker.sh && \
    echo 'echo "Démarrage NodeManager..."' >> /opt/scripts/init-worker.sh && \
    echo '$HADOOP_HOME/bin/yarn --daemon start nodemanager' >> /opt/scripts/init-worker.sh && \
    echo '' >> /opt/scripts/init-worker.sh && \
    echo '# Attendre puis afficher le statut' >> /opt/scripts/init-worker.sh && \
    echo 'sleep 5' >> /opt/scripts/init-worker.sh && \
    echo 'echo "=== Statut des services Worker $WORKER_ID ==="' >> /opt/scripts/init-worker.sh && \
    echo 'jps' >> /opt/scripts/init-worker.sh && \
    echo '' >> /opt/scripts/init-worker.sh && \
    echo 'echo "=== Worker node $WORKER_ID prêt ==="' >> /opt/scripts/init-worker.sh && \
    echo 'tail -f /dev/null' >> /opt/scripts/init-worker.sh && \
    chown hadoop:hadoop /opt/scripts/init-worker.sh && \
    chmod +x /opt/scripts/init-worker.sh

# Installer netcat pour les vérifications de connectivité
RUN apt-get update && apt-get install -y netcat && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Exposer les ports nécessaires
EXPOSE 9864 8042

# Passer à l'utilisateur hadoop
USER hadoop
WORKDIR /opt/hadoop

# Point d'entrée
CMD ["/opt/scripts/init-worker.sh"]