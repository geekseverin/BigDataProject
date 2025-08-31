# Hadoop Master Node - NameNode + ResourceManager + Secondary NameNode
FROM bigdata/hadoop-base:3.3.4

LABEL maintainer="BigData UCAO 2025"
LABEL description="Hadoop Master Node avec NameNode et ResourceManager"

USER root

# Installer des outils supplémentaires pour le master
RUN apt-get update && apt-get install -y \
    htop \
    iotop \
    supervisor \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Créer le répertoire pour les scripts et générer init-master.sh
RUN mkdir -p /opt/scripts && \
    echo '#!/bin/bash' > /opt/scripts/init-master.sh && \
    echo 'set -e' >> /opt/scripts/init-master.sh && \
    echo '' >> /opt/scripts/init-master.sh && \
    echo 'echo "=== Initialisation du Hadoop Master ==="' >> /opt/scripts/init-master.sh && \
    echo '' >> /opt/scripts/init-master.sh && \
    echo '# Démarrer SSH' >> /opt/scripts/init-master.sh && \
    echo 'sudo service ssh start' >> /opt/scripts/init-master.sh && \
    echo '' >> /opt/scripts/init-master.sh && \
    echo '# Attendre que les autres nœuds soient prêts' >> /opt/scripts/init-master.sh && \
    echo 'echo "Attente des nœuds workers..."' >> /opt/scripts/init-master.sh && \
    echo 'sleep 30' >> /opt/scripts/init-master.sh && \
    echo '' >> /opt/scripts/init-master.sh && \
    echo '# Formater HDFS si nécessaire (première fois seulement)' >> /opt/scripts/init-master.sh && \
    echo 'if [ ! -d "$HADOOP_HOME/data/dfs/name/current" ]; then' >> /opt/scripts/init-master.sh && \
    echo '    echo "Formatage du NameNode..."' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs namenode -format -force' >> /opt/scripts/init-master.sh && \
    echo 'fi' >> /opt/scripts/init-master.sh && \
    echo '' >> /opt/scripts/init-master.sh && \
    echo '# Démarrer les services selon le rôle' >> /opt/scripts/init-master.sh && \
    echo 'if [ "$HADOOP_ROLE" = "master" ]; then' >> /opt/scripts/init-master.sh && \
    echo '    echo "Démarrage NameNode..."' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs --daemon start namenode' >> /opt/scripts/init-master.sh && \
    echo '    echo "Démarrage ResourceManager..."' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/yarn --daemon start resourcemanager' >> /opt/scripts/init-master.sh && \
    echo '    echo "Démarrage JobHistoryServer..."' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/mapred --daemon start historyserver' >> /opt/scripts/init-master.sh && \
    echo 'elif [ "$HADOOP_ROLE" = "secondary" ]; then' >> /opt/scripts/init-master.sh && \
    echo '    echo "Démarrage SecondaryNameNode..."' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs --daemon start secondarynamenode' >> /opt/scripts/init-master.sh && \
    echo 'fi' >> /opt/scripts/init-master.sh && \
    echo '' >> /opt/scripts/init-master.sh && \
    echo '# Attendre un peu puis afficher le statut' >> /opt/scripts/init-master.sh && \
    echo 'sleep 10' >> /opt/scripts/init-master.sh && \
    echo 'echo "=== Statut des services ==="' >> /opt/scripts/init-master.sh && \
    echo 'jps' >> /opt/scripts/init-master.sh && \
    echo '' >> /opt/scripts/init-master.sh && \
    echo '# Créer des répertoires HDFS utiles' >> /opt/scripts/init-master.sh && \
    echo 'if [ "$HADOOP_ROLE" = "master" ]; then' >> /opt/scripts/init-master.sh && \
    echo '    sleep 15  # Attendre que HDFS soit complètement initialisé' >> /opt/scripts/init-master.sh && \
    echo '    echo "Création des répertoires HDFS..."' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hadoop' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /data/input' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /data/output' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs dfs -chmod 777 /user' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs dfs -chmod 777 /data' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs dfs -chmod 777 /data/input' >> /opt/scripts/init-master.sh && \
    echo '    $HADOOP_HOME/bin/hdfs dfs -chmod 777 /data/output' >> /opt/scripts/init-master.sh && \
    echo 'fi' >> /opt/scripts/init-master.sh && \
    echo '' >> /opt/scripts/init-master.sh && \
    echo 'echo "=== Master node prêt ==="' >> /opt/scripts/init-master.sh && \
    echo 'tail -f /dev/null' >> /opt/scripts/init-master.sh && \
    chmod +x /opt/scripts/init-master.sh

# Exposer les ports nécessaires
EXPOSE 9870 8088 19888 8020 8032 9868

# Passer à l'utilisateur hadoop
USER hadoop
WORKDIR /opt/hadoop

# Point d'entrée
CMD ["/opt/scripts/init-master.sh"]