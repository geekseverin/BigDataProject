# Hadoop Base Image - Foundation pour tous les nœuds Hadoop
FROM ubuntu:22.04

LABEL maintainer="BigData UCAO 2025"
LABEL description="Base image pour cluster Hadoop avec Java 8 et Hadoop 3.3.4"

# Variables d'environnement
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_VERSION=3.3.4
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_LOG_DIR=$HADOOP_HOME/logs
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Eviter les interactions lors de l'installation
ENV DEBIAN_FRONTEND=noninteractive

# Mettre à jour et installer les dépendances
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    wget \
    curl \
    ssh \
    rsync \
    vim \
    net-tools \
    iputils-ping \
    python3 \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Créer utilisateur hadoop
RUN useradd -m -s /bin/bash hadoop && \
    echo "hadoop:hadoop" | chpasswd && \
    adduser hadoop sudo

# Configuration SSH pour communication inter-nœuds
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "Host *" >> ~/.ssh/config && \
    echo "  StrictHostKeyChecking no" >> ~/.ssh/config && \
    echo "  UserKnownHostsFile=/dev/null" >> ~/.ssh/config

# Télécharger et installer Hadoop
RUN cd /opt && \
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    chown -R hadoop:hadoop /opt/hadoop

# Télécharger et installer Apache Pig
ENV PIG_VERSION=0.17.0
ENV PIG_HOME=/opt/pig
RUN cd /opt && \
    wget -q https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz && \
    mv pig-${PIG_VERSION} pig && \
    rm pig-${PIG_VERSION}.tar.gz && \
    chown -R hadoop:hadoop /opt/pig

ENV PATH=$PATH:$PIG_HOME/bin

# Créer les répertoires nécessaires
RUN mkdir -p $HADOOP_HOME/data && \
    mkdir -p $HADOOP_LOG_DIR && \
    mkdir -p /opt/scripts && \
    chown -R hadoop:hadoop $HADOOP_HOME/data && \
    chown -R hadoop:hadoop $HADOOP_LOG_DIR && \
    chown -R hadoop:hadoop /opt/scripts

# Installer les connecteurs MongoDB pour Hadoop
RUN cd $HADOOP_HOME/share/hadoop/common/lib && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongo-hadoop/mongo-hadoop-core/2.0.2/mongo-hadoop-core-2.0.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-legacy/4.7.2/mongodb-driver-legacy-4.7.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/bson/4.7.2/bson-4.7.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.7.2/mongodb-driver-core-4.7.2.jar

# Créer script de démarrage SSH
RUN echo '#!/bin/bash' > /start-ssh.sh && \
    echo 'service ssh start' >> /start-ssh.sh && \
    echo 'tail -f /dev/null' >> /start-ssh.sh && \
    chmod +x /start-ssh.sh

# Passer à l'utilisateur hadoop
USER hadoop
WORKDIR /opt/hadoop

# Variables d'environnement pour l'utilisateur hadoop
RUN echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc && \
    echo "export HADOOP_HOME=$HADOOP_HOME" >> ~/.bashrc && \
    echo "export HADOOP_CONF_DIR=$HADOOP_CONF_DIR" >> ~/.bashrc && \
    echo "export PATH=$PATH" >> ~/.bashrc && \
    echo "export PIG_HOME=$PIG_HOME" >> ~/.bashrc

# Configuration SSH pour l'utilisateur hadoop
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "Host *" >> ~/.ssh/config && \
    echo "  StrictHostKeyChecking no" >> ~/.ssh/config && \
    echo "  UserKnownHostsFile=/dev/null" >> ~/.ssh/config

# Point d'entrée par défaut
CMD ["/bin/bash"]