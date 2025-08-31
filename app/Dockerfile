# Dockerfile pour l'application Flask Big Data
FROM python:3.10-slim

LABEL maintainer="BigData UCAO 2025"
LABEL description="Application web Flask pour visualisation Big Data"

# Variables d'environnement
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV FLASK_APP=main.py
ENV FLASK_ENV=production

# Installer les dépendances système
RUN apt-get update && apt-get install -y \
    curl \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Créer l'utilisateur d'application
RUN useradd -m -s /bin/bash flaskuser

# Créer les répertoires de travail
WORKDIR /app
RUN mkdir -p /app/results /app/static /app/templates
RUN chown -R flaskuser:flaskuser /app

# Copier les requirements et installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application
COPY . .

# Ajuster les permissions
RUN chown -R flaskuser:flaskuser /app
RUN chmod +x /app/main.py

# Passer à l'utilisateur non-root
USER flaskuser

# Exposer le port
EXPOSE 5000

# Script de démarrage avec vérifications de santé
RUN echo '#!/bin/bash\n\
echo "=== Démarrage Application Flask Big Data ==="\n\
echo "Vérification des services externes..."\n\
\n\
# Attendre MongoDB\n\
echo "⏳ Attente MongoDB..."\n\
while ! nc -z mongodb 27017; do\n\
    echo "MongoDB non disponible, attente..."\n\
    sleep 2\n\
done\n\
echo "✅ MongoDB disponible"\n\
\n\
# Attendre Hadoop (optionnel)\n\
echo "⏳ Vérification Hadoop..."\n\
if nc -z hadoop-master 9870; then\n\
    echo "✅ Hadoop disponible"\n\
else\n\
    echo "⚠️  Hadoop non disponible (continuons quand même)"\n\
fi\n\
\n\
# Attendre Spark (optionnel)\n\
echo "⏳ Vérification Spark..."\n\
if nc -z spark-master 8080; then\n\
    echo "✅ Spark disponible"\n\
else\n\
    echo "⚠️  Spark non disponible (continuons quand même)"\n\
fi\n\
\n\
echo "🚀 Démarrage Flask..."\n\
exec python main.py\n\
' > /app/start.sh && chmod +x /app/start.sh

# Healthcheck pour Docker
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:5000/api/system/status || exit 1

# Point d'entrée
CMD ["/app/start.sh"]