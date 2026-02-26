#!/bin/bash
echo "Démarrage du pipeline d'analyse de tickets..."

echo "Création des dossiers..."
mkdir -p scripts config monitoring volumes/redpanda-data volumes/spark-logs volumes/stats-data

echo "Vérification de la structure..."
ls -la src/producers/ src/processors/

echo "Construction des images Docker..."
docker-compose build

echo "Démarrage des services..."
docker-compose up -d

echo "Attente du démarrage des services..."
sleep 30

echo "Statut des services :"
docker-compose ps

echo "Pipeline démarré avec succès !"
echo "Interfaces disponibles :"
echo "   - Redpanda Console: http://localhost:8080"
echo "   - Schema Registry: http://localhost:18081"
echo "   - Kafka API: localhost:19092"
echo "Volumes créés :"
echo "   - Redpanda data: ./volumes/redpanda-data"
echo "   - Spark logs: ./volumes/spark-logs"
echo "   - Stats data: ./volumes/stats-data"