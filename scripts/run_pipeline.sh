#!/bin/bash
echo "Démarrage du pipeline d'analyse de tickets (Python direct)..."

echo "Vérification des dépendances..."
if ! pip show kafka-python > /dev/null 2>&1; then
    echo "Installation des dépendances..."
    pip install -r requirements.txt
fi

echo "Vérification de la connectivité Redpanda..."
if ! nc -z localhost 19092 2>/dev/null; then
    echo "Redpanda n'est pas accessible sur localhost:19092"
    echo "Démarrage automatique de Redpanda..."
    docker-compose up redpanda redpanda-console -d
    
    echo "Attente du démarrage de Redpanda..."
    sleep 15
    
    if ! nc -z localhost 19092 2>/dev/null; then
        echo "Erreur : Redpanda n'a pas pu démarrer correctement"
        exit 1
    fi
    echo "Redpanda démarré avec succès !"
else
    echo "Redpanda est déjà accessible"
fi

if [ ! -d "stats" ]; then
    echo "Création du dossier stats..."
    mkdir -p stats
fi

echo "Étape 1: Génération des tickets..."
python src/producers/ticket_producer.py

if [ $? -ne 0 ]; then
    echo "Erreur lors de la génération des tickets"
    exit 1
fi

echo "Étape 2: Analyse et distribution des tickets..."
python src/processors/spark_analyzer.py

if [ $? -ne 0 ]; then
    echo "Erreur lors de l'analyse des tickets"
    exit 1
fi

echo "Pipeline terminé avec succès !"
echo "Résultats disponibles dans :"
echo "   - Dossier 'stats/' : Fichiers Parquet et JSON"
echo "   - Topics Kafka par équipe :"
echo "     * tickets_equipe_incident"
echo "     * tickets_equipe_support_general"
echo "     * tickets_equipe_produit"
echo "Console Redpanda : http://localhost:8080"
echo "Pour voir les logs détaillés :"
echo "   - Logs du producteur : Regarder la sortie ci-dessus"
echo "   - Logs de l'analyseur : Regarder la sortie ci-dessus" 