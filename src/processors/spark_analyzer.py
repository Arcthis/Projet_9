from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, count, desc
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import explode, split, lower, regexp_replace
import json
from datetime import datetime
import os

spark = SparkSession.builder \
    .appName("Analyze Tickets Insights") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Démarrage de l'analyse des tickets...")

schema = StructType() \
    .add("ticket_id", StringType()) \
    .add("client_id", StringType()) \
    .add("created_at", StringType()) \
    .add("subject", StringType()) \
    .add("type", StringType()) \
    .add("priority", StringType())

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select(
        col("key"),
        col("data.*")
    )

print(f"Nombre total de tickets lus : {df_parsed.count()}")

df_with_teams = df_parsed.withColumn(
    "support_team",
    when(col("type") == "incident", "Équipe Incident")
    .when(col("type") == "question", "Équipe Support Général")
    .when(col("type") == "feature_request", "Équipe Produit")
    .otherwise("Équipe Support Général")
)

df_with_priority_level = df_with_teams.withColumn(
    "priority_level",
    when(col("priority") == "high", 3)
    .when(col("priority") == "medium", 2)
    .otherwise(1)
)

print("\n Aperçu des tickets avec équipes assignées :")
df_with_priority_level.select("ticket_id", "type", "priority", "support_team", "subject").show(truncate=False)

print("\n Statistiques par type de ticket :")
tickets_by_type = df_with_priority_level.groupBy("type") \
    .agg(
        count("*").alias("nombre_tickets"),
        count(when(col("priority") == "high", True)).alias("tickets_haute_priorite"),
        count(when(col("priority") == "medium", True)).alias("tickets_priorite_moyenne"),
        count(when(col("priority") == "low", True)).alias("tickets_basse_priorite")
    ) \
    .orderBy(desc("nombre_tickets"))

tickets_by_type.show()

print("\n Analyse des mots-clés dans les sujets :")

df_words = df_with_priority_level \
    .withColumn("clean_subject", regexp_replace(lower(col("subject")), "[^a-zA-Z\\s]", "")) \
    .withColumn("words", split(col("clean_subject"), "\\s+")) \
    .withColumn("word", explode(col("words"))) \
    .filter(col("word") != "")

word_frequency = df_words.groupBy("word") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)

print("Top 10 des mots les plus fréquents dans les sujets :")
word_frequency.show()

print("="*60)

total_tickets = df_with_priority_level.count()
high_priority = df_with_priority_level.filter(col("priority") == "high").count()
incidents = df_with_priority_level.filter(col("type") == "incident").count()

print(f"Total des tickets analysés : {total_tickets}")
print(f"Tickets haute priorité : {high_priority} ({high_priority/total_tickets*100:.1f}%)")
print(f"Incidents : {incidents} ({incidents/total_tickets*100:.1f}%)")

tickets_by_team = df_with_priority_level.groupBy("support_team") \
    .count() \
    .orderBy(desc("count"))

most_busy_team = tickets_by_team.first()
print(f"Équipe la plus sollicitée : {most_busy_team['support_team']} ({most_busy_team['count']} tickets)")

print("="*60)

print("\n Sauvegarde des insights...")

if not os.path.exists("stats"):
    os.makedirs("stats")

print("Sauvegarde des données enrichies en Parquet...")
df_with_priority_level.write.mode("overwrite").parquet("stats/tickets_enriched.parquet")

print("Sauvegarde des statistiques par type en Parquet...")
tickets_by_type.write.mode("overwrite").parquet("stats/stats_by_type.parquet")

print("Sauvegarde des mots-clés en Parquet...")
word_frequency.write.mode("overwrite").parquet("stats/word_frequency.parquet")

print("Sauvegarde des statistiques par équipe en Parquet...")
tickets_by_team.write.mode("overwrite").parquet("stats/stats_by_team.parquet")

print("Création du résumé JSON...")
summary_data = {
    "timestamp_analyse": datetime.now().isoformat(),
    "total_tickets": total_tickets,
    "tickets_haute_priorite": high_priority,
    "pourcentage_haute_priorite": round(high_priority/total_tickets*100, 1),
    "incidents": incidents,
    "pourcentage_incidents": round(incidents/total_tickets*100, 1),
    "equipe_plus_sollicitee": {
        "nom": most_busy_team['support_team'],
        "nombre_tickets": most_busy_team['count']
    },
    "statistiques_par_type": tickets_by_type.toPandas().to_dict('records'),
    "top_mots_cles": word_frequency.toPandas().to_dict('records'),
    "statistiques_par_equipe": tickets_by_team.toPandas().to_dict('records')
}

with open("stats/insights_summary.json", "w", encoding="utf-8") as f:
    json.dump(summary_data, f, ensure_ascii=False, indent=2)

print("Sauvegarde terminée !")
print("Fichiers créés dans le dossier 'stats' :")
print(" - tickets_enriched.parquet (données enrichies)")
print(" - stats_by_type.parquet (statistiques par type)")
print(" - word_frequency.parquet (fréquence des mots)")
print(" - stats_by_team.parquet (statistiques par équipe)")
print(" - insights_summary.json (résumé complet)")

# --------------- | Tickets par équipe | ---------------
print("\n" + "="*60)
print("TICKETS PAR ÉQUIPE")
print("="*60)

df_to_write = df_with_priority_level.selectExpr("key", "to_json(struct(*)) as value")

equipes = ["Équipe Incident", "Équipe Support Général", "Équipe Produit"]

for equipe in equipes:
    print(f"\n Distribution des tickets vers {equipe}...")
    
    df_equipe = df_to_write.filter(col("support_team") == equipe)
    
    topic_name = equipe.lower().replace(" ", "_").replace("é", "e").replace("è", "e")
    
    df_equipe.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:19092") \
        .option("topic", f"tickets_{topic_name}") \
        .save()
    
    count_equipe = df_equipe.count()
    print(f"{count_equipe} tickets envoyés vers le topic 'tickets_{topic_name}'")

print("\n" + "="*60)
print("RÉSUMÉ DISTRIBUTION")
print("="*60)

for equipe in equipes:
    df_equipe_count = df_with_priority_level.filter(col("support_team") == equipe)
    count_equipe = df_equipe_count.count()
    topic_name = equipe.lower().replace(" ", "_").replace("é", "e").replace("è", "e")
    
    print(f"{equipe}: {count_equipe} tickets → topic 'tickets_{topic_name}'")
    
    if count_equipe > 0:
        high_count = df_equipe_count.filter(col("priority") == "high").count()
        medium_count = df_equipe_count.filter(col("priority") == "medium").count()
        low_count = df_equipe_count.filter(col("priority") == "low").count()
        
        print(f"Haute priorité: {high_count}")
        print(f"Priorité moyenne: {medium_count}")
        print(f"Basse priorité: {low_count}")

print("="*60)

spark.stop()
print("Analyse et distribution terminées !") 