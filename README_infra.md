# Architecture hybride cloud/on-premise avec Redpanda

Ce projet présente une architecture hybride combinant une infrastructure on-premise avec des services cloud pour permettre l'ingestion, le traitement, l'analyse et la sécurisation de données en temps réel autour de **Redpanda** comme moteur de streaming central.

---

## Objectifs

- Migrer et analyser des données on-premise dans le cloud.
- Stocker les données non structurées à l’échelle.
- Permettre le traitement temps réel via Redpanda.
- Unifier la gestion des identités et des accès.

---

## Composants principaux

| Fonction                     | Composant Cloud                     | Justification |
|-----------------------------|-------------------------------------|------------------------------|
| Ingestion temps réel        | **Redpanda**                        | Kafka-compatible, simple, léger, orchestration intégrée |
| Stockage non structuré      | **S3**           | Scalable, durable, économique |
| Entrepôt analytique         | **Azure Synapse**       | Requêtes SQL complexes, scalable, forte intégration |
| Gestion des identités       | **Azure Active Directory**          | SSO, MFA, RBAC unifié avec l’AD local |
| Sync identité hybride       | **Azure AD Connect**                | Synchronisation AD on-prem ↔ cloud |

---

## Fonctionnement global

### Ingestion et traitement de données

- Les données issues des bases **SQL Server**, **ERP** ou **CRM** sont extraites via des outils d’ETL/CDC (Airbyte).
- Les **données non structurées** (IoT, logs, fichiers) sont captées directement en temps réel et envoyées à **Redpanda**.
- Redpanda distribue les flux vers :
  - **Cloud Object Storage** pour archivage ou accès différé
  - **Entrepôt de données** pour analyses

### Sécurité et accès

- L’**Active Directory local** est synchronisé via **Azure AD Connect** vers **Azure Active Directory**.
- Azure AD fournit :
  - Authentification unique (SSO)
  - MFA
  - RBAC unifié
- Les accès au DWH, au stockage, à Redpanda UI sont sécurisés par ces identités cloud.

---