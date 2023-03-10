---
description: Examen de validation des compétences

Documentation officiel de notre projet : https://elhadjioumar.gitbook.io/projet-data-pipeline/
---

# Projet : Data Pipeline

## Membres du groupe

| Nom            | Prenom         |
| -------------- | -------------- |
| MAILLARD       | Luc            |
| PERCEAU        | Paul           |
| PAVLOVIC-DUVAL | Alexandre      |
| MBENGUE        | EL Hadji Oumar |

# Introduction

Dans le cadre de notre projet pour le module **Data Pipeline** nous avons réalisé une étude portant sur des données politiques en vue des élections présidentielles 2022.

Le projet comporte 3 parties:
- Collecte des données.
- Traitement des données (mise en forme, nettoyage..).
- Orchestration et automatisation du Data Pipeline.


## Collecte des données

Cette première étape est indispensable pour alimenter le pipeline de données en inputs.

Nous avons tiré nos datasets de ce site : https://www.data.gouv.fr/fr/reuses/50-1-dis-moi-ou-tu-habites-je-te-dirai-pour-qui-tu-votes/

Puis nous avons mis en place une Fake API `Mockable.io` : https://www.mockable.io/ où nous avons déployé nos datasets

Cette API joue le rôle d'intermédiaire entre le site où nous avons déployé nos apis (Mockable) et `Nifi` , notre gestionnaire de flux de données.


Les process group collectent les données des API créé précédemment .

## Traitement des données (mise en forme, nettoyage..)

Une fois les données collectées, nous avons procédés à la création d'un dataset regroupant les outputs de nos différents batchs pour créer un fichier final pour chaque candidat.

Nous avons effectués plusieurs opérations moyennant principalement `Pyspark` sur les données collectés des élections présidentielles de 2022 tel que:

- Import des fichiers en dataframes
- Suppression de colonnes
- Fusion de dataframes  
- Sortie d'un fichier au format CSV

##  Orchestration et automatisation du Data Pipeline.

Dans le but d'automatiser notre flot de données, nous avons utilisés deux DAGs sur Airflow:

- Le premier se déclenche pour effectuer la préparation des données, fusionner les datasets et créer dossier comprenant un fichier csv final  

- Le second se déclenche pour lire le fichier csv généré précedemment et pour lancer des fichiers spark d'analyse 


## Applications supplémentaires:

Nous avons pu réaliser à partir des données politiques collectées une visualisation sur tableau
