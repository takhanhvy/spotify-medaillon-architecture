# Spotify Medallion

Pipeline end-to-end de type Medallion pour un dataset Spotify volumineux, avec :

- ingestion et préparation des données source
- stockage Raw sur HDFS
- transformation Silver dans Hive
- exposition Gold dans MySQL
- API Flask sécurisée par JWT
- dashboard Streamlit

Le projet est pensé pour tourner au-dessus du stack Hadoop/Spark de `Marcel-Jan/docker-hadoop-spark`.

## Vue d'ensemble

Le pipeline suit trois couches :

- `Raw / Bronze`
  Les sources sont chargées dans HDFS au format Parquet partitionné par date d'ingestion.
- `Silver`
  Les données sont validées, jointes et enrichies dans Hive.
- `Gold`
  Quatre datamarts sont construits dans MySQL et consommés par l'API et le dashboard.

## Architecture fonctionnelle

### Sources

- `spotify_data.csv`
  Dataset source principal
- `catalogue.csv`
  Vue catalogue issue du split vertical
- `audio_features.csv`
  Vue features audio issue du split vertical

### Silver Hive

Tables créées dans la base `silver` :

- `tracks_unified`
- `genre_stats`
- `artist_stats`

### Gold MySQL

Datamarts créés dans la base `spotify_bdf` :

- `dm_track_popularity`
- `dm_genre_trends`
- `dm_top_artists`
- `dm_hits_emergents`

### Services exposés

- HDFS UI : `http://localhost:9870`
- YARN UI : `http://localhost:8088`
- Spark UI : `http://localhost:8080`
- HiveServer2 : `localhost:10000`
- MySQL Gold : `localhost:3307`
- API Swagger : `http://localhost:8000/docs`
- Dashboard Streamlit : `http://localhost:8501`

## Prérequis

### Outils

- Docker Desktop avec moteur Linux
- Bash disponible depuis Windows
  Exemple : WSL ou Git Bash
- `docker-compose` disponible dans le terminal si vous utilisez `setup_env.sh`
- Python 3 uniquement si vous voulez lancer certains scripts localement hors conteneur

### Stack externe requis

Ce projet ne démarre pas Hadoop/Spark lui-même. Il suppose que le dépôt `docker-hadoop-spark` est déjà cloné et démarré.

Exemple :

```bash
cd ../docker-hadoop-spark
docker-compose up -d
```

Avant de continuer, les conteneurs suivants doivent exister :

- `spark-master`
- `spark-worker-1`
- `spark-worker-2`
- `namenode`
- `datanode`
- `hive-server`
- `hive-metastore`

### Dataset

Le fichier source attendu est :

```text
data/spotify_data.csv
```

Ce fichier n'est pas versionné dans le dépôt Git.

Il faut le télécharger depuis Kaggle :

```text
https://www.kaggle.com/datasets/amitanshjoshi/spotify-1million-tracks/data?select=spotify_data.csv
```

Puis le placer dans le dossier `data/` avec exactement ce nom :

```text
data/spotify_data.csv
```

Les fichiers `data/catalogue.csv` et `data/audio_features.csv` sont des fichiers générés par le pipeline. Ils ne sont pas destinés à être versionnés non plus.

## Structure du projet

```text
spotify-medallion/
├── api/                     # API Flask + JWT + Swagger
├── config/                  # Scripts SQL d'initialisation MySQL
├── dashboard/               # Dashboard Streamlit
├── data/                    # Dataset source et fichiers CSV intermédiaires
├── jars/                    # Driver JDBC MySQL pour Spark
├── scripts/                 # Scripts Spark/Python du pipeline
├── tests/                   # Scripts de vérification
├── docker-compose.yml       # MySQL + API + Dashboard
├── setup_env.sh             # Préparation de l'environnement
└── run_pipeline.sh          # Exécution end-to-end du pipeline
```

## Démarrage rapide

Depuis la racine du projet :

```bash
bash setup_env.sh
bash run_pipeline.sh
```

Avant cela, assurez-vous d'avoir téléchargé `spotify_data.csv` depuis Kaggle et de l'avoir placé dans `data/`.

Exemple de séquence complète :

```bash
bash setup_env.sh
bash run_pipeline.sh
```

Une fois le pipeline terminé :

- API : `http://localhost:8000/docs`
- Dashboard : `http://localhost:8501`

## Ce que fait `setup_env.sh`

Le script [setup_env.sh](./setup_env.sh) exécute les étapes suivantes :

1. vérifie que `spark-master` tourne
2. télécharge le driver JDBC MySQL dans `jars/` s'il est absent
3. démarre `mysql`, `api` et `dashboard`
4. copie les scripts dans `spark-master`
5. copie le JAR MySQL dans `spark-master:/spark/jars`
6. crée les répertoires HDFS nécessaires
7. démarre HiveServer2

## Ce que fait `run_pipeline.sh`

Le script [run_pipeline.sh](./run_pipeline.sh) exécute le flux complet :

1. synchronisation des scripts vers `spark-master`
2. split de `spotify_data.csv` en `catalogue.csv` et `audio_features.csv`
3. chargement du `catalogue` dans MySQL
4. ingestion Raw `catalogue` vers HDFS via `feeder.py`
5. ingestion Raw `audio_features` vers HDFS via `feeder.py`
6. création de la couche Silver via `processor.py`
7. création des datamarts Gold via `datamart.py`

## Utilisation depuis PowerShell

Depuis PowerShell, l'invite suivante n'est pas une commande :

```powershell
PS C:\...\spotify-medallion>
```

C'est uniquement le terminal qui attend une commande.

Pour lancer les scripts shell depuis PowerShell :

```powershell
bash setup_env.sh
bash run_pipeline.sh
bash ./tests/check_silver.sh
```

## Exécution manuelle des composants

### 1. Split du dataset

Précondition :

- le fichier `data/spotify_data.csv` doit être présent

Commande locale :

```bash
python3 scripts/split_dataset.py --input data/spotify_data.csv --output ./data
```

Sous Windows, si vous lancez le script depuis PowerShell, évitez `\data`.

Correct :

```powershell
python .\scripts\split_dataset.py --input data\spotify_data.csv --output .\data
```

Incorrect si vous visez le dossier du projet :

```powershell
python .\scripts\split_dataset.py --input data\spotify_data.csv --output \data
```

` \data ` pointe vers `C:\data`, pas vers `.\data`.

### 2. Lancer uniquement l'API et le dashboard

```bash
docker compose up -d --build api dashboard
```

### 3. Vérifier les services Docker

```bash
docker compose ps
docker ps
```

## API Flask

### URL

- Swagger : `http://localhost:8000/docs`
- Healthcheck : `http://localhost:8000/health`

### Authentification

Compte par défaut :

- utilisateur : `admin`
- mot de passe : `spotify123`

### Endpoints principaux

- `POST /auth/login`
- `GET /datamarts/`
- `GET /datamarts/track-popularity`
- `GET /datamarts/genre-trends`
- `GET /datamarts/top-artists`
- `GET /datamarts/hits-emergents`
- `GET /health`

### Exemple de récupération d'un token

```bash
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"spotify123"}'
```

### Exemple d'appel à un datamart

```bash
curl http://localhost:8000/datamarts/track-popularity?page=1&page_size=5 \
  -H "Authorization: Bearer <TOKEN>"
```

## Dashboard Streamlit

URL :

```text
http://localhost:8501
```

Le dashboard lit directement les datamarts MySQL et affiche notamment :

- métriques de synthèse
- popularité moyenne par genre
- tendances audio par décennie
- vues orientées artistes et tracks

## Vérification des données Silver

Le script [tests/check_silver.sh](./tests/check_silver.sh) exécute deux requêtes Hive simples :

- contrôle technique sur `silver.tracks_unified`
- contrôle métier simple par genre

Exécution :

```bash
./tests/check_silver.sh
```

### Vérification Hive manuelle

Exemple :

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SHOW TABLES IN silver;"
```

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM silver.tracks_unified;"
```

## Vérification SQL des datamarts Gold

Connexion interactive :

```bash
docker exec -it mysql mysql -uroot -proot123 spotify_bdf
```

Exemples de requêtes :

```sql
SHOW TABLES;
```

```sql
SELECT COUNT(*) FROM dm_track_popularity;
```

```sql
SELECT genre, ROUND(AVG(popularity), 2) AS avg_pop, COUNT(*) AS nb
FROM dm_track_popularity
GROUP BY genre
ORDER BY nb DESC
LIMIT 10;
```

```sql
SELECT artist_name, decade, influence_score, rank_in_decade
FROM dm_top_artists
ORDER BY influence_score DESC
LIMIT 10;
```

## Logs

### Logs du pipeline Spark

Les fichiers de logs sont écrits dans `spark-master` :

```text
/spark/apps/logs
```

Lister les logs :

```bash
docker exec spark-master ls -la /spark/apps/logs
```

Afficher un log :

```bash
docker exec spark-master cat /spark/apps/logs/processor_YYYYMMDD_HHMMSS.txt
```

Suivre un log en temps réel :

```bash
docker exec -it spark-master tail -f /spark/apps/logs/processor_YYYYMMDD_HHMMSS.txt
```

Copier les logs vers l'hôte :

```bash
mkdir -p logs
docker cp spark-master:/spark/apps/logs/. ./logs/
```

### Logs Docker

```bash
docker compose logs -f api
docker compose logs -f dashboard
docker compose logs -f mysql
```

## Dépannage

### `spark-master` introuvable

Cause probable :

- le stack `docker-hadoop-spark` n'est pas démarré

Correction :

```bash
cd ../docker-hadoop-spark
docker-compose up -d
```

### Le split n'écrit pas dans le dossier attendu

Cause probable :

- sous PowerShell, `\data` pointe vers `C:\data`

Correction :

- utiliser `data` ou `.\data`

### L'API démarre mais ne lit pas MySQL

Vérifications :

- `docker compose ps`
- `http://localhost:8000/health`

Le résultat attendu est :

```json
{"database":"ok","status":"ok","version":"1.0.0"}
```

### Swagger n'affiche pas les datamarts

Faire un refresh forcé du navigateur :

- `Ctrl+F5`

La documentation expose maintenant les routes `/datamarts/...` dans `/docs`.

### Port MySQL

Le conteneur MySQL est exposé sur `3307`, pas `3306`, pour éviter les conflits avec un MySQL local Windows.

## Sécurité et limites

- les identifiants et secrets présents dans ce dépôt sont adaptés à un environnement local de démonstration
- le compte API par défaut `admin / spotify123` ne doit pas être conservé tel quel en production
- le mot de passe root MySQL `root123` est un mot de passe de développement

## Commandes utiles

### Démarrage

```bash
bash setup_env.sh
bash run_pipeline.sh
```

### API / dashboard

```bash
docker compose up -d --build api dashboard
```

### Validation Silver

```bash
./tests/check_silver.sh
```

### Logs

```bash
docker exec spark-master ls -la /spark/apps/logs
docker compose logs -f api dashboard mysql
```

## Résultat attendu

À la fin d'un run réussi :

- HDFS contient les données Raw `catalogue` et `audio_features`
- Hive contient `silver.tracks_unified`, `silver.genre_stats`, `silver.artist_stats`
- MySQL contient les 4 datamarts Gold
- l'API répond sur `http://localhost:8000`
- le dashboard répond sur `http://localhost:8501`
