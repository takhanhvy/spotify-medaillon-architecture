#!/bin/bash
# ─────────────────────────────────────────────────────────────
#  run_pipeline.sh — Pipeline Spotify Medallion
#
#  Prerequis : bash setup_env.sh (a lancer une fois avant)
# ─────────────────────────────────────────────────────────────
set -e

HDFS_NAMENODE="hdfs://namenode:9000"
MYSQL_URL="jdbc:mysql://mysql:3306/spotify_bdf"
MYSQL_USER="root"
MYSQL_PASS="root123"

SCRIPTS_DIR="/spark/apps/scripts"
DATA_DIR="/spark/apps/data"
LOG_DIR="/spark/apps/logs"
MYSQL_JAR="/spark/jars/mysql-connector-j-8.0.33.jar"

SPARK_SUBMIT="/spark/bin/spark-submit"
SPARK_MASTER="spark://spark-master:7077"

# Detecter le bon executable Python 3 dans spark-master
PYTHON3=$(docker exec spark-master bash -c "
  for p in /usr/bin/python3.7 /usr/bin/python3.8 /usr/bin/python3 /usr/local/bin/python3; do
    [ -x \"\$p\" ] && echo \"\$p\" && break
  done
")

if [ -z "$PYTHON3" ]; then
  echo "  ERREUR : Aucun Python 3 trouve dans spark-master !"
  exit 1
fi
echo "  Python detecte : $PYTHON3"

SPARK_CONF="--conf spark.hadoop.fs.defaultFS=$HDFS_NAMENODE \
  --conf spark.sql.warehouse.dir=$HDFS_NAMENODE/user/hive/warehouse \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.pyspark.python=$PYTHON3 \
  --conf spark.pyspark.driver.python=$PYTHON3"

# spark-submit wrapper : injecte PYSPARK_PYTHON + PYSPARK_DRIVER_PYTHON
# pour garantir que driver ET executors utilisent Python 3
SPARK_RUN="docker exec -e PYSPARK_PYTHON=$PYTHON3 -e PYSPARK_DRIVER_PYTHON=$PYTHON3 spark-master $SPARK_SUBMIT"

echo "======================================================"
echo "  SPOTIFY MEDALLION PIPELINE"
echo "======================================================"

# ── Sync scripts -> spark-master ──────────────────────────────
echo ""
echo "[Prep] Synchronisation des scripts vers spark-master..."
docker exec spark-master mkdir -p $SCRIPTS_DIR
docker cp ./scripts/. spark-master:$SCRIPTS_DIR/
echo "[Prep] Scripts synchronises ✓"

# ── [0] Split du dataset ──────────────────────────────────────
echo ""
echo "[0/4] SPLIT — Decoupe de spotify_data.csv..."
docker exec spark-master $PYTHON3 $SCRIPTS_DIR/split_dataset.py \
  --input  $DATA_DIR/spotify_data.csv \
  --output $DATA_DIR/
echo "[0/4] Split termine ✓"

# ── [0b] Chargement catalogue.csv dans MySQL ──────────────────
echo ""
echo "[0b/4] LOAD — catalogue.csv -> MySQL (table catalogue)..."
# docker cp ne supporte pas container->container : on passe par l'hote
docker cp spark-master:/spark/apps/data/catalogue.csv ./data/catalogue.csv
docker cp ./data/catalogue.csv mysql:/var/lib/mysql-files/catalogue.csv
# Vider la table et recharger
docker exec mysql mysql -uroot -proot123 spotify_bdf -e "
  TRUNCATE TABLE catalogue;
  LOAD DATA INFILE '/var/lib/mysql-files/catalogue.csv'
  INTO TABLE catalogue
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '\"'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES
  (@track_id, @track_name, @artist_name, @genre, @year, @duration_ms)
  SET track_id    = @track_id,
      track_name  = SUBSTR(@track_name,  1, 500),
      artist_name = SUBSTR(@artist_name, 1, 500),
      genre       = NULLIF(@genre, ''),
      year        = IF(@year REGEXP '^[0-9]+\$' AND @year+0 BETWEEN 1000 AND 9999, @year+0, NULL),
      duration_ms = IF(@duration_ms REGEXP '^[0-9]+\$', @duration_ms+0, NULL);
  SELECT CONCAT('  catalogue charge : ', COUNT(*), ' lignes') AS status FROM catalogue;
"
echo "[0b/4] Chargement MySQL termine ✓"

# ── [1a] Feeder — catalogue MySQL -> HDFS ─────────────────────
echo ""
echo "[1a/4] FEEDER — Catalogue (MySQL -> HDFS/raw)..."
$SPARK_RUN \
  --master $SPARK_MASTER \
  --jars   $MYSQL_JAR \
  $SPARK_CONF \
  $SCRIPTS_DIR/feeder.py \
  --source         catalogue \
  --output-path    $HDFS_NAMENODE/raw/catalogue \
  --mysql-url      $MYSQL_URL \
  --mysql-user     $MYSQL_USER \
  --mysql-password $MYSQL_PASS \
  --log-dir        $LOG_DIR

echo "[1a/4] Feeder catalogue ✓"
echo "  Verification HDFS raw/catalogue :"
docker exec namenode hdfs dfs -ls -R /raw/catalogue/ 2>/dev/null | head -20 || echo "  AVERTISSEMENT : /raw/catalogue vide ou inaccessible"

# ── [1b] Feeder — audio_features CSV -> HDFS ──────────────────
echo ""
echo "[1b/4] FEEDER — Audio features (CSV -> HDFS/raw)..."

$SPARK_RUN \
  --master $SPARK_MASTER \
  $SPARK_CONF \
  $SCRIPTS_DIR/feeder.py \
  --source       audio_features \
  --input-path   $DATA_DIR/audio_features.csv \
  --output-path  $HDFS_NAMENODE/raw/audio_features \
  --log-dir      $LOG_DIR

echo "[1b/4] Feeder audio_features ✓"
echo "  Verification HDFS raw/audio_features :"
docker exec namenode hdfs dfs -ls -R /raw/audio_features/ 2>/dev/null | head -20 || echo "  AVERTISSEMENT : /raw/audio_features vide ou inaccessible"

# ── [2] Processor — HDFS/raw -> Hive/silver ───────────────────
echo ""
echo "[2/4] PROCESSOR — Raw -> Hive Silver..."
$SPARK_RUN \
  --master $SPARK_MASTER \
  $SPARK_CONF \
  $SCRIPTS_DIR/processor.py \
  --raw-catalogue  $HDFS_NAMENODE/raw/catalogue \
  --raw-audio      $HDFS_NAMENODE/raw/audio_features \
  --hive-db        silver \
  --log-dir        $LOG_DIR
echo "[2/4] Processor ✓"

# ── [3] Datamart — Hive/silver -> MySQL/gold ──────────────────
echo ""
echo "[3/4] DATAMART — Hive Silver -> MySQL Gold..."
$SPARK_RUN \
  --master $SPARK_MASTER \
  --jars   $MYSQL_JAR \
  $SPARK_CONF \
  $SCRIPTS_DIR/datamart.py \
  --hive-db        silver \
  --mysql-url      $MYSQL_URL \
  --mysql-user     $MYSQL_USER \
  --mysql-password $MYSQL_PASS \
  --log-dir        $LOG_DIR
echo "[3/4] Datamart ✓"

echo ""
echo "======================================================"
echo "  PIPELINE TERMINE"
echo "  HDFS UI        : http://localhost:9870"
echo "  YARN UI        : http://localhost:8088"
echo "  Spark UI       : http://localhost:8080"
echo "  API Swagger    : http://localhost:8000/docs"
echo "  Dashboard      : http://localhost:8501"
echo "======================================================"
