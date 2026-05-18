#!/bin/bash
# ─────────────────────────────────────────────────────────────
#  setup_env.sh — Preparation de l'environnement Spotify Medallion
#
#  Prerequis : Marcel-Jan/docker-hadoop-spark tourne deja.
#  Ce script :
#    1. Telecharge le JAR MySQL si absent
#    2. Lance notre docker-compose (MySQL + API + Dashboard)
#    3. Copie les scripts dans spark-master
#    4. Copie le JAR MySQL dans spark-master
#    5. Cree les repertoires HDFS
#    6. Demarre HiveServer2
# ─────────────────────────────────────────────────────────────
set -e

MYSQL_JAR_LOCAL="./jars/mysql-connector-j-8.0.33.jar"
MYSQL_JAR_URL="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"
HADOOP_NETWORK="docker-hadoop-spark_default"

echo "======================================================"
echo "  SETUP ENVIRONNEMENT — Spotify Medallion"
echo "======================================================"

# ── Verification que Marcel-Jan tourne ───────────────────────
echo ""
echo "[Check] Verification que spark-master tourne..."
if ! docker ps --format '{{.Names}}' | grep -q "^spark-master$"; then
  echo "  ERREUR : spark-master n'est pas en cours d'execution."
  echo "  Lancer d'abord : cd docker-hadoop-spark && docker-compose up -d"
  exit 1
fi
echo "  spark-master OK ✓"

# ── 1. JAR MySQL ─────────────────────────────────────────────
echo ""
echo "[1/6] Verification du JAR MySQL Connector..."
mkdir -p ./jars
if [ ! -f "$MYSQL_JAR_LOCAL" ]; then
  echo "  Telechargement mysql-connector-j-8.0.33.jar..."
  curl -L -o "$MYSQL_JAR_LOCAL" "$MYSQL_JAR_URL"
  echo "  JAR telecharge ✓"
else
  echo "  JAR deja present ✓"
fi

# ── 2. Docker-compose MySQL + API + Dashboard ─────────────────
echo ""
echo "[2/6] Demarrage MySQL + API + Dashboard..."
docker-compose down --remove-orphans 2>/dev/null || true
docker-compose up -d

echo "  Attente MySQL healthy..."
for i in $(seq 1 24); do
  STATUS=$(docker inspect --format='{{.State.Health.Status}}' mysql 2>/dev/null || echo "starting")
  [ "$STATUS" = "healthy" ] && echo "  MySQL healthy ✓" && break
  echo "  ... ($i/24) $STATUS"
  sleep 5
done

# ── 3. Copie des scripts dans spark-master ───────────────────
echo ""
echo "[3/6] Copie des scripts dans spark-master..."
docker exec spark-master mkdir -p /spark/apps/scripts /spark/apps/data /spark/apps/logs
docker cp ./scripts/. spark-master:/spark/apps/scripts/
docker cp ./data/spotify_data.csv spark-master:/spark/apps/data/ 2>/dev/null \
  || echo "  ATTENTION : spotify_data.csv absent de data/ — a copier manuellement"
echo "  Scripts copies ✓"

# ── 4. JAR MySQL dans spark-master ───────────────────────────
echo ""
echo "[4/6] Copie du JAR MySQL dans spark-master..."
docker exec spark-master mkdir -p /spark/jars
docker cp "$MYSQL_JAR_LOCAL" spark-master:/spark/jars/mysql-connector-j-8.0.33.jar
echo "  JAR copie dans spark-master ✓"

# ── 5. Repertoires HDFS ───────────────────────────────────────
echo ""
echo "[5/6] Creation des repertoires HDFS..."
docker exec namenode hdfs dfs -mkdir -p /raw/catalogue
docker exec namenode hdfs dfs -mkdir -p /raw/audio_features
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 777 /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 777 /raw
echo "  Repertoires HDFS crees ✓"

# ── 6. HiveServer2 ────────────────────────────────────────────
echo ""
echo "[6/6] Demarrage de HiveServer2..."
docker exec -d hive-server bash -c "hiveserver2 > /tmp/hiveserver2.log 2>&1"
echo "  Attente 30s..."
sleep 30
docker exec hive-server bash -c "ss -tlnp 2>/dev/null | grep 10000 || netstat -an 2>/dev/null | grep 10000" \
  && echo "  HiveServer2 pret sur port 10000 ✓" \
  || echo "  ATTENTION : HiveServer2 pas encore pret (verifier : docker logs hive-server)"

echo ""
echo "======================================================"
echo "  SETUP TERMINE"
echo "  Lancer le pipeline : bash run_pipeline.sh"
echo "  API Swagger        : http://localhost:8000/docs"
echo "  Dashboard          : http://localhost:8501"
echo "======================================================"
