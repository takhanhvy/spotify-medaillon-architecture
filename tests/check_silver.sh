#!/bin/bash
set -euo pipefail

BEELINE_CMD=(
  docker exec hive-server
  beeline
  -u
  jdbc:hive2://localhost:10000
)

echo "[1/2] Verification technique de silver.tracks_unified..."
"${BEELINE_CMD[@]}" -e "
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT track_id) AS distinct_tracks,
  SUM(CASE WHEN track_id IS NULL OR track_id = '' THEN 1 ELSE 0 END) AS null_track_id,
  SUM(CASE WHEN popularity < 0 OR popularity > 100 THEN 1 ELSE 0 END) AS invalid_popularity
FROM silver.tracks_unified;
"

echo
echo "[2/2] Verification metier simple par genre..."
"${BEELINE_CMD[@]}" -e "
SELECT
  genre,
  COUNT(*) AS nb_tracks,
  ROUND(AVG(popularity), 2) AS avg_pop
FROM silver.tracks_unified
GROUP BY genre
ORDER BY nb_tracks DESC
LIMIT 10;
"
