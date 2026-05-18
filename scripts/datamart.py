# -*- coding: utf-8 -*-
"""
datamart.py -- Couche GOLD (MySQL)
Lit Hive/Silver, construit 4 datamarts, ecrit dans MySQL via JDBC.

Utilisation:
    spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar \
      --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
      scripts/datamart.py \
      --hive-db        silver \
      --mysql-url      jdbc:mysql://mysql:3306/spotify_bdf \
      --mysql-user     root \
      --mysql-password root123 \
      --log-dir        /opt/spark/logs
"""

import sys
import os
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from utils.logger        import get_logger
from utils.spark_session import create_spark_session
from utils.config        import parse_datamart_args


# ─── LECTURE SILVER ───────────────────────────────────────────
def read_silver(spark: SparkSession, hive_db: str, log) -> DataFrame:
    """Lit la table silver.tracks_unified depuis Hive."""
    table = f"{hive_db}.tracks_unified"
    log.info("Lecture Hive : %s", table)
    df = spark.table(table)
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    count = df.count()
    log.info("persist(MEMORY_AND_DISK) materialise : %d lignes", count)
    return df


# ─── DM1 : dm_track_popularity ────────────────────────────────
def build_dm_track_popularity(df: DataFrame, log) -> DataFrame:
    """
    DM1 — Une ligne par track avec features audio + rang dans son genre.
    Colonnes: track_id, track_name, artist_name, genre, year, popularity,
              danceability, energy, valence, tempo, acousticness,
              instrumentalness, duration_ms, rank_in_genre,
              popularity_category, ingestion_date
    """
    log.info("Construction DM1 : dm_track_popularity...")
    today = date.today().isoformat()

    dm = (
        df.select(
            "track_id", "track_name", "artist_name", "genre", "year",
            "popularity", "danceability", "energy", "valence", "tempo",
            "acousticness", "instrumentalness", "duration_ms",
            "rank_in_genre", "popularity_category"
        )
        .withColumn("ingestion_date", F.lit(today).cast("date"))
    )
    log.info("  DM1 : %d lignes", dm.count())
    return dm


# ─── DM2 : dm_genre_trends ────────────────────────────────────
def build_dm_genre_trends(df: DataFrame, log) -> DataFrame:
    """
    DM2 — Tendances par genre x decennie + top_track_name.
    Colonnes: genre, decade, avg_popularity, nb_tracks,
              avg_danceability, avg_energy, top_track_name, ingestion_date
    """
    log.info("Construction DM2 : dm_genre_trends...")
    today = date.today().isoformat()

    # Agregations de base
    agg = (
        df.groupBy("genre", "decade")
        .agg(
            F.round(F.avg("popularity"),   2).alias("avg_popularity"),
            F.count("track_id").alias("nb_tracks"),
            F.round(F.avg("danceability"), 4).alias("avg_danceability"),
            F.round(F.avg("energy"),       4).alias("avg_energy"),
        )
    )

    # Top track par genre x decade (meilleure popularite)
    w_top = Window.partitionBy("genre", "decade").orderBy(F.col("popularity").desc())
    top_tracks = (
        df.withColumn("rn", F.row_number().over(w_top))
        .filter(F.col("rn") == 1)
        .select(
            F.col("genre").alias("g"),
            F.col("decade").alias("d"),
            F.col("track_name").alias("top_track_name")
        )
    )

    dm = (
        agg.join(
            top_tracks,
            (agg["genre"] == top_tracks["g"]) & (agg["decade"] == top_tracks["d"]),
            how="left"
        )
        .drop("g", "d")
        .withColumn("ingestion_date", F.lit(today).cast("date"))
        .orderBy("genre", "decade")
    )
    log.info("  DM2 : %d lignes", dm.count())
    return dm


# ─── DM3 : dm_top_artists ─────────────────────────────────────
def build_dm_top_artists(df: DataFrame, log) -> DataFrame:
    """
    DM3 — Top artistes par decennie avec influence_score et rang.
    Colonnes: artist_name, decade, total_tracks, avg_popularity,
              max_popularity, main_genre, influence_score, rank_in_decade,
              ingestion_date
    influence_score = avg_popularity * log1p(total_tracks)
    """
    log.info("Construction DM3 : dm_top_artists...")
    today = date.today().isoformat()

    agg = (
        df.groupBy("artist_name", "decade")
        .agg(
            F.count("track_id").alias("total_tracks"),
            F.round(F.avg("popularity"), 2).alias("avg_popularity"),
            F.round(F.max("popularity"), 2).alias("max_popularity"),
            F.first("genre").alias("main_genre"),
        )
        .withColumn(
            "influence_score",
            F.round(F.col("avg_popularity") * F.log1p(F.col("total_tracks").cast("double")), 2)
        )
    )

    w_rank = Window.partitionBy("decade").orderBy(F.col("influence_score").desc())
    dm = (
        agg.withColumn("rank_in_decade", F.rank().over(w_rank))
        .withColumn("ingestion_date", F.lit(today).cast("date"))
        .orderBy("decade", "rank_in_decade")
    )
    log.info("  DM3 : %d lignes", dm.count())
    return dm


# ─── DM4 : dm_hits_emergents ──────────────────────────────────
def build_dm_hits_emergents(df: DataFrame, log) -> DataFrame:
    """
    DM4 — Top 10 tracks par genre x annee (hits emergents).
    Colonnes: genre, year, track_id, track_name, artist_name,
              popularity, danceability, energy, valence, tempo,
              rank_in_year, ingestion_date
    """
    log.info("Construction DM4 : dm_hits_emergents...")
    today = date.today().isoformat()

    w_year = Window.partitionBy("genre", "year").orderBy(F.col("popularity").desc())
    dm = (
        df.withColumn("rank_in_year", F.rank().over(w_year))
        .filter(F.col("rank_in_year") <= 10)
        .select(
            "genre", "year", "track_id", "track_name", "artist_name",
            "popularity", "danceability", "energy", "valence", "tempo",
            "rank_in_year"
        )
        .withColumn("ingestion_date", F.lit(today).cast("date"))
        .orderBy("genre", "year", "rank_in_year")
    )
    log.info("  DM4 : %d lignes", dm.count())
    return dm


# ─── ECRITURE MYSQL ───────────────────────────────────────────
def write_to_mysql(df: DataFrame, table: str, mysql_url: str,
                   mysql_user: str, mysql_password: str, mode: str, log) -> None:
    count = df.count()
    log.info("Ecriture MySQL : %s (mode=%s) -- %d lignes", table, mode, count)
    (
        df.write
        .format("jdbc")
        .option("url",      mysql_url)
        .option("dbtable",  table)
        .option("user",     mysql_user)
        .option("password", mysql_password)
        .option("driver",   "com.mysql.cj.jdbc.Driver")
        .mode(mode)
        .save()
    )
    log.info("  Table MySQL %s ecrite.", table)


# ─── POINT D'ENTREE ───────────────────────────────────────────
def main():
    args = parse_datamart_args()
    log  = get_logger("datamart", args.log_dir)

    log.info("=" * 60)
    log.info("DATAMART demarre")
    log.info("  Hive DB   : %s", args.hive_db)
    log.info("  MySQL URL : %s", args.mysql_url)
    log.info("  Mode      : %s", args.mode)
    log.info("=" * 60)

    spark = None
    start = datetime.now()

    try:
        spark = create_spark_session(
            app_name="Spotify-Datamart-Gold",
            hdfs_namenode=args.hdfs_namenode,
            enable_hive=True,
            extra_conf={
                "spark.sql.warehouse.dir": f"{args.hdfs_namenode}/user/hive/warehouse",
            },
        )

        df_silver = read_silver(spark, args.hive_db, log)

        dm1 = build_dm_track_popularity(df_silver, log)
        dm2 = build_dm_genre_trends(df_silver, log)
        dm3 = build_dm_top_artists(df_silver, log)
        dm4 = build_dm_hits_emergents(df_silver, log)

        write_to_mysql(dm1, "dm_track_popularity", args.mysql_url, args.mysql_user, args.mysql_password, args.mode, log)
        write_to_mysql(dm2, "dm_genre_trends",     args.mysql_url, args.mysql_user, args.mysql_password, args.mode, log)
        write_to_mysql(dm3, "dm_top_artists",      args.mysql_url, args.mysql_user, args.mysql_password, args.mode, log)
        write_to_mysql(dm4, "dm_hits_emergents",   args.mysql_url, args.mysql_user, args.mysql_password, args.mode, log)

        df_silver.unpersist()
        log.info("persist() libere.")

        duration = (datetime.now() - start).total_seconds()
        log.info("=" * 60)
        log.info("DATAMART termine avec succes | duree : %.1fs", duration)
        log.info("Tables Gold : dm_track_popularity | dm_genre_trends | dm_top_artists | dm_hits_emergents")
        log.info("=" * 60)

    except Exception as exc:
        log.error("ERREUR CRITIQUE datamart : %s", exc, exc_info=True)
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            log.info("SparkSession fermee.")


if __name__ == "__main__":
    main()
