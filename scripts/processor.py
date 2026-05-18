# -*- coding: utf-8 -*-
"""
processor.py -- Couche SILVER (Hive)
Lit HDFS/Raw, valide, joint, window functions, persist(), ecrit dans Hive.

Utilisation:
    spark-submit \
      --master spark://spark-master:7077 \
      --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
      scripts/processor.py \
      --raw-catalogue  hdfs://namenode:9000/raw/catalogue \
      --raw-audio      hdfs://namenode:9000/raw/audio_features \
      --hive-db        silver \
      --log-dir        /opt/spark/logs
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from utils.logger        import get_logger
from utils.spark_session import create_spark_session
from utils.config        import parse_processor_args


RAW_PARTITION_COLS = ["ingestion_year", "ingestion_month", "ingestion_day"]


def drop_raw_partition_metadata(df: DataFrame, source_name: str, log) -> DataFrame:
    present_cols = [col for col in RAW_PARTITION_COLS if col in df.columns]
    if not present_cols:
        return df

    log.info("[%s] Suppression des colonnes de partition raw : %s", source_name, ", ".join(present_cols))
    return df.drop(*present_cols)


# ─── VALIDATION ──────────────────────────────────────────────
def apply_validation(df_cat: DataFrame, df_audio: DataFrame, log) -> tuple:
    """Applique 6 regles de validation. Logue les rejets par regle."""
    log.info("--- Application des regles de validation ---")

    # R1 : track_id non nul sur les deux sources
    before = df_cat.count()
    df_cat = df_cat.filter(F.col("track_id").isNotNull() & (F.col("track_id") != ""))
    log.info("R1 [catalogue]  track_id non nul    : %d rejetes", before - df_cat.count())

    before = df_audio.count()
    df_audio = df_audio.filter(F.col("track_id").isNotNull() & (F.col("track_id") != ""))
    log.info("R1 [audio]      track_id non nul    : %d rejetes", before - df_audio.count())

    # R2 : Unicite sur track_id
    before = df_cat.count()
    df_cat = df_cat.dropDuplicates(["track_id"])
    log.info("R2 [catalogue]  Unicite track_id    : %d doublons supprimes", before - df_cat.count())

    before = df_audio.count()
    df_audio = df_audio.dropDuplicates(["track_id"])
    log.info("R2 [audio]      Unicite track_id    : %d doublons supprimes", before - df_audio.count())

    # R3 : popularity dans [0, 100]
    before = df_audio.count()
    df_audio = df_audio.filter(F.col("popularity").between(0, 100))
    log.info("R3 [audio]      popularity [0,100]  : %d rejetes", before - df_audio.count())

    # R4 : danceability et energy dans [0, 1]
    before = df_audio.count()
    df_audio = df_audio.filter(
        F.col("danceability").between(0.0, 1.0) &
        F.col("energy").between(0.0, 1.0)
    )
    log.info("R4 [audio]      danceability/energy : %d rejetes", before - df_audio.count())

    # R5 : duration_ms > 30 000
    before = df_cat.count()
    df_cat = df_cat.filter(F.col("duration_ms") > 30000)
    log.info("R5 [catalogue]  duration_ms>30000   : %d rejetes", before - df_cat.count())

    # R6 : integrite referentielle (anti-join)
    ids_cat  = df_cat.select("track_id")
    orphans  = df_audio.join(ids_cat, on="track_id", how="left_anti").count()
    df_audio = df_audio.join(ids_cat, on="track_id", how="inner")
    log.info("R6 [audio]      integrite ref.      : %d orphelins supprimes", orphans)

    log.info("Apres validation : %d catalogue | %d audio", df_cat.count(), df_audio.count())
    return df_cat, df_audio


# ─── JOINTURE + PERSIST ───────────────────────────────────────
def join_and_persist(df_cat: DataFrame, df_audio: DataFrame, log) -> DataFrame:
    """Inner join sur track_id + persist(MEMORY_AND_DISK)."""
    log.info("Jointure : catalogue inner join audio_features sur track_id...")
    df_unified = df_cat.join(df_audio, on="track_id", how="inner")
    df_unified = df_unified.persist(StorageLevel.MEMORY_AND_DISK)
    count = df_unified.count()  # force la materialisation
    log.info("persist(MEMORY_AND_DISK) materialise : %d lignes (voir Spark UI > Storage)", count)
    return df_unified


# ─── WINDOW FUNCTIONS + ENRICHISSEMENTS ──────────────────────
def apply_windows_and_enrich(df: DataFrame, log) -> DataFrame:
    """Window 1: rank par genre. Window 2: lag par artiste/annee."""
    log.info("Application des window functions...")

    # Window 1 : rank de popularite dans chaque genre
    w_genre = Window.partitionBy("genre").orderBy(F.col("popularity").desc())
    df = df.withColumn("rank_in_genre", F.rank().over(w_genre))
    log.info("  rank() par genre applique")

    # Window 2 : lag popularite par artiste x annee
    w_artist = Window.partitionBy("artist_name").orderBy("year")
    df = df.withColumn("pop_prev_year", F.lag("popularity", 1).over(w_artist))
    log.info("  lag() popularite annee precedente par artiste applique")

    # Colonnes calculees
    df = (
        df
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("integer"))
        .withColumn("duration_sec", F.round(F.col("duration_ms") / 1000.0, 1))
        .withColumn(
            "popularity_category",
            F.when(F.col("popularity") >= 70, "High")
             .when(F.col("popularity") >= 40, "Medium")
             .otherwise("Low")
        )
        .withColumn(
            "is_acoustic",
            F.when(F.col("acousticness") >= 0.5, True).otherwise(False)
        )
    )
    log.info("  Colonnes calculees : decade, duration_sec, popularity_category, is_acoustic")
    return df


# ─── AGREGATIONS SILVER ───────────────────────────────────────
def build_genre_stats(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("genre", "year", "decade")
        .agg(
            F.round(F.avg("popularity"),   2).alias("avg_popularity"),
            F.count("track_id").alias("nb_tracks"),
            F.round(F.avg("tempo"),        2).alias("avg_tempo"),
            F.round(F.avg("energy"),       4).alias("avg_energy"),
            F.round(F.avg("danceability"), 4).alias("avg_danceability"),
            F.round(F.avg("valence"),      4).alias("avg_valence"),
        )
        .orderBy("genre", "year")
    )


def build_artist_stats(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("artist_name", "decade")
        .agg(
            F.count("track_id").alias("total_tracks"),
            F.round(F.avg("popularity"), 2).alias("avg_popularity"),
            F.first("genre").alias("main_genre"),
        )
        .orderBy("decade", F.col("avg_popularity").desc())
    )


# ─── ECRITURE HIVE ───────────────────────────────────────────
def write_to_hive(df: DataFrame, hive_db: str, table: str, mode: str, log) -> None:
    full_table = f"{hive_db}.{table}"
    count = df.count()
    log.info("Ecriture Hive : %s (mode=%s) -- %d lignes", full_table, mode, count)
    df.write.mode(mode).format("hive").saveAsTable(full_table)
    log.info("  Table Hive %s creee/mise a jour", full_table)


# ─── POINT D'ENTREE ───────────────────────────────────────────
def main():
    args = parse_processor_args()
    log  = get_logger("processor", args.log_dir)

    log.info("=" * 60)
    log.info("PROCESSOR demarre")
    log.info("  Raw catalogue : %s", args.raw_catalogue)
    log.info("  Raw audio     : %s", args.raw_audio)
    log.info("  Hive DB       : %s", args.hive_db)
    log.info("  Mode          : %s", args.mode)
    log.info("=" * 60)

    spark = None
    start = datetime.now()

    try:
        spark = create_spark_session(
            app_name="Spotify-Processor-Silver",
            hdfs_namenode=args.hdfs_namenode,
            enable_hive=True,
            extra_conf={
                "spark.sql.warehouse.dir": f"{args.hdfs_namenode}/user/hive/warehouse",
            },
        )

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.hive_db}")
        log.info("Base Hive '%s' prete.", args.hive_db)

        log.info("Lecture HDFS/Raw...")
        df_cat   = spark.read.parquet(args.raw_catalogue)
        df_audio = spark.read.parquet(args.raw_audio)
        df_cat   = drop_raw_partition_metadata(df_cat, "catalogue", log)
        df_audio = drop_raw_partition_metadata(df_audio, "audio_features", log)
        log.info("  catalogue      : %d lignes", df_cat.count())
        log.info("  audio_features : %d lignes", df_audio.count())

        df_cat, df_audio = apply_validation(df_cat, df_audio, log)
        df_unified       = join_and_persist(df_cat, df_audio, log)
        df_enriched      = apply_windows_and_enrich(df_unified, log)
        df_genre         = build_genre_stats(df_enriched)
        df_artist        = build_artist_stats(df_enriched)

        write_to_hive(df_enriched, args.hive_db, "tracks_unified", args.mode, log)
        write_to_hive(df_genre,    args.hive_db, "genre_stats",    args.mode, log)
        write_to_hive(df_artist,   args.hive_db, "artist_stats",   args.mode, log)

        df_unified.unpersist()
        log.info("persist() libere.")

        duration = (datetime.now() - start).total_seconds()
        log.info("=" * 60)
        log.info("PROCESSOR termine avec succes | duree : %.1fs", duration)
        log.info("Tables Hive : %s.tracks_unified | genre_stats | artist_stats", args.hive_db)
        log.info("=" * 60)

    except Exception as exc:
        log.error("ERREUR CRITIQUE processor : %s", exc, exc_info=True)
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            log.info("SparkSession fermee.")


if __name__ == "__main__":
    main()
