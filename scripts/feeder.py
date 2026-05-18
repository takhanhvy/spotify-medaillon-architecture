# -*- coding: utf-8 -*-
"""
feeder.py - Couche RAW (Bronze)
Ingestion vers HDFS/Raw en Parquet partitionne par date d'ingestion.
"""

import sys
import os
from datetime import datetime, date
from urllib.parse import urlparse
from uuid import uuid4

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, ShortType, IntegerType, DoubleType
)

from utils.logger import get_logger
from utils.spark_session import create_spark_session
from utils.config import parse_feeder_args


SCHEMA_CATALOGUE = StructType([
    StructField("track_id",    StringType(),  False),
    StructField("track_name",  StringType(),  False),
    StructField("artist_name", StringType(),  False),
    StructField("genre",       StringType(),  True),
    StructField("year",        ShortType(),   True),
    StructField("duration_ms", IntegerType(), True),
])

SCHEMA_AUDIO = StructType([
    StructField("track_id",         StringType(),  False),
    StructField("danceability",     DoubleType(),  True),
    StructField("energy",           DoubleType(),  True),
    StructField("key",              IntegerType(), True),
    StructField("loudness",         DoubleType(),  True),
    StructField("mode",             IntegerType(), True),
    StructField("speechiness",      DoubleType(),  True),
    StructField("acousticness",     DoubleType(),  True),
    StructField("instrumentalness", DoubleType(),  True),
    StructField("liveness",         DoubleType(),  True),
    StructField("valence",          DoubleType(),  True),
    StructField("tempo",            DoubleType(),  True),
    StructField("time_signature",   IntegerType(), True),
    StructField("popularity",       IntegerType(), True),
])


def _is_distributed_path(path: str) -> bool:
    scheme = urlparse(path).scheme.lower()
    return scheme not in ("", "file")


def _normalize_local_path(path: str) -> str:
    parsed = urlparse(path)
    if parsed.scheme.lower() == "file":
        return parsed.path
    return os.path.abspath(path)


def stage_local_csv_to_hdfs(spark, local_path, hdfs_namenode, log):
    resolved_local_path = _normalize_local_path(local_path)
    if not os.path.isfile(resolved_local_path):
        log.error("CSV local introuvable : %s", resolved_local_path)
        sys.exit(1)

    stage_name = f"{uuid4().hex}_{os.path.basename(resolved_local_path)}"
    hdfs_stage_path = f"{hdfs_namenode.rstrip('/')}/tmp/feeder_staging/{stage_name}"

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    dst = jvm.org.apache.hadoop.fs.Path(hdfs_stage_path)
    fs = dst.getFileSystem(conf)
    fs.mkdirs(dst.getParent())
    fs.copyFromLocalFile(
        False,
        True,
        jvm.org.apache.hadoop.fs.Path(f"file://{resolved_local_path}"),
        dst,
    )
    log.info("CSV local stage vers HDFS : %s -> %s", resolved_local_path, hdfs_stage_path)
    return hdfs_stage_path


def cleanup_hdfs_staging_path(spark, staged_path, log):
    if not staged_path:
        return

    try:
        conf = spark._jsc.hadoopConfiguration()
        path = spark._jvm.org.apache.hadoop.fs.Path(staged_path)
        fs = path.getFileSystem(conf)
        if fs.delete(path, True):
            log.info("Nettoyage staging HDFS : %s", staged_path)
        else:
            log.warning("Nettoyage staging HDFS ignore : %s", staged_path)
    except Exception as exc:
        log.warning("Impossible de supprimer le staging HDFS %s : %s", staged_path, exc)


def read_catalogue_from_mysql(spark, args, log):
    if not all([args.mysql_url, args.mysql_user, args.mysql_password]):
        log.error("--mysql-url, --mysql-user et --mysql-password sont requis pour --source catalogue")
        sys.exit(1)
    log.info("Lecture du catalogue depuis MySQL : %s", args.mysql_url)
    df = (
        spark.read
        .format("jdbc")
        .option("url",      args.mysql_url)
        .option("dbtable",  "catalogue")
        .option("user",     args.mysql_user)
        .option("password", args.mysql_password)
        .option("driver",   "com.mysql.cj.jdbc.Driver")
        .load()
    )
    count = df.count()
    log.info("  Catalogue charge depuis MySQL : %d lignes", count)
    return df


def read_audio_features_from_csv(spark, args, log):
    if not args.input_path:
        log.error("--input-path est requis pour --source audio_features")
        sys.exit(1)

    source_path = args.input_path
    staged_path = None
    if _is_distributed_path(args.input_path):
        log.info("Lecture des audio_features depuis CSV distribue : %s", args.input_path)
    else:
        staged_path = stage_local_csv_to_hdfs(spark, args.input_path, args.hdfs_namenode, log)
        source_path = staged_path
        log.info("Lecture des audio_features depuis CSV local stage sur HDFS : %s", source_path)

    df = (
        spark.read
        .option("header", "true")
        .schema(SCHEMA_AUDIO)
        .csv(source_path)
    )
    count = df.count()
    log.info("  audio_features charge depuis CSV : %d lignes", count)
    return df, staged_path


def add_ingestion_partition(df, execution_date, log):
    if execution_date:
        d = datetime.strptime(execution_date, "%Y-%m-%d").date()
    else:
        d = date.today()
    df = (
        df
        .withColumn("ingestion_year",  F.lit(d.year))
        .withColumn("ingestion_month", F.lit(d.month))
        .withColumn("ingestion_day",   F.lit(d.day))
    )
    log.info("Partition ajoutee : %d/%02d/%02d", d.year, d.month, d.day)
    return df


def write_to_hdfs(df, output_path, mode, log):
    log.info("Ecriture Parquet (mode=%s) vers HDFS : %s", mode, output_path)
    (
        df.write
        .mode(mode)
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day")
        .parquet(output_path)
    )
    log.info("  OK - donnees ecrites dans %s", output_path)


def main():
    args = parse_feeder_args()
    log = get_logger("feeder_%s" % args.source, args.log_dir)
    log.info("=== FEEDER START : source=%s ===", args.source)
    log.info("  output_path = %s", args.output_path)

    spark = create_spark_session(
        app_name="Feeder_%s" % args.source,
        hdfs_namenode=args.hdfs_namenode,
        enable_hive=False,
    )

    staged_input_path = None

    try:
        if args.source == "catalogue":
            df = read_catalogue_from_mysql(spark, args, log)
        else:
            df, staged_input_path = read_audio_features_from_csv(spark, args, log)

        df = add_ingestion_partition(df, args.execution_date, log)
        write_to_hdfs(df, args.output_path, args.mode, log)

        log.info("=== FEEDER DONE : source=%s ===", args.source)
    finally:
        cleanup_hdfs_staging_path(spark, staged_input_path, log)
        spark.stop()


if __name__ == "__main__":
    main()
