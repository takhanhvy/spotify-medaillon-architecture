# -*- coding: utf-8 -*-
"""
utils/spark_session.py - Factory SparkSession
Configure pour HDFS (namenode:9000), Hive (enableHiveSupport), et MySQL JDBC.
"""

import logging
from pyspark.sql import SparkSession


def create_spark_session(app_name, hdfs_namenode="hdfs://namenode:9000",
                         enable_hive=False, extra_conf=None):
    """
    Cree et retourne une SparkSession.

    Args:
        app_name      : Nom affiche dans la Spark UI
        hdfs_namenode : URI du namenode HDFS
        enable_hive   : Active enableHiveSupport() pour ecrire dans Hive
        extra_conf    : Dict de configs Spark supplementaires
    """
    log = logging.getLogger("spark_session")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.defaultFS", hdfs_namenode)
        .config("spark.sql.parquet.compression.codec",   "snappy")
        .config("spark.sql.parquet.filterPushdown",      "true")
        .config("spark.sql.parquet.mergeSchema",         "false")
        .config("spark.sql.shuffle.partitions",          "50")
        .config("spark.sql.adaptive.enabled",            "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if enable_hive:
        builder = builder.enableHiveSupport()
        log.info("HiveSupport active")

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)
            log.info("Config Spark : %s = %s", key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession creee : %s | Spark %s", app_name, spark.version)
    return spark
