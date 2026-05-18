# -*- coding: utf-8 -*-
"""
utils/config.py — Parsing des arguments CLI (argparse)
========================================================
Chaque script importe parse_feeder_args(), parse_processor_args()
ou parse_datamart_args() selon son rôle.
Aucun chemin n'est codé en dur — tout passe par les arguments.
"""

import argparse


# ─────────────────────────────────────────────────────────────
#  Arguments communs (partagés par tous les scripts)
# ─────────────────────────────────────────────────────────────
def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-dir",
        dest="log_dir",
        required=True,
        help="Répertoire de sortie des logs .txt (ex: /opt/spark/logs)",
    )
    parser.add_argument(
        "--execution-date",
        dest="execution_date",
        default=None,
        help="Date d'exécution au format YYYY-MM-DD (défaut : aujourd'hui)",
    )
    parser.add_argument(
        "--hdfs-namenode",
        dest="hdfs_namenode",
        default="hdfs://namenode:9000",
        help="URI du namenode HDFS (défaut: hdfs://namenode:9000)",
    )


# ─────────────────────────────────────────────────────────────
#  feeder.py
# ─────────────────────────────────────────────────────────────
def parse_feeder_args() -> argparse.Namespace:
    """
    Arguments de feeder.py.

    spark-submit feeder.py
      --source         catalogue|audio_features
      --input-path     /data/catalogue.csv  (chemin local ou HDFS)
      --output-path    hdfs://namenode:9000/raw/catalogue
      --mysql-url      jdbc:mysql://mysql:3306/spotify_bdf
      --mysql-user     root
      --mysql-password root123
      --log-dir        /opt/spark/logs
    """
    parser = argparse.ArgumentParser(description="Feeder — Ingestion vers HDFS/Raw")
    _add_common_args(parser)

    parser.add_argument(
        "--source",
        required=True,
        choices=["catalogue", "audio_features"],
        help="Source à ingérer : 'catalogue' (MySQL) ou 'audio_features' (CSV)",
    )
    parser.add_argument(
        "--input-path",
        dest="input_path",
        default=None,
        help="Chemin vers le CSV source (local ou HDFS). Pour un chemin local, feeder.py le stage automatiquement vers HDFS.",
    )
    parser.add_argument(
        "--output-path",
        dest="output_path",
        required=True,
        help="Chemin HDFS de sortie (ex: hdfs://namenode:9000/raw/catalogue)",
    )
    # MySQL (Source A)
    parser.add_argument("--mysql-url",      dest="mysql_url",      default=None)
    parser.add_argument("--mysql-user",     dest="mysql_user",     default=None)
    parser.add_argument("--mysql-password", dest="mysql_password", default=None)
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Mode d'écriture Spark (défaut: overwrite)",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────
#  processor.py
# ─────────────────────────────────────────────────────────────
def parse_processor_args() -> argparse.Namespace:
    """
    Arguments de processor.py.

    spark-submit processor.py
      --raw-catalogue    hdfs://namenode:9000/raw/catalogue
      --raw-audio        hdfs://namenode:9000/raw/audio_features
      --hive-db          silver
      --log-dir          /opt/spark/logs
    """
    parser = argparse.ArgumentParser(description="Processor — Raw → Hive/Silver")
    _add_common_args(parser)

    parser.add_argument(
        "--raw-catalogue",
        dest="raw_catalogue",
        required=True,
        help="Chemin HDFS Raw du catalogue (ex: hdfs://namenode:9000/raw/catalogue)",
    )
    parser.add_argument(
        "--raw-audio",
        dest="raw_audio",
        required=True,
        help="Chemin HDFS Raw des audio_features (ex: hdfs://namenode:9000/raw/audio_features)",
    )
    parser.add_argument(
        "--hive-db",
        dest="hive_db",
        default="silver",
        help="Base Hive de destination (défaut: silver)",
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────
#  datamart.py
# ─────────────────────────────────────────────────────────────
def parse_datamart_args() -> argparse.Namespace:
    """
    Arguments de datamart.py.

    spark-submit datamart.py
      --hive-db          silver
      --mysql-url        jdbc:mysql://mysql:3306/spotify_bdf
      --mysql-user       root
      --mysql-password   root123
      --log-dir          /opt/spark/logs
    """
    parser = argparse.ArgumentParser(description="Datamart — Hive/Silver → MySQL/Gold")
    _add_common_args(parser)

    parser.add_argument(
        "--hive-db",
        dest="hive_db",
        default="silver",
        help="Base Hive source (défaut: silver)",
    )
    parser.add_argument("--mysql-url",      dest="mysql_url",      required=True)
    parser.add_argument("--mysql-user",     dest="mysql_user",     required=True)
    parser.add_argument("--mysql-password", dest="mysql_password", required=True)
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append"],
    )
    return parser.parse_args()
