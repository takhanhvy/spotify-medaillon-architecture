# -*- coding: utf-8 -*-
"""
utils/logger.py - Module de logging unifie
Chaque execution produit un fichier : logs/<job>_YYYYMMDD_HHMMSS.txt
"""

import logging
import os
import sys
from datetime import datetime


def get_logger(job_name, log_dir):
    """
    Cree et retourne un logger nomme <job_name>.
    Ecrit simultanement dans un fichier .txt et sur stdout.
    """
    os.makedirs(log_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, "%s_%s.txt" % (job_name, ts))

    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    # Evite d'ajouter plusieurs handlers lors de re-imports
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler fichier
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.info("Logger initialise - fichier : %s", log_file)
    return logger
