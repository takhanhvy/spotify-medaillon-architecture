"""
db.py — Connexion MySQL et helpers de requêtage
================================================
"""
import os
import pymysql
import pymysql.cursors


def get_connection():
    """Retourne une connexion pymysql vers MySQL."""
    url = os.getenv("MYSQL_URL", "mysql://root:root123@mysql:3306/spotify_bdf")
    # Parsing simple de l'URL jdbc:mysql://host:port/db ou mysql://host:port/db
    url = url.replace("jdbc:mysql://", "").replace("mysql://", "")
    userinfo, hostinfo = (url.split("@") + [""])[:2] if "@" in url else ("", url)
    user, password = (userinfo.split(":") + [""])[:2] if ":" in userinfo else (userinfo, "")
    hostport, db   = (hostinfo.split("/") + ["spotify_bdf"])[:2]
    host, port     = (hostport.split(":") + ["3306"])[:2]

    return pymysql.connect(
        host=host,
        port=int(port),
        user=user or os.getenv("MYSQL_USER", "root"),
        password=password or os.getenv("MYSQL_PASSWORD", "root123"),
        database=db or os.getenv("MYSQL_DB", "spotify_bdf"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def query_list(sql: str, params: list = None) -> list[dict]:
    """Exécute une requête et retourne une liste de dicts."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            return cur.fetchall()
    finally:
        conn.close()


def query_paginated(
    sql: str,
    params: list,
    page_size: int,
    offset: int,
) -> tuple[list[dict], int]:
    """
    Exécute sql avec LIMIT/OFFSET et retourne (rows, total_count).
    Le total est calculé en enveloppant la requête dans un COUNT(*).
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Compter le total
            cur.execute(f"SELECT COUNT(*) AS cnt FROM ({sql}) AS sub", params)
            total = cur.fetchone()["cnt"]

            # Récupérer la page
            cur.execute(f"{sql} LIMIT %s OFFSET %s", params + [page_size, offset])
            rows = cur.fetchall()

        return rows, total
    finally:
        conn.close()
