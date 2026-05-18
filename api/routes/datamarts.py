"""
routes/datamarts.py — Endpoints des 4 datamarts
=================================================
Tous les endpoints sont sécurisés par JWT (@jwt_required)
et retournent des réponses paginées.
"""

import math
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from db import query_paginated, query_list

dm_bp = Blueprint("datamarts", __name__, url_prefix="/datamarts")


def _paginated_response(rows, total, page, page_size):
    """Format de réponse paginée standard."""
    return jsonify({
        "data":        rows,
        "page":        page,
        "page_size":   page_size,
        "total":       total,
        "total_pages": math.ceil(total / page_size) if page_size else 1,
    })


# ─────────────────────────────────────────────────────────────
#  GET /datamarts/ — Liste des datamarts disponibles
# ─────────────────────────────────────────────────────────────
@dm_bp.get("/")
@jwt_required()
def list_datamarts():
    """
    Liste les datamarts disponibles.
    ---
    tags:
      - Datamarts
    security:
      - Bearer: []
    responses:
      200:
        description: Liste des datamarts exposes par l'API
      401:
        description: Token JWT manquant ou invalide
    """
    return jsonify({
        "datamarts": [
            {"name": "track-popularity",  "endpoint": "/datamarts/track-popularity",
             "description": "Tracks avec features audio et rang par genre"},
            {"name": "genre-trends",      "endpoint": "/datamarts/genre-trends",
             "description": "Tendances par genre et décennie"},
            {"name": "top-artists",       "endpoint": "/datamarts/top-artists",
             "description": "Top artistes par décennie avec score d'influence"},
            {"name": "hits-emergents",    "endpoint": "/datamarts/hits-emergents",
             "description": "Top 10 morceaux par genre et année"},
        ]
    })


# ─────────────────────────────────────────────────────────────
#  GET /datamarts/track-popularity — DM1
# ─────────────────────────────────────────────────────────────
@dm_bp.get("/track-popularity")
@jwt_required()
def track_popularity():
    """
    DM1 — Tracks avec features audio et rang par genre.
    ---
    tags:
      - Datamarts
    security:
      - Bearer: []
    parameters:
      - in: query
        name: page
        schema: {type: integer, default: 1}
        description: Numero de page
      - in: query
        name: page_size
        schema: {type: integer, default: 50, maximum: 200}
        description: Taille de page
      - in: query
        name: artist
        schema: {type: string}
        description: Filtre sur le nom d'artiste
      - in: query
        name: genre
        schema: {type: string}
        description: Filtre sur le genre
      - in: query
        name: year
        schema: {type: integer}
        description: Filtre sur l'annee
      - in: query
        name: min_popularity
        schema: {type: integer}
        description: Popularite minimale
      - in: query
        name: max_rank
        schema: {type: integer}
        description: Rang maximal dans le genre
    responses:
      200:
        description: Page de resultats du datamart dm_track_popularity
      401:
        description: Token JWT manquant ou invalide
    """
    page      = max(1, request.args.get("page",      1,   type=int))
    page_size = min(200, max(1, request.args.get("page_size", 50, type=int)))
    artist    = request.args.get("artist")
    genre     = request.args.get("genre")
    year      = request.args.get("year",         type=int)
    min_pop   = request.args.get("min_popularity", type=int)
    max_rank  = request.args.get("max_rank",       type=int)

    filters, params = [], []
    if artist:   filters.append("artist_name LIKE %s");  params.append(f"%{artist}%")
    if genre:    filters.append("genre = %s");           params.append(genre)
    if year:     filters.append("year = %s");            params.append(year)
    if min_pop:  filters.append("popularity >= %s");     params.append(min_pop)
    if max_rank: filters.append("rank_in_genre <= %s");  params.append(max_rank)

    where  = ("WHERE " + " AND ".join(filters)) if filters else ""
    offset = (page - 1) * page_size

    rows, total = query_paginated(
        f"SELECT * FROM dm_track_popularity {where} ORDER BY popularity DESC",
        params, page_size, offset
    )
    return _paginated_response(rows, total, page, page_size)


# ─────────────────────────────────────────────────────────────
#  GET /datamarts/genre-trends — DM2
# ─────────────────────────────────────────────────────────────
@dm_bp.get("/genre-trends")
@jwt_required()
def genre_trends():
    """
    DM2 — Tendances par genre et décennie.
    ---
    tags:
      - Datamarts
    security:
      - Bearer: []
    parameters:
      - in: query
        name: page
        schema: {type: integer, default: 1}
        description: Numero de page
      - in: query
        name: page_size
        schema: {type: integer, default: 50, maximum: 200}
        description: Taille de page
      - in: query
        name: genre
        schema: {type: string}
        description: Filtre sur le genre
      - in: query
        name: decade
        schema: {type: integer}
        description: Filtre sur la decennie
    responses:
      200:
        description: Page de resultats du datamart dm_genre_trends
      401:
        description: Token JWT manquant ou invalide
    """
    page      = max(1, request.args.get("page",      1,   type=int))
    page_size = min(200, max(1, request.args.get("page_size", 50, type=int)))
    genre     = request.args.get("genre")
    decade    = request.args.get("decade", type=int)

    filters, params = [], []
    if genre:  filters.append("genre = %s");  params.append(genre)
    if decade: filters.append("decade = %s"); params.append(decade)

    where  = ("WHERE " + " AND ".join(filters)) if filters else ""
    offset = (page - 1) * page_size

    rows, total = query_paginated(
        f"SELECT * FROM dm_genre_trends {where} ORDER BY decade, avg_popularity DESC",
        params, page_size, offset
    )
    return _paginated_response(rows, total, page, page_size)


# ─────────────────────────────────────────────────────────────
#  GET /datamarts/top-artists — DM3
# ─────────────────────────────────────────────────────────────
@dm_bp.get("/top-artists")
@jwt_required()
def top_artists():
    """
    DM3 — Top artistes par décennie avec score d'influence.
    ---
    tags:
      - Datamarts
    security:
      - Bearer: []
    parameters:
      - in: query
        name: page
        schema: {type: integer, default: 1}
        description: Numero de page
      - in: query
        name: page_size
        schema: {type: integer, default: 50, maximum: 200}
        description: Taille de page
      - in: query
        name: decade
        schema: {type: integer}
        description: Filtre sur la decennie
      - in: query
        name: genre
        schema: {type: string}
        description: Filtre sur le genre principal
      - in: query
        name: max_rank
        schema: {type: integer}
        description: Rang maximal dans la decennie
    responses:
      200:
        description: Page de resultats du datamart dm_top_artists
      401:
        description: Token JWT manquant ou invalide
    """
    page      = max(1, request.args.get("page",      1,   type=int))
    page_size = min(200, max(1, request.args.get("page_size", 50, type=int)))
    decade    = request.args.get("decade",   type=int)
    genre     = request.args.get("genre")
    max_rank  = request.args.get("max_rank", type=int)

    filters, params = [], []
    if decade:   filters.append("decade = %s");          params.append(decade)
    if genre:    filters.append("main_genre = %s");      params.append(genre)
    if max_rank: filters.append("rank_in_decade <= %s"); params.append(max_rank)

    where  = ("WHERE " + " AND ".join(filters)) if filters else ""
    offset = (page - 1) * page_size

    rows, total = query_paginated(
        f"SELECT * FROM dm_top_artists {where} ORDER BY decade, rank_in_decade",
        params, page_size, offset
    )
    return _paginated_response(rows, total, page, page_size)


# ─────────────────────────────────────────────────────────────
#  GET /datamarts/hits-emergents — DM4
# ─────────────────────────────────────────────────────────────
@dm_bp.get("/hits-emergents")
@jwt_required()
def hits_emergents():
    """
    DM4 — Top 10 morceaux par genre et année (hits émergents).
    ---
    tags:
      - Datamarts
    security:
      - Bearer: []
    parameters:
      - in: query
        name: page
        schema: {type: integer, default: 1}
        description: Numero de page
      - in: query
        name: page_size
        schema: {type: integer, default: 50, maximum: 200}
        description: Taille de page
      - in: query
        name: genre
        schema: {type: string}
        description: Filtre sur le genre
      - in: query
        name: year
        schema: {type: integer}
        description: Filtre sur l'annee
    responses:
      200:
        description: Page de resultats du datamart dm_hits_emergents
      401:
        description: Token JWT manquant ou invalide
    """
    page      = max(1, request.args.get("page",      1,   type=int))
    page_size = min(200, max(1, request.args.get("page_size", 50, type=int)))
    genre     = request.args.get("genre")
    year      = request.args.get("year", type=int)

    filters, params = [], []
    if genre: filters.append("genre = %s"); params.append(genre)
    if year:  filters.append("year = %s");  params.append(year)

    where  = ("WHERE " + " AND ".join(filters)) if filters else ""
    offset = (page - 1) * page_size

    rows, total = query_paginated(
        f"SELECT * FROM dm_hits_emergents {where} ORDER BY year DESC, rank_in_year",
        params, page_size, offset
    )
    return _paginated_response(rows, total, page, page_size)
