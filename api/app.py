"""
app.py — API REST Flask
========================
API sécurisée JWT exposant les 4 datamarts MySQL de la couche Gold.
Documentation Swagger sur /docs (Flasgger).

Endpoints :
  POST /auth/login                 — Obtenir un token JWT
  GET  /datamarts/                 — Liste des datamarts disponibles
  GET  /datamarts/track-popularity — DM1 paginé
  GET  /datamarts/genre-trends     — DM2 paginé
  GET  /datamarts/top-artists      — DM3 paginé
  GET  /datamarts/hits-emergents   — DM4 paginé
  GET  /health                     — Statut de l'API

Lancement :
  python app.py
  # ou
  flask run --host=0.0.0.0 --port=8000
"""

import os
import pymysql
from flask import Flask, jsonify, request
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from flasgger import Swagger

from auth   import init_jwt, JWT_EXPIRATION_MIN
from db     import get_connection, query_list
from routes.datamarts import dm_bp


# ─────────────────────────────────────────────────────────────
#  Initialisation Flask
# ─────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

# JWT
jwt = init_jwt(app)

# Swagger (Flasgger) — accessible sur /docs
swagger_config = {
    "headers":  [],
    "specs":    [{"endpoint": "apispec", "route": "/apispec.json"}],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs",
}
swagger_template = {
    "info": {
        "title":       "Spotify Médaillon API",
        "description": "API REST JWT — Datamarts Spotify (Architecture Médaillon)",
        "version":     "1.0.0",
    },
    "securityDefinitions": {
        "Bearer": {
            "type": "apiKey",
            "name": "Authorization",
            "in":   "header",
            "description": "Format : Bearer <token>",
        }
    },
    "security": [{"Bearer": []}],
}
Swagger(app, config=swagger_config, template=swagger_template)

# Blueprints
app.register_blueprint(dm_bp)


# ─────────────────────────────────────────────────────────────
#  POST /auth/login — Authentification
# ─────────────────────────────────────────────────────────────
@app.post("/auth/login")
def login():
    """
    Authentification — retourne un token JWT.
    ---
    tags:
      - Authentification
    parameters:
      - in: body
        name: body
        schema:
          type: object
          required: [username, password]
          properties:
            username: {type: string, example: admin}
            password: {type: string, example: spotify123}
    responses:
      200:
        description: Token JWT généré
      401:
        description: Identifiants incorrects
    """
    body = request.get_json(silent=True) or {}
    username = body.get("username", "")
    password = body.get("password", "")

    if not username or not password:
        return jsonify({"error": "username et password requis"}), 400

    rows = query_list(
        "SELECT password_hash FROM api_users WHERE username = %s",
        [username]
    )
    if not rows:
        return jsonify({"error": "Identifiants incorrects"}), 401

    from passlib.context import CryptContext
    pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
    if not pwd_ctx.verify(password, rows[0]["password_hash"]):
        return jsonify({"error": "Identifiants incorrects"}), 401

    token = create_access_token(identity=username)
    return jsonify({
        "access_token": token,
        "token_type":   "bearer",
        "expires_in":   JWT_EXPIRATION_MIN * 60,
    }), 200


# ─────────────────────────────────────────────────────────────
#  GET /health — Healthcheck
# ─────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    """
    Healthcheck.
    ---
    tags:
      - Système
    responses:
      200:
        description: API et base de données opérationnelles
    """
    try:
        conn = get_connection()
        conn.close()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {e}"

    return jsonify({
        "status":   "ok",
        "database": db_status,
        "version":  "1.0.0",
    })


# ─────────────────────────────────────────────────────────────
#  Gestion des erreurs globales
# ─────────────────────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint introuvable"}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Erreur interne du serveur", "detail": str(e)}), 500


# ─────────────────────────────────────────────────────────────
#  Point d'entrée
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(
        host=os.getenv("FLASK_HOST", "0.0.0.0"),
        port=int(os.getenv("FLASK_PORT", "8000")),
        debug=os.getenv("FLASK_DEBUG", "false").lower() == "true",
    )
