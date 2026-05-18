"""
auth.py -- Configuration JWT pour l'API Flask Spotify.
Initialise Flask-JWT-Extended avec gestion des erreurs personnalisee.
"""

import os
from datetime import timedelta

from flask import jsonify
from flask_jwt_extended import JWTManager

jwt = JWTManager()
JWT_EXPIRATION_MIN = int(os.getenv("JWT_EXPIRATION_MINUTES", "30"))


def init_jwt(app):
    """
    Initialise le gestionnaire JWT sur l'application Flask.
    A appeler apres app.config["JWT_SECRET_KEY"] est defini.
    """
    app.config.setdefault(
        "JWT_SECRET_KEY",
        os.getenv("JWT_SECRET_KEY", "spotify-super-secret-key-change-in-production"),
    )
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=JWT_EXPIRATION_MIN)
    jwt.init_app(app)

    @jwt.expired_token_loader
    def expired_token_callback(jwt_header, jwt_payload):
        return jsonify({
            "error": "token_expired",
            "message": "Le token a expire. Veuillez vous reconnecter."
        }), 401

    @jwt.invalid_token_loader
    def invalid_token_callback(error):
        return jsonify({
            "error": "invalid_token",
            "message": "Token invalide."
        }), 401

    @jwt.unauthorized_loader
    def missing_token_callback(error):
        return jsonify({
            "error": "authorization_required",
            "message": "Token d'autorisation manquant."
        }), 401

    @jwt.revoked_token_loader
    def revoked_token_callback(jwt_header, jwt_payload):
        return jsonify({
            "error": "token_revoked",
            "message": "Le token a ete revoque."
        }), 401

    return jwt
