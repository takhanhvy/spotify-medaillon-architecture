-- ============================================================================
--  init_mysql_catalogue.sql — Source A : table catalogue + users API
--  Base : spotify_bdf | Moteur : InnoDB | Charset : utf8mb4
-- ============================================================================

CREATE DATABASE IF NOT EXISTS spotify_bdf
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE spotify_bdf;

-- ── Table catalogue (Source A) ─────────────────────────────────────────────
DROP TABLE IF EXISTS catalogue;
CREATE TABLE catalogue (
    track_id     VARCHAR(16)   NOT NULL  COMMENT 'Hash MD5 (track_name + artist_name)',
    track_name   TEXT          NOT NULL,
    artist_name  TEXT          NOT NULL,
    genre        VARCHAR(100),
    year         SMALLINT,
    duration_ms  INT UNSIGNED,
    created_at   TIMESTAMP     NOT NULL  DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (track_id),
    INDEX idx_artist (artist_name(255)),
    INDEX idx_genre  (genre),
    INDEX idx_year   (year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Chargement depuis catalogue.csv (adapter le chemin) ───────────────────
-- LOAD DATA LOCAL INFILE '/data/catalogue.csv'
-- INTO TABLE catalogue
-- FIELDS TERMINATED BY ',' ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS (track_id, track_name, artist_name, genre, year, duration_ms);

-- ── Utilisateur API ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS api_users (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    username      VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Utilisateur par défaut : admin / spotify123
INSERT IGNORE INTO api_users (username, password_hash)
VALUES ('admin', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW');
