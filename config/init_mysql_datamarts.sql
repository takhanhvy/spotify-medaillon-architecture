-- ============================================================================
--  init_mysql_datamarts.sql — 4 tables Gold (créées par datamart.py via JDBC)
--  Base : spotify_bdf
-- ============================================================================

USE spotify_bdf;

-- ── DM1 — dm_track_popularity ─────────────────────────────────────────────
DROP TABLE IF EXISTS dm_track_popularity;
CREATE TABLE dm_track_popularity (
    track_id              VARCHAR(16)    NOT NULL,
    track_name            VARCHAR(500),
    artist_name           VARCHAR(500),
    genre                 VARCHAR(100),
    year                  SMALLINT,
    popularity            TINYINT UNSIGNED,
    danceability          DECIMAL(6,4),
    energy                DECIMAL(6,4),
    valence               DECIMAL(6,4),
    tempo                 DECIMAL(8,2),
    acousticness          DECIMAL(6,4),
    instrumentalness      DECIMAL(6,4),
    duration_ms           INT UNSIGNED,
    rank_in_genre         INT,
    popularity_category   VARCHAR(10),
    ingestion_date        DATE,
    PRIMARY KEY (track_id),
    INDEX idx_genre        (genre),
    INDEX idx_artist       (artist_name),
    INDEX idx_popularity   (popularity),
    INDEX idx_rank         (rank_in_genre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── DM2 — dm_genre_trends ──────────────────────────────────────────────────
DROP TABLE IF EXISTS dm_genre_trends;
CREATE TABLE dm_genre_trends (
    genre             VARCHAR(100)   NOT NULL,
    decade            SMALLINT       NOT NULL,
    avg_popularity    DECIMAL(6,2),
    nb_tracks         INT,
    avg_danceability  DECIMAL(6,4),
    avg_energy        DECIMAL(6,4),
    top_track_name    VARCHAR(500),
    ingestion_date    DATE,
    PRIMARY KEY (genre, decade),
    INDEX idx_decade  (decade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── DM3 — dm_top_artists ──────────────────────────────────────────────────
DROP TABLE IF EXISTS dm_top_artists;
CREATE TABLE dm_top_artists (
    artist_name       VARCHAR(500)   NOT NULL,
    decade            SMALLINT       NOT NULL,
    total_tracks      INT,
    avg_popularity    DECIMAL(6,2),
    max_popularity    DECIMAL(6,2),
    main_genre        VARCHAR(100),
    influence_score   DECIMAL(10,2),
    rank_in_decade    INT,
    ingestion_date    DATE,
    PRIMARY KEY (artist_name, decade),
    INDEX idx_decade  (decade),
    INDEX idx_rank    (rank_in_decade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── DM4 — dm_hits_emergents ────────────────────────────────────────────────
DROP TABLE IF EXISTS dm_hits_emergents;
CREATE TABLE dm_hits_emergents (
    genre           VARCHAR(100)   NOT NULL,
    year            SMALLINT       NOT NULL,
    track_id        VARCHAR(16)    NOT NULL,
    track_name      VARCHAR(500),
    artist_name     VARCHAR(500),
    popularity      TINYINT UNSIGNED,
    danceability    DECIMAL(6,4),
    energy          DECIMAL(6,4),
    valence         DECIMAL(6,4),
    tempo           DECIMAL(8,2),
    rank_in_year    INT,
    ingestion_date  DATE,
    PRIMARY KEY (genre, year, track_id),
    INDEX idx_year  (year),
    INDEX idx_genre (genre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
