"""
split_dataset.py -- Decoupage vertical de spotify_data.csv
===========================================================
Produit deux fichiers CSV :
  - catalogue.csv      : track_id, track_name, artist_name, genre, year, duration_ms
  - audio_features.csv : track_id, danceability, energy, key, loudness, mode,
                         speechiness, acousticness, instrumentalness, liveness,
                         valence, tempo, time_signature, popularity

Aucune dependance externe -- stdlib uniquement (csv, hashlib, argparse).

Usage :
    python3 split_dataset.py --input spotify_data.csv --output data
    python3 split_dataset.py --input spotify_data.csv --output ./data
"""

import argparse
import csv
import hashlib
import os
import sys


CATALOGUE_COLS = ["track_name", "artist_name", "genre", "year", "duration_ms"]

AUDIO_COLS = [
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness", "liveness",
    "valence", "tempo", "time_signature", "popularity",
]


def make_track_id(track_name: str, artist_name: str) -> str:
    raw = track_name + artist_name
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def split(input_path: str, output_dir: str) -> None:
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    cat_path   = os.path.join(output_dir, "catalogue.csv")
    audio_path = os.path.join(output_dir, "audio_features.csv")

    cat_header   = ["track_id"] + CATALOGUE_COLS
    audio_header = ["track_id"] + AUDIO_COLS

    total = skipped = 0

    with open(input_path, newline="", encoding="utf-8", errors="replace") as fin, \
         open(cat_path,   "w", newline="", encoding="utf-8") as f_cat, \
         open(audio_path, "w", newline="", encoding="utf-8") as f_audio:

        reader   = csv.DictReader(fin)
        w_cat    = csv.DictWriter(f_cat,   fieldnames=cat_header)
        w_audio  = csv.DictWriter(f_audio, fieldnames=audio_header)
        w_cat.writeheader()
        w_audio.writeheader()

        for row in reader:
            total += 1
            track_name  = row.get("track_name",  "").strip()
            artist_name = row.get("artists",      "").strip() \
                       or row.get("artist_name",  "").strip()

            if not track_name or not artist_name:
                skipped += 1
                continue

            track_id = make_track_id(track_name, artist_name)

            cat_row = {"track_id": track_id, "track_name": track_name, "artist_name": artist_name}
            for col in ["genre", "year", "duration_ms"]:
                cat_row[col] = row.get(col, "").strip()
            w_cat.writerow(cat_row)

            audio_row = {"track_id": track_id}
            for col in AUDIO_COLS:
                audio_row[col] = row.get(col, "").strip()
            w_audio.writerow(audio_row)

    kept = total - skipped
    print(f"Split termine : {total} lignes lues, {skipped} ignorees, {kept} ecrites.")
    print(f"  -> {cat_path}")
    print(f"  -> {audio_path}")


def main():
    parser = argparse.ArgumentParser(description="Decoupage vertical de spotify_data.csv")
    parser.add_argument("--input",  required=True, help="Chemin vers spotify_data.csv")
    parser.add_argument(
        "--output",
        required=True,
        help="Repertoire de sortie (ex: data ou ./data ; sous Windows, eviter \\data si vous voulez le dossier du projet)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"ERREUR : fichier introuvable : {args.input}", file=sys.stderr)
        sys.exit(1)

    split(args.input, args.output)


if __name__ == "__main__":
    main()
