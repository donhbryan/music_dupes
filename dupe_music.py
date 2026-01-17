# import os
# import sqlite3
# import shutil
# import acoustid
# from mutagen import File

import os
import sqlite3
import shutil
import acoustid
import requests
from mutagen import File
from mutagen.easyid3 import EasyID3
from mutagen.flac import FLAC

# --- CONFIGURATION ---
API_KEY = "YOUR_ACOUSTID_API_KEY"
LIBRARY_PATH = "./music"
DUPS_PATH = "./music/dups"
DB_PATH = "music_manager.db"
# ---------------------

# --- SETTINGS ---
# API_KEY = 'YOUR_ACOUSTID_API_KEY'
# LIBRARY_PATH = '/mnt/nas/music'
# DUPS_PATH = '/mnt/nas/music/dups'
# DB_PATH = 'audio_registry.db'

def get_audio_score(path):
    """Calculates quality score based on format and bitrate."""
    audio = File(path, easy=True)
    if not audio: return 0
    ext = os.path.splitext(path)[1].lower()
    info = audio.info
    
    # Priority: Lossless (FLAC/WAV/ALAC) gets a +10000 boost
    format_weight = 10000 if ext in ['.flac', '.wav', '.alac'] else 1000
    bitrate = getattr(info, 'bitrate', 0) or 0
    sample_rate = getattr(info, 'sample_rate', 0) or 0
    return format_weight + (sample_rate // 100) + (bitrate // 1000)

def fetch_normalized_tags(fingerprint, duration):
    """Hits AcoustID/MusicBrainz for verified metadata."""
    try:
        results = acoustid.lookup(API_KEY, fingerprint, duration, meta=['recordings', 'releasegroups'])
        if results['status'] == 'ok' and results['results']:
            # Take the highest-score match from the community database
            rec = results['results'][0].get('recordings', [{}])[0]
            return {
                'title': rec.get('title'),
                'artist': rec.get('artists', [{}])[0].get('name'),
                'album': rec.get('releasegroups', [{}])[0].get('title')
            }
    except Exception:
        return None

# [Logic continues to iterate through the SQLite DB, rank files, and move duplicates]


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS tracks
                 (path TEXT PRIMARY KEY, fingerprint TEXT, 
                  format TEXT, bitrate INTEGER, sample_rate INTEGER, 
                  bit_depth INTEGER, score INTEGER, metadata_synced INTEGER DEFAULT 0)"""
    )
    conn.commit()
    return conn


def get_quality_metrics(file_path):
    """
    Extracts tech specs and calculates a quality score.
    Score = (Format Weight) + (Bitrate/100) + (SampleRate/10000)
    """
    
    audio = File(file_path, easy=True)
    if not audio:
        return None

    ext = os.path.splitext(file_path)[1].lower()
    info = audio.info

    # Weighting: Lossless (10000) > Lossy (1000)
    format_weight = 10000 if ext in [".flac", ".wav", ".alac"] else 1000
    bitrate = getattr(info, "bitrate", 0) or 0
    sample_rate = getattr(info, "sample_rate", 0) or 0

    # Score favors Lossless first, then Sample Rate, then Bitrate
    score = format_weight + (sample_rate // 100) + (bitrate // 1000)

    return {
        "format": ext,
        "bitrate": bitrate,
        "sample_rate": sample_rate,
        "score": score,
    }


def fetch_musicbrainz_tags(fingerprint, duration):
    """Queries AcoustID to get MusicBrainz metadata."""
    try:
        results = acoustid.lookup(
            API_KEY, fingerprint, duration, meta=["recordings", "releasegroups"]
        )
        if results["status"] == "ok" and results["results"]:
            # Take the best match
            best_match = results["results"][0]
            if "recordings" in best_match:
                rec = best_match["recordings"][0]
                return {
                    "title": rec.get("title"),
                    "artist": rec.get("artists", [{}])[0].get("name"),
                    "album": rec.get("releasegroups", [{}])[0].get("title"),
                }
    except Exception as e:
        print(f"AcoustID Lookup failed: {e}")
    return None


def apply_tags(path, tags):
    """Writes metadata to the file."""
    try:
        audio = File(path, easy=True)
        if audio is not None:
            if tags.get("title"):
                audio["title"] = tags["title"]
            if tags.get("artist"):
                audio["artist"] = tags["artist"]
            if tags.get("album"):
                audio["album"] = tags["album"]
            audio.save()
            print(f"  [Tagging Success] {tags['title']} - {tags['artist']}")
    except Exception as e:
        print(f"  [Tagging Failed] {e}")


def run_workflow():
    conn = init_db()
    cursor = conn.cursor()

    # PHASE 1: Scan and Index
    for root, _, files in os.walk(LIBRARY_PATH):
        if "dups" in root:
            continue
        for name in files:
            path = os.path.abspath(os.path.join(root, name))
            if path.lower().endswith((".mp3", ".flac", ".m4a", ".wma")):
                cursor.execute("SELECT path FROM tracks WHERE path=?", (path,))
                if cursor.fetchone():
                    continue

                try:
                    duration, fp = acoustid.fingerprint_file(path)
                    metrics = get_quality_metrics(path)
                    cursor.execute(
                        "INSERT INTO tracks VALUES (?,?,?,?,?,?,0)",
                        (
                            path,
                            fp,
                            metrics["format"],
                            metrics["bitrate"],
                            metrics["sample_rate"],
                            metrics["score"],
                        ),
                    )
                    conn.commit()
                except:
                    continue

    # PHASE 2: Duplicate Identification & Metadata Normalization
    cursor.execute(
        "SELECT fingerprint, COUNT(*) c FROM tracks GROUP BY fingerprint HAVING c > 1"
    )
    duplicates = cursor.fetchall()

    for fp, count in duplicates:
        cursor.execute(
            "SELECT path, score FROM tracks WHERE fingerprint=? ORDER BY score DESC",
            (fp,),
        )
        ordered_files = cursor.fetchall()

        winner_path = ordered_files[0][0]
        losers = ordered_files[1:]

        print(f"\nProcessing Duplicate Group. Winner: {os.path.basename(winner_path)}")

        # 1. Get official tags for the Winner
        duration, _ = acoustid.fingerprint_file(winner_path)
        official_tags = fetch_musicbrainz_tags(fp, duration)

        if official_tags:
            apply_tags(winner_path, official_tags)

        # 2. Move losers to dups and sync their tags before moving
        for loser_path, _ in losers:
            if official_tags:
                apply_tags(loser_path, official_tags)

            if not os.path.exists(DUPS_PATH):
                os.makedirs(DUPS_PATH)
            shutil.move(
                loser_path, os.path.join(DUPS_PATH, os.path.basename(loser_path))
            )
            cursor.execute("DELETE FROM tracks WHERE path=?", (loser_path,))
            print(f"  [Moved] {os.path.basename(loser_path)} to /dups")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    run_workflow()
