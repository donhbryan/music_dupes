'''
music_manager
    Criteria,       Preference,         Action
    Format,         FLAC > MP3,         Higher base score for lossless.
    Bitrate,        320kbps > 128kbps,  Tie-breaker for same formats.
    Metadata,       MusicBrainz,        Overwrites local inconsistent tags.
    Cleanup,        Move to /dups,      Non-destructive removal.
'''

import logging
import os
import shutil
import sqlite3

import acoustid
from mutagen import File
from mutagen.easyid3 import EasyID3
from mutagen.flac import FLAC

# --- CONFIGURATION ---
# Get your API Key at https://acoustid.org/
API_KEY = "7dlZplmc3N"
LIBRARY_ROOT = "./data/music"
# DUPS_DIR = os.path.join(LIBRARY_ROOT, "dups")
DUPS_DIR = os.path.join("./data", "dups")
DB_PATH = "music_inventory.db"
# Tell the library exactly where the extracted tool is
os.environ["FPCALC"] = r"/home/don/Scripts/python/music_dupes/bin/chromaprint-fpcalc-1.6.0-linux-x86_64/fpcalc"

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def init_db():
    """Initializes the SQLite database for library persistence."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS tracks
                 (path TEXT PRIMARY KEY, fingerprint TEXT, duration REAL,
                  format TEXT, bitrate INTEGER, sample_rate INTEGER, 
                  score INTEGER, metadata_synced INTEGER DEFAULT 0)"""
    )
    conn.commit()
    return conn


def get_quality_metrics(path):
    """
    Ranks files based on audio fidelity.
    Priority: Lossless (10k) > Sample Rate > Bitrate.
    """
    try:
        audio = File(path, easy=True)
        if not audio:
            return 0

        ext = os.path.splitext(path)[1].lower()
        info = audio.info

        # Base score: Lossless gets a significant head start
        base = 10000 if ext in [".flac", ".wav", ".alac"] else 1000
        bitrate = getattr(info, "bitrate", 0) or 0
        sample_rate = getattr(info, "sample_rate", 0) or 0

        # Tie-breaker logic
        # Score favors Lossless first, then Sample Rate, then Bitrate
        score = base + (sample_rate // 100) + (bitrate // 1000)
        return {
            "format": ext,
            "bitrate": bitrate,
            "sample_rate": sample_rate,
            "score": score,
        }
    except Exception as e:
        logging.error(f"Score calculation failed for {path}: {e}")
        return None


def fetch_musicbrainz_tags(fingerprint, duration):
    """Queries AcoustID to get verified MusicBrainz metadata."""
    try:
        results = acoustid.lookup(
            API_KEY, fingerprint, duration, meta=["recordings", "releasegroups","releases"]
        )
        if results["status"] == "ok" and results["results"]:
            # Grab the first recording result as the 'Source of Truth'
            best_match = results["results"][0]
            if "recordings" in best_match:
                rec = best_match["recordings"][0]
                return {
                    "title": rec.get("title"),
                    "artist": rec.get("artists", [{}])[0].get("name"),
                    "album": rec.get("releasegroups", [{}])[0].get("title"),
                    "trach"
                }
                # TODO: Expand to include tracknumber, genre if available
    except Exception as e:
        logging.warning(f"AcoustID lookup failed: {e}")
    return None


def apply_metadata(path, tags):
    """Applies normalized tags to the file."""
    try:
        audio = File(path, easy=True)
        if audio is not None and tags:
            for key in [
                "title",
                "artist",
                "album",
                "tracknumber",
                "genre",
                "tracknumber"
            ]:
                if tags.get(key):
                    audio[key] = tags[key]
            audio.save()
            logging.info(
                f"  [Tagging Success] {tags.get('title')} - {tags.get('artist')}"
            )
            return True
    except Exception as e:
        logging.error(f"Metadata write failed for {path}: {e}")
    return False


def scan_library(conn):
    """Scans the filesystem and populates the database."""
    cursor = conn.cursor()
    supported = (".mp3", ".flac", ".m4a", ".wma", ".wav")

    logging.info("Starting library scan...")
    for root, _, files in os.walk(LIBRARY_ROOT):
        if DUPS_DIR in root:
            continue

        for name in files:
            if name.lower().endswith(supported):
                path = os.path.abspath(os.path.join(root, name))

                # Skip if already in DB
                cursor.execute("SELECT path FROM tracks WHERE path=?", (path,))
                if cursor.fetchone():
                    continue

                try:
                    duration, fp = acoustid.fingerprint_file(path)
                    # score = calculate_quality_score(path)
                    # modify to include bitrate samplerate & bitdepth if needed
                    # ext = os.path.splitext(path)[1].lower()

                    metrics = get_quality_metrics(path)
                    cursor.execute(
                        "INSERT INTO tracks VALUES (?,?,?,?,?,?,?,0)",
                        (
                            path,
                            fp,
                            duration,
                            metrics["format"],
                            metrics["bitrate"],
                            metrics["sample_rate"],
                            metrics["score"],
                        ),
                    )

                    # cursor.execute(
                    #     "INSERT INTO tracks VALUES (?, ?, ?, ?, 0, 0, ?)",
                    #     (path, fp, duration, ext, score),
                    # )
                    conn.commit()
                    logging.info(f"Indexed: {name}")
                except Exception as e:
                    logging.error(f"Could not fingerprint {name}: {e}")


def process_duplicates(conn):
    """Identifies duplicates, normalizes the winner, and moves losers."""
    cursor = conn.cursor()
    if not os.path.exists(DUPS_DIR):
        os.makedirs(DUPS_DIR)

    # Find fingerprints that appear more than once
    cursor.execute(
        "SELECT fingerprint, COUNT(*) c FROM tracks GROUP BY fingerprint HAVING c > 1"
    )
    dup_fingerprints = cursor.fetchall()

    for fp_row in dup_fingerprints:
        fp = fp_row[0]
        cursor.execute(
            "SELECT path, score, duration FROM tracks WHERE fingerprint=? ORDER BY score DESC",
            (fp,),
        )
        entries = cursor.fetchall()

        winner_path, winner_score, duration = entries[0]
        losers = entries[1:]

        logging.info(f"Duplicate group found. Winner: {os.path.basename(winner_path)}")

        # 1. Normalize Winner via MusicBrainz
        official_tags = fetch_musicbrainz_tags(fp, duration)
        if official_tags:
            apply_metadata(winner_path, official_tags)
            logging.info(
                f"  [Normalized] {official_tags['title']} by {official_tags['artist']}"
            )

        # 2. Process Losers
        for loser_path, score, _ in losers:
            # Sync metadata to loser before moving (optional, helps keep dups organized)
            if official_tags:
                apply_metadata(loser_path, official_tags)

            try:
                dest = os.path.join(DUPS_DIR, os.path.basename(loser_path))
                # Handle filename collisions in /dups
                # TODO: Improve collision handling strategy as needed
                if os.path.exists(dest):
                    dest = dest + ".bak"
# TODO:keep track set processed flag and add duplicate count in db
                shutil.move(loser_path, dest)
                cursor.execute("DELETE FROM tracks WHERE path=?", (loser_path,))
                logging.info(f"  [Moved to Dups] {os.path.basename(loser_path)}")
            except Exception as e:
                logging.error(f"  [Move Failed] {loser_path}: {e}")

        conn.commit()


if __name__ == "__main__":
    db_conn = init_db()
    try:
        scan_library(db_conn)
        process_duplicates(db_conn)
    finally:
        db_conn.close()
        logging.info("Workflow complete.")
