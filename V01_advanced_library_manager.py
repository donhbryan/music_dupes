import time
import os
import sqlite3
import logging
import acoustid
import mutagen
from mutagen.id3 import ID3, TRCK, TPOS, TPE1, TPE2
from mutagen.easyid3 import EasyID3
from tqdm import tqdm
import shutil

# --- Configuration ---
API_KEY = "7dlZplmc3N"
MUSIC_FOLDER = "./data/music"
DUP_FOLDER = "./data/dups"
DB_PATH = "library_manager.db"
DRY_RUN = False  # Set to True to simulate without moving/tagging

# Setup Logging
logging.basicConfig(
    filename="library_manager.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# --- Database Logic ---
def init_db():
    conn = sqlite3.connect("library_manager.db")
    cur = conn.cursor()
    # Added file_size to the schema
    cur.execute(
        """CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    fingerprint TEXT,
                    score INTEGER DEFAULT 0,
                    format TEXT,
                    bitrate INTEGER,
                    sample_rate INTEGER,
                    bits_per_sample INTEGER,
                    file_size INTEGER,
                    processed INTEGER DEFAULT 0,
                    is_duplicate INTEGER DEFAULT 0
                )"""
    )
    conn.commit()
    return conn

# renaming files
import re
from mutagen import File


def rename_audio_by_title(file_path):
    """
    Renames an audio file based on its title metadata.
    Supports .flac, .wav, .alac (.m4a), .mp3, .m4a, and .wma.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    try:
        # Load audio file (mutagen automatically detects format)
        audio = File(file_path, easy=True)

        if audio is None or "title" not in audio:
            print(
                f"Skipping: No title metadata found for {os.path.basename(file_path)}"
            )
            return

        # Extract title (usually returned as a list)
        title = audio["title"][0]

        # Sanitize title to make it a safe filename
        # Removes: \ / : * ? " < > | and leading/trailing whitespace
        clean_title = re.sub(r'[\\/*?:"<>|]', "", title).strip()

        if not clean_title:
            print(
                f"Skipping: Sanitized title is empty for {os.path.basename(file_path)}"
            )
            return

        # Get directory and original extension
        directory = os.path.dirname(file_path)
        extension = os.path.splitext(file_path)[1]

        # Build new path
        new_filename = f"{clean_title}{extension}"
        new_path = os.path.join(directory, new_filename)

        # Handle naming collisions
        # if os.path.exists(new_path) and file_path != new_path:
        #     print(f"Collision: {new_filename} already exists. Skipping.")
        #     return
        resolved_path = safe_rename(new_path, new_filename, dry_run=False)

        # Perform the rename
        os.rename(file_path, resolved_path)
        print(f"Success: Renamed to {new_filename}")
        return resolved_path
    except Exception as e:
        print(f"An error occurred with {file_path}: {e}")
        return False

# Example Usage:
# rename_audio_by_title("path/to/my/song.mp3")

def rename_file(original_path: str, new_name: str, dry_run: bool) -> str:
    """Renames the file to the new name in the same directory."""
    extension = os.path.splitext(original_path)
    dir_name = os.path.dirname(original_path)
    # new_path = os.path.join(dir_name, new_name)

    # 3. Rename the File

    # Clean title of illegal characters for Linux/Windows filesystems
    clean_title = "".join(x for x in new_name if x.isalnum() or x in " -_")
    new_filename = f"{clean_title}{extension}"
    new_path = os.path.join(dir_name, new_filename)

    # os.rename(original_path, new_path)
    print(f"Success: {original_path}---> {new_path}")

    if dry_run:
        logging.info(f"DRY RUN: Renaming {original_path} to {new_path}")
    else:
        logging.info(f"Renaming {original_path} to {new_path}")
        os.rename(original_path, new_path)

    return new_path


# Safe_rename
def safe_rename(target_path: str, base_name: str, dry_run: bool) -> str:
    """Renames the target path if a file with the same name exists."""
    dir_name = os.path.dirname(target_path)
    name, ext = os.path.splitext(base_name)
    counter = 1
    new_target_path = target_path

    while os.path.exists(new_target_path):
        new_base_name = f"{name} ({counter:02d}){ext}"
        new_target_path = os.path.join(dir_name, new_base_name)
        counter += 1

    if dry_run:
        logging.info(
            f"DRY RUN: Renaming {base_name} to {os.path.basename(new_target_path)}"
        )
    else:
        logging.info(f"Renaming {base_name} to {os.path.basename(new_target_path)}")

    return new_target_path


def get_file_stats(file_path: str):
    """Calculates quality score and captures file size in bytes."""
    try:
        audio = mutagen.File(file_path)
        if audio is None:
            return None

        file_size = os.path.getsize(file_path)
        info = audio.info
        ext = os.path.splitext(file_path)[1].lower()

        # ... (rest of the quality score logic from the previous step)

        # Priority: Lossless = 2, Lossy = 1
        fmt_score = 2 if ext in [".flac", ".wav", ".alac"] else 1
        bitrate = getattr(info, "bitrate", 0)
        sample_rate = getattr(info, "sample_rate", 0)
        bits_per_sample = getattr(info, "bits_per_sample", 16)  # Default 16 for lossy

        # Numerical score for comparison
        quality_score = (
            (fmt_score * 10000000)
            + (bits_per_sample * 100000)
            + (sample_rate)
            + (bitrate / 1000)
        )

        return {
            "score": quality_score,
            "format": ext,
            "bitrate": bitrate,
            "sample_rate": sample_rate,
            "bits_per_sample": bits_per_sample,
            "size": file_size,
        }  # pyright: ignore[reportUnknownVariableType]

    except Exception as e:
        logging.error(f"Quality Check Error {file_path}: {e}")
        return None

    # except Exception:
    #     return None


def get_file_quality(file_path: str):
    """Returns a score and specs for the file quality."""
    try:
        audio = mutagen.File(file_path)
        if audio is None:
            return None

        info = audio.info
        ext = os.path.splitext(file_path)[1].lower()

        # Priority: Lossless = 2, Lossy = 1
        fmt_score = 2 if ext in [".flac", ".wav", ".alac"] else 1
        bitrate = getattr(info, "bitrate", 0)
        sample_rate = getattr(info, "sample_rate", 0)
        bits_per_sample = getattr(info, "bits_per_sample", 16)  # Default 16 for lossy

        # Numerical score for comparison
        quality_score = (
            (fmt_score * 10000000)
            + (bits_per_sample * 100000)
            + (sample_rate)
            + (bitrate / 1000)
        )

        return {
            "score": quality_score,
            "format": ext,
            "bitrate": bitrate,
            "sample_rate": sample_rate,
            "bits_per_sample": bits_per_sample,
        }
    except Exception as e:
        logging.error(f"Quality Check Error {file_path}: {e}")
        return None


# --- Core Logic ---
def process_library():
    conn = init_db()
    cur = conn.cursor()

    if not os.path.exists(DUP_FOLDER):
        os.makedirs(DUP_FOLDER)

    # 1. Gather files
    all_files = []
    valid_exts = (".mp3", ".flac", ".m4a", ".mp4", ".wma", ".wav")
    for root, _, files in os.walk(MUSIC_FOLDER):
        for f in files:
            if f.lower().endswith(valid_exts):
                all_files.append(os.path.join(root, f))

    # 2. Process Files
    for file_path1 in tqdm(all_files, desc="Syncing Library", unit="file"):

        if not (file_path := rename_audio_by_title(file_path1)):
            continue
        # Check if already processed
        cur.execute("SELECT processed FROM files WHERE path = ?", (file_path,))
        row = cur.fetchone()
        if row and row[0] == 1:
            continue

        try:
            time.sleep(0.1)  # To avoid hitting API rate limits
            # Get Fingerprint
            duration, fingerprint = acoustid.fingerprint_file(file_path)
            q = get_file_stats(file_path)

            if not q:
                continue

            # Check for Duplicate by Fingerprint
            cur.execute(
                "SELECT path, score FROM files WHERE fingerprint = ? AND processed = 1",
                (fingerprint,),
            )
            existing = cur.fetchone()

            if existing:
                existing_path, existing_score = existing

                if q["score"] > existing_score:
                    # New file is better: Move old one to dups
                    logging.info(f"Better quality found. Moving old: {existing_path}")

                    target_path = os.path.join(
                        DUP_FOLDER, os.path.basename(existing_path)
                    )
                    dup_path = safe_rename(
                        target_path, os.path.basename(existing_path), DRY_RUN
                    )
                    if not DRY_RUN:
                        shutil.move(existing_path, dup_path)

                        # Update DB with the new better path
                        cur.execute(
                            "UPDATE files SET path = ?, score = ?, format = ? WHERE fingerprint = ?",
                            (
                                file_path,
                                q["score"],
                                q["format"],
                                fingerprint,
                            ),  # pyright: ignore[reportUnknownArgumentType]
                        )
                else:
                    # New file is worse: Move to dups
                    logging.info(f"Duplicate (lower quality): {file_path}")
                    target_path = os.path.join(DUP_FOLDER, os.path.basename(file_path))
                    dup_path = safe_rename(
                        target_path, os.path.basename(file_path), DRY_RUN
                    )
                    if not DRY_RUN:
                        shutil.move(file_path, dup_path)
                    cur.execute(
                        "INSERT OR REPLACE INTO files (path, fingerprint, score, format, processed) VALUES (?, ?, ?, ?, 1)",
                        (
                            dup_path,
                            fingerprint,
                            q["score"],
                            q["format"],
                        ),  # pyright: ignore[reportUnknownArgumentType]
                    )
                    conn.commit()
                    continue  # Skip tagging/renaming for the discarded duplicate

            # Perform Metadata Lookup & Tagging (Only for the "winner")
            lookup = acoustid.lookup(
                API_KEY, fingerprint, duration, meta="recordings releases"
            )
            if lookup["status"] == "ok" and lookup["results"]:
                # ... [Insert the Metadata Dictionary & update_tags function from previous step here] ...
                # Use the 'meta' dict from previous script
                # TODO: Implement metadata extraction and tagging here then move it to all files
                # Mark as processed
                cur.execute(
                    "INSERT OR REPLACE INTO files (path, fingerprint, score, format, bitrate, sample_rate, bits_per_sample, file_size, processed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (
                        file_path,
                        fingerprint,
                        q["score"],
                        q["format"],
                        q["bitrate"],
                        q["sample_rate"],
                        q["bits_per_sample"],
                        q["size"],
                    ),  # pyright: ignore[reportUnknownArgumentType]
                )
                conn.commit()

        except Exception as e:
            logging.error(f"Critical error on {file_path}: {e}")

    conn.close()


def generate_library_report():
    conn = sqlite3.connect("library_manager.db")
    cur = conn.cursor()

    # 1. General Stats
    cur.execute("SELECT COUNT(*), SUM(file_size) FROM files WHERE is_duplicate = 0")
    total_files, total_size = cur.fetchone()

    # 2. Duplicate Stats
    cur.execute("SELECT COUNT(*), SUM(file_size) FROM files WHERE is_duplicate = 1")
    dup_count, saved_bytes = cur.fetchone()

    # 3. Format Distribution
    cur.execute(
        "SELECT format, COUNT(*) FROM files WHERE is_duplicate = 0 GROUP BY format"
    )
    formats = cur.fetchall()

    conn.close()

    # Conversion to GB for readability
    total_gb = (total_size or 0) / (1024**3)
    saved_gb = (saved_bytes or 0) / (1024**3)

    print("\n" + "=" * 30)
    print("MUSIC LIBRARY REPORT")
    print("=" * 30)
    print(f"Total Unique Tracks:  {total_files}")
    print(f"Library Size:         {total_gb:.2f} GB")
    print(f"Duplicates Removed:   {dup_count or 0}")
    print(f"Storage Space Saved:  {saved_gb:.2f} GB")
    print("-" * 30)
    print("Format Breakdown:")
    for fmt, count in formats:
        print(f"  {fmt.upper()}: {count} files")
    print("=" * 30 + "\n")


if __name__ == "__main__":
    process_library()
    generate_library_report()
