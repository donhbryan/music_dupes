import sqlite3
import os
import sys
import logging
from tqdm import tqdm

# Configuration
OLD_DB_PATH = "library_manager.db"  # Source
NEW_DB_PATH = "library_manager_v2.db"  # Destination
BLOCK_SIZE = 16

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_blocks(fingerprint):
    """Splits fingerprint into chunks for indexing (Logic copied from library_manager.py)."""
    if not fingerprint:
        return []
    return [
        fingerprint[i : i + BLOCK_SIZE] for i in range(0, len(fingerprint), BLOCK_SIZE)
    ][:16]


def init_new_schema(conn):
    """Creates the full V2 schema in the destination database."""
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    # 1. Albums
    cur.execute(
        """CREATE TABLE IF NOT EXISTS albums (
                    release_id TEXT PRIMARY KEY,
                    album_title TEXT,
                    album_artist TEXT,
                    release_date TEXT,
                    country TEXT
                )"""
    )

    # 2. Files
    cur.execute(
        """CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    fingerprint TEXT,
                    acoustid_id TEXT, 
                    title TEXT,
                    track_no INTEGER,
                    disc_no INTEGER,
                    format TEXT,
                    file_size INTEGER,
                    quality_score REAL,
                    album_id TEXT,
                    processed INTEGER DEFAULT 0,
                    FOREIGN KEY (album_id) REFERENCES albums (release_id)
                )"""
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_acoustid ON files(acoustid_id)")

    # 3. Known Fingerprints (History)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS known_fingerprints (
                    fingerprint TEXT,
                    acoustid_id TEXT,
                    PRIMARY KEY (fingerprint, acoustid_id)
                )"""
    )

    # 4. Known Blocks (Index)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS known_blocks (
                    block TEXT,
                    acoustid_id TEXT
                )"""
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_known_blocks ON known_blocks(block)")

    conn.commit()


def migrate():
    if not os.path.exists(OLD_DB_PATH):
        logging.error(f"Source database not found: {OLD_DB_PATH}")
        sys.exit(1)

    if os.path.exists(NEW_DB_PATH):
        print(f"Warning: Destination {NEW_DB_PATH} already exists.")
        choice = input("Overwrite? (y/n): ").lower()
        if choice != "y":
            sys.exit(0)
        os.remove(NEW_DB_PATH)

    print(f"Migrating {OLD_DB_PATH} -> {NEW_DB_PATH}...")

    src_conn = sqlite3.connect(OLD_DB_PATH)
    src_conn.row_factory = sqlite3.Row
    dst_conn = sqlite3.connect(NEW_DB_PATH)

    # 1. Initialize Schema
    init_new_schema(dst_conn)

    # 2. Migrate Albums
    print("Migrating Albums...")
    try:
        albums = src_conn.execute("SELECT * FROM albums").fetchall()
        dst_conn.executemany(
            "INSERT OR IGNORE INTO albums (release_id, album_title, album_artist, release_date, country) VALUES (?, ?, ?, ?, ?)",
            [
                (
                    r["release_id"],
                    r["album_title"],
                    r["album_artist"],
                    r["release_date"],
                    r["country"],
                )
                for r in albums
            ],
        )
        dst_conn.commit()
        print(f" -> {len(albums)} albums copied.")
    except sqlite3.OperationalError as e:
        print(f" -> Error reading albums (schema mismatch?): {e}")

    # 3. Migrate Files & Populate History
    print("Migrating Files and building Fingerprint Index...")
    try:
        # Check available columns in source
        cur = src_conn.execute("SELECT * FROM files LIMIT 1")
        columns = [description[0] for description in cur.description]
        has_acoustid = "acoustid_id" in columns
        has_fp = "fingerprint" in columns

        files = src_conn.execute("SELECT * FROM files").fetchall()

        files_migrated = 0
        history_entries = 0

        for row in tqdm(files):
            # Safe extraction with defaults
            r = dict(row)

            # Map old schema to new schema variables
            path = r.get("path")
            fp = r.get("fingerprint")
            aid = r.get("acoustid_id") if has_acoustid else None

            # Insert into new Files table
            dst_conn.execute(
                """INSERT OR IGNORE INTO files 
                   (path, fingerprint, acoustid_id, title, track_no, disc_no, format, file_size, quality_score, album_id, processed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    path,
                    fp,
                    aid,
                    r.get("title"),
                    r.get("track_no"),
                    r.get("disc_no"),
                    r.get("format"),
                    r.get("file_size"),
                    r.get("quality_score"),
                    r.get("album_id"),
                    r.get("processed", 0),
                ),
            )
            files_migrated += 1

            # --- POPULATE NEW HISTORY TABLES ---
            # If we have both a fingerprint and an ID, we can "learn" this association
            # so it persists even if this specific file is deleted later.
            if has_acoustid and has_fp and fp and aid:
                try:
                    # 1. Add to Known Fingerprints
                    dst_conn.execute(
                        "INSERT OR IGNORE INTO known_fingerprints (fingerprint, acoustid_id) VALUES (?, ?)",
                        (fp, aid),
                    )

                    # 2. Add Blocks
                    blocks = [(b, aid) for b in get_blocks(fp)]
                    dst_conn.executemany(
                        "INSERT INTO known_blocks (block, acoustid_id) VALUES (?, ?)",
                        blocks,
                    )
                    history_entries += 1
                except sqlite3.Error as e:
                    logging.warning(f"Failed to index history for {path}: {e}")

        dst_conn.commit()
        print(f" -> {files_migrated} files migrated.")
        print(
            f" -> {history_entries} historical fingerprints indexed for local lookup."
        )

    except sqlite3.OperationalError as e:
        print(f" -> Critical Error reading files: {e}")

    src_conn.close()
    dst_conn.close()
    print("\nMigration Complete.")
    print(f"New database created at: {NEW_DB_PATH}")
    print("Please update your library_manager.py CONFIG to use this new file.")


if __name__ == "__main__":
    migrate()
