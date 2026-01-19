import os
import shutil
import sqlite3
import logging
import csv
import difflib
import re
import time
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List, Generator

# Third-party imports
from tqdm import tqdm
import acoustid
import mutagen
from mutagen.asf import ASF


# --- Configuration ---
@dataclass
class Config:
    API_KEY: str = "7dlZplmc3N"
    MUSIC_FOLDER: Path = Path("./data/music")
    DUP_FOLDER: Path = Path("./data/dups")
    DB_PATH: str = "library_manager.db"
    EXPORT_PATH: str = "music_library_report.csv"
    LOG_FILE: str = "library_manager.log"
    DRY_RUN: bool = False

    # Tuning
    SLEEP_TIME: float = 0.1  # Increased slightly to be nice to API
    SIMILARITY_AUTO: float = 0.98
    SIMILARITY_ASK: float = 0.95
    BLOCK_SIZE: int = 16

    VALID_EXTS: tuple = (
        ".mp3",
        ".mpeg",
        ".mpg",
        ".mpe",
        ".flac",
        ".m4a",
        ".mp4",
        ".mp4v",
        ".wma",
        ".wav",
    )

    def validate(self):
        """Checks for external dependencies."""
        if not shutil.which("fpcalc"):
            print("ERROR: 'fpcalc' binary not found in PATH.")
            print("Please install Chromaprint/fpcalc before running.")
            sys.exit(1)
        self.MUSIC_FOLDER.mkdir(parents=True, exist_ok=True)
        self.DUP_FOLDER.mkdir(parents=True, exist_ok=True)


# --- Utilities ---
def sanitize_filename(text: str) -> str:
    """Removes illegal characters from filenames."""
    if not text:
        return "Unknown"
    # # Replace fancy quotes with standard ones
    # text = text.replace("“", '"').replace("”", '"')
    # text = text.replace("‘", "'").replace("’", "'")

    # Remove invalid fs chars and trim whitespace
    clean = re.sub(r'[\\/*?:"<>|]', "", str(text)).strip()
    return clean[:100]  # Limit length to avoid OS errors


def get_safe_path(target_path: Path) -> Path:
    """Returns a unique path (appends _01, _02) if file exists."""
    if not target_path.exists():
        return target_path

    counter = 1
    base = target_path
    while target_path.exists():
        target_path = base.with_name(f"{base.stem}_{counter:02d}{base.suffix}")
        counter += 1
    return target_path


# --- Database Handler ---
class DatabaseHandler:
    def __init__(self, db_path: str, block_size: int):
        self.db_path = db_path
        self.block_size = block_size
        self.conn = sqlite3.connect(self.db_path)
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    fingerprint TEXT,
                    score INTEGER,
                    format TEXT,
                    bitrate INTEGER,
                    sample_rate INTEGER,
                    file_size INTEGER,
                    last_mod REAL,
                    is_duplicate INTEGER DEFAULT 0
                )
            """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fingerprint_index (
                    block TEXT,
                    path TEXT,
                    FOREIGN KEY(path) REFERENCES files(path) ON DELETE CASCADE
                )
            """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_block ON fingerprint_index(block)"
            )

    def get_blocks(self, fingerprint: str) -> List[str]:
        """Splits fingerprint into chunks for indexing."""
        return [
            fingerprint[i : i + self.block_size]
            for i in range(0, len(fingerprint), self.block_size)
        ][:8]

    def find_potential_matches(self, fingerprint: str) -> List[str]:
        """Finds paths that share at least one fingerprint block."""
        blocks = self.get_blocks(fingerprint)
        placeholders = ",".join(["?"] * len(blocks))
        query = f"SELECT DISTINCT path FROM fingerprint_index WHERE block IN ({placeholders})"
        cursor = self.conn.execute(query, blocks)
        return [row[0] for row in cursor.fetchall()]

    def get_file_record(self, path: str) -> Optional[Tuple]:
        cursor = self.conn.execute(
            "SELECT score, format, bitrate, file_size, fingerprint FROM files WHERE path = ?",
            (path,),
        )
        return cursor.fetchone()

    def upsert_file(
        self, path: str, fp: str, stats: Dict[str, Any], is_duplicate: bool = False
    ):
        """Updates or inserts a file record and its index."""
        with self.conn:
            # 1. Update Main Table
            self.conn.execute(
                """
                INSERT OR REPLACE INTO files 
                (path, fingerprint, score, format, bitrate, sample_rate, file_size, last_mod, is_duplicate) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    path,
                    fp,
                    stats["score"],
                    stats["format"],
                    stats["bitrate"],
                    stats["sample_rate"],
                    stats["file_size"],
                    stats["mtime"],
                    1 if is_duplicate else 0,
                ),
            )

            # 2. Update Index (only if not duplicate to keep index clean)
            self.conn.execute("DELETE FROM fingerprint_index WHERE path = ?", (path,))
            if not is_duplicate:
                blocks = [(b, path) for b in self.get_blocks(fp)]
                self.conn.executemany(
                    "INSERT INTO fingerprint_index VALUES (?, ?)", blocks
                )

    def delete_index(self, path: str):
        with self.conn:
            self.conn.execute("DELETE FROM fingerprint_index WHERE path = ?", (path,))

    def close(self):
        self.conn.close()


# --- Audio Processor ---
class AudioProcessor:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def calculate_score(
        self, ext: str, bits: int, sample_rate: int, bitrate: int
    ) -> int:
        """Calculates a quality score. Higher is better."""
        # 20M base for lossless, 10M base for others
        base_score = (
            20_000_000 if ext in [".flac", ".wav", ".alac", ".aiff"] else 10_000_000
        )
        return int(base_score + (bits * 100_000) + sample_rate + (bitrate / 1000))

    def get_audio_stats(self, file_path: Path) -> Optional[Dict[str, Any]]:
        try:
            audio = mutagen.File(file_path)
            if not audio:
                return None

            info = audio.info
            ext = file_path.suffix.lower()

            # Safe attribute access
            bitrate = getattr(info, "bitrate", 0)
            sample_rate = getattr(info, "sample_rate", 0)
            bits = getattr(info, "bits_per_sample", 16)  # Default to 16 if unknown

            stats = {
                "format": getattr(info, "format", ext),
                "bitrate": bitrate,
                "sample_rate": sample_rate,
                "file_size": file_path.stat().st_size,
                "mtime": file_path.stat().st_mtime,
                "score": self.calculate_score(ext, bits, sample_rate, bitrate),
            }
            return stats
        except Exception as e:
            logging.warning(f"Metadata error {file_path}: {e}")
            return None

    def get_fingerprint(self, path: Path) -> Tuple[float, str]:
        """Returns (duration, fingerprint)."""
        return acoustid.fingerprint_file(str(path))

    def fetch_tags(self, fingerprint: str, duration: float) -> Dict[str, str]:
        """Queries AcoustID for metadata."""
        try:
            resp = acoustid.lookup(
                self.api_key, fingerprint, duration, meta="recordings releases"
            )
            if resp["status"] != "ok" or not resp.get("results"):
                return {}

            recording = resp["results"][0]["recordings"][0]
            release = recording.get("releases", [{}])[0]

            return {
                "title": recording.get("title", "Unknown Track"),
                "artist": recording.get("artists", [{}])[0].get(
                    "name", "Unknown Artist"
                ),
                "album": release.get("title", "Unknown Album"),
                "date": str(release.get("date", {}).get("year", "")),
            }
        except Exception as e:
            logging.warning(f"Tag fetch failed for fp start {fingerprint[:10]}: {e}")
            return {}

    def write_tags(self, file_path: Path, tags: Dict[str, str], is_dry_run: bool):
        if not tags or is_dry_run:
            return

        try:
            # Special handling for WMA
            if file_path.suffix.lower() == ".wma":
                audio = ASF(file_path)
                audio["WM/AlbumTitle"] = [tags["album"]]
                audio["Author"] = [tags["artist"]]
                audio["Title"] = [tags["title"]]
                if tags["date"]:
                    audio["WM/Year"] = [tags["date"]]
                audio.save()
            else:
                # EasyID3/EasyMP4 wrapper for everything else
                audio = mutagen.File(file_path, easy=True)
                if audio:
                    audio["title"] = tags.get("title")
                    audio["artist"] = tags.get("artist")
                    audio["album"] = tags.get("album")
                    audio["date"] = tags.get("date")
                    audio.save()
        except Exception as e:
            logging.error(f"Failed to write tags to {file_path}: {e}")


# --- Library Manager (The Core) ---
class LibraryManager:
    def __init__(self, config: Config):
        self.cfg = config
        self.cfg.validate()
        self.prune_database()

        logging.basicConfig(
            filename=self.cfg.LOG_FILE,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        self.db = DatabaseHandler(self.cfg.DB_PATH, self.cfg.BLOCK_SIZE)
        self.audio = AudioProcessor(self.cfg.API_KEY)

    # --- NEW: Database Pruning Utility ---
    def prune_database(self):
        """Checks DB entries against filesystem and removes non-existent files."""
        if not self.cfg.MUSIC_FOLDER.exists():
            logging.error(
                "Music folder %s not found. Aborting prune to prevent DB wipe.",
                self.cfg.MUSIC_FOLDER,
            )
            print("(!) Music folder not found. Skipping prune safety check.")
            return

        print("Checking for ghost entries in database...")
        removed_count = 0

        with sqlite3.connect(self.cfg.DB_PATH) as conn:
            # Get all paths currently in the DB
            cursor = conn.execute("SELECT path FROM files")
            all_paths = cursor.fetchall()

            for (path_str,) in tqdm(all_paths, desc="Pruning DB"):
                if not Path(path_str).exists():
                    # File is missing, delete from both tables
                    conn.execute("DELETE FROM files WHERE path = ?", (path_str,))
                    conn.execute(
                        "DELETE FROM fingerprint_index WHERE path = ?", (path_str,)
                    )
                    removed_count += 1

            conn.commit()

        if removed_count > 0:
            logging.info("Pruned %d ghost entries from database.", removed_count)
            print(f"cleaned up {removed_count} missing files from the database.")
        else:
            print("Database is clean.")

    def scan_files(self) -> Generator[Path, None, None]:
        """Yields valid audio files from the music directory."""
        for root, _, files in os.walk(self.cfg.MUSIC_FOLDER):
            for f in files:
                if f.lower().endswith(self.cfg.VALID_EXTS):
                    yield Path(root) / f

    def process_library(self):
        files = list(self.scan_files())
        print(f"Scanning {len(files)} files...")

        for current_path in tqdm(files, desc="Processing"):
            try:
                self._process_single_file(current_path)
            except Exception as e:
                logging.error(f"Critical failure on file {current_path}: {e}")

        self.cleanup_empty_folders()
        self.db.close()

    def _process_single_file(self, current_path: Path):
        # 1. Skip if unmodified
        if not current_path.exists():
            return
        current_mtime = current_path.stat().st_mtime

        # Check DB for modification time
        cached_row = self.db.conn.execute(
            "SELECT last_mod FROM files WHERE path = ?", (str(current_path),)
        ).fetchone()
        if cached_row and cached_row[0] == current_mtime:
            return

        # 2. Analyze Audio
        stats = self.audio.get_audio_stats(current_path)
        if not stats:
            return

        try:
            duration, fingerprint = self.audio.get_fingerprint(current_path)
        except Exception:
            logging.error(f"Fingerprinting failed: {current_path}")
            return

        # 3. Check for Duplicates
        match_path, match_stats, similarity = self._find_best_match(fingerprint)

        # 4. Decide Fate
        if match_path:
            self._handle_duplicate(
                current_path, stats, fingerprint, match_path, match_stats, similarity
            )
        else:
            self._handle_unique(current_path, stats, fingerprint, duration)

    def _find_best_match(
        self, fingerprint: str
    ) -> Tuple[Optional[str], Optional[dict], float]:
        """Returns (path, stats, similarity_score) if a match is found."""
        candidates = self.db.find_potential_matches(fingerprint)

        for path in candidates:
            # Get full stats for the candidate
            row = self.db.get_file_record(path)
            if not row:
                continue  # Should not happen due to FK, but safety first

            cached_score, c_fmt, c_bit, c_size, c_fp = row

            # Heavy similarity check
            sim = difflib.SequenceMatcher(None, fingerprint, c_fp).ratio()

            if sim >= self.cfg.SIMILARITY_ASK:
                return (
                    path,
                    {
                        "score": cached_score,
                        "format": c_fmt,
                        "bitrate": c_bit,
                        "size": c_size,
                    },
                    sim,
                )

        return None, None, 0.0

    def _handle_duplicate(
        self,
        new_path: Path,
        new_stats: dict,
        new_fp: str,
        old_path_str: str,
        old_stats: dict,
        similarity: float,
    ):

        # Determine who survives
        decision = "1"  # Default: Keep New (1)

        if similarity >= self.cfg.SIMILARITY_AUTO:
            # Auto-decide based on quality score
            if old_stats["score"] > new_stats["score"]:
                decision = "2"  # Keep Old
        else:
            # Ask User
            decision = self._prompt_user(
                new_path, new_stats, old_path_str, old_stats, similarity
            )

        if decision == "1":
            # New wins. Move Old to dups, Treat New as Unique (re-index it)
            self._move_file(Path(old_path_str), self.cfg.DUP_FOLDER, is_duplicate=True)
            # Re-process new file as if it were unique to ensure it gets tagged/indexed correctly
            duration, _ = self.audio.get_fingerprint(new_path)
            self._handle_unique(new_path, new_stats, new_fp, duration)

        elif decision == "2":
            # Old wins. Move New to dups.
            self._move_file(
                new_path,
                self.cfg.DUP_FOLDER,
                is_duplicate=True,
                stats=new_stats,
                fp=new_fp,
            )
        else:
            # Skip (do nothing)
            pass

    def _handle_unique(
        self, path: Path, stats: dict, fingerprint: str, duration: float
    ):
        # 1. Fetch Tags
        time.sleep(self.cfg.SLEEP_TIME)
        tags = self.audio.fetch_tags(fingerprint, duration)

        # 2. Write Tags & Rename
        final_path = path
        if tags:
            self.audio.write_tags(path, tags, self.cfg.DRY_RUN)

            # Construct new path: Artist/Album/Title.ext
            new_dir = (
                self.cfg.MUSIC_FOLDER
                / sanitize_filename(tags["artist"])
                / sanitize_filename(tags["album"])
            )
            target_name = f"{sanitize_filename(tags['title'])}{path.suffix}"

            final_path = self._move_file(
                path, new_dir, filename=target_name, is_duplicate=False
            )

        # 3. Update DB
        self.db.upsert_file(str(final_path), fingerprint, stats, is_duplicate=False)

    def _move_file(
        self,
        src: Path,
        dest_folder: Path,
        filename: str = None,
        is_duplicate: bool = False,
        stats=None,
        fp=None,
    ) -> Path:

        target_name = filename if filename else src.name
        target_path = get_safe_path(dest_folder / target_name)

        if self.cfg.DRY_RUN:
            logging.info(f"[DRY RUN] Move {src} -> {target_path}")
            return target_path

        try:
            dest_folder.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(target_path))

            # If we just moved a file that was in the index, we need to update the DB
            if is_duplicate:
                # If it's a duplicate, we remove it from the index (searching) but keep in 'files' table
                self.db.delete_index(str(src))
                if stats and fp:
                    self.db.upsert_file(str(target_path), fp, stats, is_duplicate=True)
                else:
                    # Update existing record pointer
                    self.db.conn.execute(
                        "UPDATE files SET path = ?, is_duplicate = 1 WHERE path = ?",
                        (str(target_path), str(src)),
                    )

            return target_path
        except Exception as e:
            logging.error(f"Move failed {src} -> {target_path}: {e}")
            return src

    def _prompt_user(self, new_path, new_stats, old_path, old_stats, similarity) -> str:
        print(f"\n[?] Uncertain Match ({similarity:.1%})")
        print(
            f"   NEW: {new_path.name} | {new_stats['format']} {new_stats['bitrate']//1000}k"
        )
        print(
            f"   OLD: {Path(old_path).name} | {old_stats['format']} {old_stats['bitrate']//1000}k"
        )
        while True:
            choice = input("Keep (1) New, (2) Old, or (S)kip? ").lower()
            if choice in ["1", "2", "s"]:
                return choice

    def cleanup_empty_folders(self):
        """Recursively removes empty folders in the music dir."""
        for root, dirs, _ in os.walk(self.cfg.MUSIC_FOLDER, topdown=False):
            for name in dirs:
                try:
                    (Path(root) / name).rmdir()
                except OSError:
                    pass  # Directory not empty


if __name__ == "__main__":
    # Example usage
    config = Config(
        MUSIC_FOLDER=Path("/mnt/ssk/music/"),
        DUP_FOLDER=Path("/mnt/ssk/duplicates/"),
        DRY_RUN=False,
    )
    print(
        f"starting with music folder: {config.MUSIC_FOLDER} and dup folder: {config.DUP_FOLDER}"
    )
    manager = LibraryManager(config)
    manager.process_library()
    print("Library Sync Complete.")
