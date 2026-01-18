"""
This Python script is a sophisticated music library organizer and deduplicator.

It scans a music directory, identifies songs based on their actual audio content (audio fingerprinting), automatically keeps the highest-quality version of duplicate tracks, fetches correct metadata tags, and reorganizes the files into a clean folder structure.

Here is a breakdown of its core functions and logic:

1. Core Capabilities
Audio-Based Deduplication: Unlike simple scripts that compare file names or sizes, this uses AcoustID (Chromaprint) to "listen" to the audio. It can identify that track01.mp3 and Queen - Bohemian Rhapsody.flac are the same song.

Quality Scoring: When it finds duplicates, it calculates a "Quality Score" for each file based on format (FLAC > MP3), bit depth, sample rate, and bitrate. It automatically preserves the higher-quality file and moves the lower-quality one to a "duplicates" folder.

Automatic Tagging: It queries the AcoustID API to fetch the correct Title, Artist, Album, and Year for unidentified tracks and writes these tags to the file metadata (ID3, Vorbis, etc.).

File Organization: It renames and moves files into a standardized structure: Artist / Album / Title.ext.

Incremental Sync: It uses a local SQLite database to track processed files. If a file hasn't been modified since the last run, the script skips it to save time.

"""

import os
import inspect
import sqlite3
import logging
import acoustid  # type: ignore
import shutil
import re
import time
import csv
import difflib
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List
from tqdm import tqdm
from mutagen.asf import ASF
import mutagen


# --- Configuration ---
@dataclass
class Config:
    API_KEY: str = "7dlZplmc3N"
    MUSIC_FOLDER: Path = Path("./data/music")
    DUP_FOLDER: Path = Path("./data/dups")
    DB_PATH: str = "library_manager.db"
    EXPORT_PATH: str = "music_library_report.csv"
    DRY_RUN: bool = False
    LOG_FILE: str = "library_manager.log"
    VALID_EXTS: tuple[Any, ...] = (
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
    SLEEP_TIME: float = 0.01
    SIMILARITY_AUTO: float = 0.98
    SIMILARITY_ASK: float = 0.95
    BLOCK_SIZE: int = 16


logging.basicConfig(
    filename=Config.LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class MusicLibraryManager:
    def __init__(self, config: Config):
        self.config = config
        # with open(self.config.LOG_FILE, "w") as f:
        #     f.write("")
        self._init_db()

    def _init_db(self):
        try:
            with sqlite3.connect(self.config.DB_PATH) as conn:
                # conn.execute("DROP TABLE IF EXISTS files")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS files (
                        path TEXT PRIMARY KEY, fingerprint TEXT, score INTEGER,
                        format TEXT, bitrate INTEGER, sample_rate INTEGER,
                        bits_per_sample INTEGER, file_size INTEGER,
                        last_mod REAL, processed INTEGER DEFAULT 0, is_duplicate INTEGER DEFAULT 0
                    )
                """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fingerprint_index (
                        block TEXT, path TEXT,
                        FOREIGN KEY(path) REFERENCES files(path)
                    )
                """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_block ON fingerprint_index(block)"
                )
        except Exception as e:
            current_function_name = inspect.currentframe().f_code.co_name
            logging.critical(
                f"{current_function_name} Database initialization error for {self.config.DB_PATH}: {e}"
            )

    # --- Technical Logic ---
    def _get_blocks(self, fingerprint: str) -> List[str]:
        return [
            fingerprint[i : i + self.config.BLOCK_SIZE]
            for i in range(0, len(fingerprint), self.config.BLOCK_SIZE)
        ][:8]

    def get_audio_stats(self, file_path: Path) -> Optional[Dict[str, Any]]:
        try:
            audio = mutagen.File(
                file_path
            )  # pyright: ignore[reportUnknownVariableType]
            if audio is None:
                return None
            info: any = audio.info  # pyright: ignore[reportUnknownVariableType]
            ext = file_path.suffix.lower()
            mtime = file_path.stat().st_mtime

            stats = {
                "format": getattr(info, "format", ext),
                "bitrate": getattr(info, "bitrate", 0),
                "sample_rate": getattr(info, "sample_rate", 0),
                "bits_per_sample": getattr(info, "bits_per_sample", 0),
                "file_size": file_path.stat().st_size,
                "last_mod": file_path.stat().st_mtime,
                "mtime": mtime,
            }  # pyright: ignore[reportUnknownVariableType]
            stats["score"] = int(
                (2 if ext in [".flac", ".wav", ".alac"] else 1) * 10_000_000
                + stats["bits_per_sample"] * 100_000
                + stats["sample_rate"]
                + (
                    stats["bitrate"] / 1000
                )  # pyright: ignore[reportUnknownVariableType]
            )
            return stats
        except Exception as e:
            current_function_name = inspect.currentframe().f_code.co_name
            logging.info(
                f"{current_function_name} Failed to get audio stats for {file_path}: {e}"
            )
            return None

    def resolve_conflict(
        self,
        new_path: Path,
        new_stats: dict,
        old_path: str,
        old_stats: dict,
        similarity: float,
    ) -> str:
        print(f"\n[!] UNCERTAIN MATCH ({similarity:.1%})")
        print(
            f"1. NEW: {new_path.name} ({new_stats['format']}, {new_stats['bitrate']//1000}kbps, {new_stats['size']/(1024**2):.1f}MB)"
        )
        print(
            f"2. OLD: {Path(old_path).name} ({old_stats['format']}, {old_stats['bitrate']//1000}kbps, {old_stats['size']/(1024**2):.1f}MB)"
        )
        while True:
            choice = input("Keep (1) New, (2) Old, or (S)kip? [1/2/S]: ").lower()
            if choice in ["1", "2", "s"]:
                return choice

    # --- Index-Based Fuzzy Matching ---
    def find_match(
        self, conn, new_fp: str
    ) -> Tuple[Optional[str], Optional[dict], float]:
        """Uses the index to find candidate matches, then performs heavy similarity check."""

        blocks = self._get_blocks(new_fp)

        # 1. Find candidates that share at least one block
        query = f"SELECT DISTINCT path FROM fingerprint_index WHERE block IN ({','.join(['?']*len(blocks))})"
        candidates = [row[0] for row in conn.execute(query, blocks).fetchall()]

        # 2. Refined comparison (SequenceMatcher) only on candidates
        for path in candidates:
            row = conn.execute(
                "SELECT score, format, bitrate, file_size, fingerprint FROM files WHERE path = ? AND is_duplicate = 0",
                (path,),
            ).fetchone()
            if not row:
                continue
            # SequenceMatcher is perfect for finding similarity in long strings
            sim = difflib.SequenceMatcher(None, new_fp, row[4]).ratio()
            if sim >= self.config.SIMILARITY_ASK:
                return (
                    path,
                    {
                        "score": row[0],
                        "format": row[1],
                        "bitrate": row[2],
                        "size": row[3],
                    },
                    sim,
                )
        return None, None, 0.0

    # --- Tagging & Organizing ---
    def fetch_tags(self, fingerprint: str, duration: float) -> Dict[str, str]:
        try:
            time.sleep(self.config.SLEEP_TIME)
            results = acoustid.lookup(
                self.config.API_KEY, fingerprint, duration, meta="recordings releases"
            )
            if results["status"] == "ok" and results.get("results"):
                rec = results["results"][0].get("recordings", [{}])[
                    0
                ]  # pyright: ignore[reportUnknownVariableType]
                release = rec.get("releases", [{}])[0]
                return {
                    "title": rec.get("title", "Unknown Track"),
                    "artist": rec.get("artists", [{}])[0].get("name", "Unknown Artist"),
                    "album": release.get("title", "Unknown Album"),
                    "date": str(release.get("date", {}).get("year", "")),
                }
        except Exception:
            current_function_name = inspect.currentframe().f_code.co_name
            logging.info(
                f"{current_function_name} No tag info for [ {fingerprint} ]: {e}"
            )
            pass
        return {}

    def organize_and_tag(self, file_path: Path, tags: Dict[str, str]) -> Path:
        if not tags:
            return file_path
        try:
            if not self.config.DRY_RUN:
                if file_path.suffix.lower() == ".wma":
                    audio = ASF(file_path)
                    audio["WM/AlbumTitle"], audio["Author"], audio["Title"] = (
                        [tags["album"]],
                        [tags["artist"]],
                        [tags["title"]],
                    )
                    if tags["date"]:
                        audio["WM/Year"] = [tags["date"]]
                    audio.save()
                else:
                    audio = mutagen.File(file_path, easy=True)
                    if audio:
                        for key in ["title", "artist", "album", "date"]:
                            if tags.get(key):
                                audio[key] = tags[key]
                        audio.save()
        except Exception as e:
            current_function_name = inspect.currentframe().f_code.co_name
            logging.error(f"{current_function_name} Tag Write Error {file_path}: {e}")

        new_dir = (
            self.config.MUSIC_FOLDER
            / self.sanitize(tags["artist"])
            / self.sanitize(tags["album"])
        )
        target_path = new_dir / f"{self.sanitize(tags['title'])}{file_path.suffix}"

        if not self.config.DRY_RUN:
            new_dir.mkdir(parents=True, exist_ok=True)
            final_path = self.get_safe_path(target_path)
            shutil.move(str(file_path), str(final_path))
            return final_path
        return target_path

    # --- Utility & Stats ---
    def sanitize(self, text: str) -> str:
        if not text:
            return "Unknown"
        clean = re.sub(r'[\\/*?:"<>|]', "", str(text)).strip()
        return clean[:150]

    # --- Core Processing Loop ---
    def process_library(self):
        self.config.DUP_FOLDER.mkdir(parents=True, exist_ok=True)
        all_files = [
            Path(r) / f
            for r, _, fs in os.walk(self.config.MUSIC_FOLDER)
            for f in fs
            if f.lower().endswith(self.config.VALID_EXTS)
        ]

        with sqlite3.connect(self.config.DB_PATH) as conn:
            for current_path in tqdm(all_files, desc="Incremental Sync"):
                mtime = current_path.stat().st_mtime

                # INCREMENTAL CHECK: Skip if path exists AND mtime hasn't changed
                cursor = conn.execute(
                    "SELECT last_mod FROM files WHERE path = ?", (str(current_path),)
                )
                row = cursor.fetchone()
                if row and row[0] == mtime:
                    continue

                try:
                    duration, fingerprint = acoustid.fingerprint_file(str(current_path))
                    stats = self.get_audio_stats(current_path)
                    if not stats:
                        continue

                    match_path, match_stats, similarity = self.find_match(
                        conn, fingerprint
                    )

                    decision = "1"
                    if match_path:
                        if similarity >= self.config.SIMILARITY_AUTO:
                            decision = (
                                "1" if stats["score"] > match_stats["score"] else "2"
                            )
                        else:
                            decision = self.resolve_conflict(
                                current_path, stats, match_path, match_stats, similarity
                            )

                    if decision == "1":
                        if match_path:
                            self._move_to_dups(conn, Path(match_path))
                        self._handle_winner(conn, current_path, fingerprint, stats)  # type: ignore
                    elif decision == "2":
                        self._move_to_dups(conn, current_path, fingerprint, stats)  # type: ignore

                    conn.commit()
                except Exception as e:
                    current_function_name = inspect.currentframe().f_code.co_name
                    logging.critical(
                        f"{current_function_name} Critical error on {current_path}: {e}"
                    )

        self.cleanup_empty_folders()

    def _handle_winner(self, conn, path: Path, fingerprint: str, stats: dict):  # type: ignore
        """Organizes, tags, and updates both metadata and index tables."""
        try:
            # Metadata and Indexing
            duration, _ = acoustid.fingerprint_file(str(path))
            tags = self.fetch_tags(fingerprint, duration)
            final_path = self.organize_and_tag(path, tags)

            # Update Main Table
            conn.execute(
                """
                INSERT OR REPLACE INTO files 
                (path, fingerprint, score, format, bitrate, sample_rate, bits_per_sample, file_size, last_mod, processed, is_duplicate) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
            """,
                (
                    str(final_path),
                    fingerprint,
                    stats["score"],
                    stats["format"],
                    stats["bitrate"],
                    stats["sample_rate"],
                    stats["bits_per_sample"],
                    stats["file_size"],
                    stats["mtime"],
                ),
            )

            # Update Index Table
            conn.execute("DELETE FROM fingerprint_index WHERE path = ?", (str(path),))
            conn.executemany(
                "INSERT INTO fingerprint_index VALUES (?, ?)",
                [(b, str(path)) for b in self._get_blocks(fingerprint)],
            )
        except Exception as e:
            current_function_name = inspect.currentframe().f_code.co_name
            logging.critical(f"{current_function_name} Database update Error: {e}")

    def _move_to_dups(self, conn, file_path: Path, fp=None, stats=None):
        try:
            target = self.get_safe_path(self.config.DUP_FOLDER / file_path.name)
            if not self.config.DRY_RUN and file_path.exists():
                shutil.move(str(file_path), str(target))
                conn.execute(
                    "DELETE FROM fingerprint_index WHERE path = ?", (str(file_path),)
                )
                if stats:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO files 
                        (path, fingerprint, score, format, bitrate, sample_rate, bits_per_sample, file_size, last_mod, processed, is_duplicate) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
                    """,
                        (
                            str(target),
                            fp,
                            stats["score"],
                            stats["format"],
                            stats["bitrate"],
                            stats["sample_rate"],
                            stats["bits_per_sample"],
                            stats["file_size"],
                            stats["mtime"],
                        ),
                    )
                else:
                    conn.execute(
                        "UPDATE files SET is_duplicate = 1, path = ? WHERE path = ?",
                        (str(target), str(file_path)),
                    )
        except Exception as e:
            current_function_name = inspect.currentframe().f_code.co_name
            logging.critical(
                f"{current_function_name} FIle Move Database update Error {file_path} --> {target}: {e}"
            )

    def get_safe_path(self, target_path: Path) -> Path:
        counter = 1
        base = target_path
        while target_path.exists():
            target_path = base.with_name(f"{base.stem}_{counter:02d}{base.suffix}")
            counter += 1
        return target_path

    def cleanup_empty_folders(self):
        for root, dirs, _ in os.walk(self.config.MUSIC_FOLDER, topdown=False):
            for n in dirs:
                try:
                    (Path(root) / n).rmdir()
                except Exception:
                    pass

    def export_library_data(self):
        """Exports the database to a CSV file for spreadsheet analysis."""
        print(f"Exporting library data to {self.config.EXPORT_PATH}...")
        with sqlite3.connect(self.config.DB_PATH) as conn:
            cursor = conn.execute(
                """
                SELECT 
                path, score ,
                format , bitrate , sample_rate ,
                bits_per_sample , file_size ,
                last_mod , processed , is_duplicate , fingerprint
                FROM files
                """
            )
            rows = cursor.fetchall()
            headers = [description[0] for description in cursor.description]

        with open(self.config.EXPORT_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

    def generate_report(self):
        with sqlite3.connect(self.config.DB_PATH) as conn:
            total = conn.execute(
                "SELECT COUNT(*), SUM(file_size) FROM files WHERE is_duplicate = 0"
            ).fetchone()
            dups = conn.execute(
                "SELECT COUNT(*), SUM(file_size) FROM files WHERE is_duplicate = 1"
            ).fetchone()
        print(
            f"\n{'='*30}\nREPORT: {total[0]} Unique Tracks | {(total[1] or 0)/(1024**3):.2f} GB | Dups removed: {dups[0] or 0}\n{'='*30}"
        )


if __name__ == "__main__":
    manager = MusicLibraryManager(Config())
    manager.process_library()
    manager.export_library_data()
    manager.generate_report()
    print("Sync Complete.")
