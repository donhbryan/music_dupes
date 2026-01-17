import os
import sqlite3
import logging
import acoustid
import mutagen
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
    VALID_EXTS: tuple = (".mp3", ".flac", ".m4a", ".mp4", ".wma", ".wav")
    SLEEP_TIME: float = 0.5
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
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.config.DB_PATH) as conn:
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

    # --- Technical Logic ---
    def _get_blocks(self, fingerprint: str) -> List[str]:
        return [
            fingerprint[i : i + self.config.BLOCK_SIZE]
            for i in range(0, len(fingerprint), self.config.BLOCK_SIZE)
        ][:8]

    def get_audio_stats(self, file_path: Path) -> Optional[Dict[str, Any]]:
        try:
            audio = mutagen.File(file_path)
            if audio is None:
                return None
            info = audio.info
            ext = file_path.suffix.lower()
            bitrate, sr, bits = (
                getattr(info, "bitrate", 0) or 0,
                getattr(info, "sample_rate", 0) or 0,
                getattr(info, "bits_per_sample", 16) or 16,
            )
            score = (
                (2 if ext in [".flac", ".wav", ".alac"] else 1) * 10_000_000
                + bits * 100_000
                + sr
                + (bitrate / 1000)
            )
            return {
                "score": int(score),
                "format": ext,
                "bitrate": int(bitrate),
                "sample_rate": int(sr),
                "bits_per_sample": int(bits),
                "size": file_path.stat().st_size,
                "mtime": file_path.stat().st_mtime,
            }
        except Exception as e:
            logging.error(f"Stats Error {file_path}: {e}")
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
        blocks = self._get_blocks(new_fp)
        query = f"SELECT DISTINCT path FROM fingerprint_index WHERE block IN ({','.join(['?']*len(blocks))})"
        candidates = [row[0] for row in conn.execute(query, blocks).fetchall()]

        for path in candidates:
            row = conn.execute(
                "SELECT score, format, bitrate, file_size, fingerprint FROM files WHERE path = ? AND is_duplicate = 0",
                (path,),
            ).fetchone()
            if not row:
                continue
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
                        self._handle_winner(conn, current_path, fingerprint, stats)
                    elif decision == "2":
                        self._move_to_dups(conn, current_path, fingerprint, stats)

                    conn.commit()
                except Exception as e:
                    logging.error(f"Critical error on {current_path}: {e}")

        self.cleanup_empty_folders()

    def _handle_winner(self, conn, path: Path, fingerprint: str, stats: dict):
        # Metadata and Indexing
        conn.execute(
            """
            INSERT OR REPLACE INTO files 
            (path, fingerprint, score, format, bitrate, sample_rate, bits_per_sample, file_size, last_mod, processed, is_duplicate) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
        """,
            (
                str(path),
                fingerprint,
                stats["score"],
                stats["format"],
                stats["bitrate"],
                stats["sample_rate"],
                stats["bits_per_sample"],
                stats["size"],
                stats["mtime"],
            ),
        )

        conn.execute("DELETE FROM fingerprint_index WHERE path = ?", (str(path),))
        conn.executemany(
            "INSERT INTO fingerprint_index VALUES (?, ?)",
            [(b, str(path)) for b in self._get_blocks(fingerprint)],
        )

    def _move_to_dups(self, conn, file_path: Path, fp=None, stats=None):
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
                        stats["size"],
                        stats["mtime"],
                    ),
                )
            else:
                conn.execute(
                    "UPDATE files SET is_duplicate = 1, path = ? WHERE path = ?",
                    (str(target), str(file_path)),
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


if __name__ == "__main__":
    manager = MusicLibraryManager(Config())
    manager.process_library()
    print("Sync Complete.")
