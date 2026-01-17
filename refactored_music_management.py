import os
import sqlite3
import logging
import acoustid
import mutagen
import shutil
import re
import time
import csv
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any
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
                )
            """
            )

    # --- Utility & Stats ---
    def sanitize(self, text: str) -> str:
        if not text:
            return "Unknown"
        clean = re.sub(r'[\\/*?:"<>|]', "", str(text)).strip()
        return clean[:150]

    def get_safe_path(self, target_path: Path) -> Path:
        counter = 1
        base = target_path
        while target_path.exists():
            target_path = base.with_name(f"{base.stem} ({counter:02d}){base.suffix}")
            counter += 1
        return target_path

    def get_audio_stats(self, file_path: Path) -> Optional[Dict[str, Any]]:
        try:
            audio = mutagen.File(file_path)
            if audio is None:
                return None
            info = audio.info
            ext = file_path.suffix.lower()

            bitrate = getattr(info, "bitrate", 0) or 0
            sample_rate = getattr(info, "sample_rate", 0) or 0
            bits_per_sample = getattr(info, "bits_per_sample", 16) or 16

            fmt_priority = 2 if ext in [".flac", ".wav", ".alac"] else 1
            quality_score = (
                (fmt_priority * 10_000_000)
                + (bits_per_sample * 100_000)
                + sample_rate
                + (bitrate / 1000)
            )

            return {
                "score": int(quality_score),
                "format": ext,
                "bitrate": int(bitrate),
                "sample_rate": int(sample_rate),
                "bits_per_sample": int(bits_per_sample),
                "size": file_path.stat().st_size,
            }
        except Exception as e:
            logging.error(f"Stats Error {file_path}: {e}")
            return None

    # --- Tagging & Organizing ---
    def fetch_tags(self, fingerprint: str, duration: float) -> Dict[str, str]:
        try:
            time.sleep(self.config.SLEEP_TIME)
            results = acoustid.lookup(
                self.config.API_KEY, fingerprint, duration, meta="recordings releases"
            )
            if results["status"] == "ok" and results.get("results"):
                rec = results["results"][0].get("recordings", [{}])[0]
                release = rec.get("releases", [{}])[0]
                return {
                    "title": rec.get("title", "Unknown Track"),
                    "artist": rec.get("artists", [{}])[0].get("name", "Unknown Artist"),
                    "album": release.get("title", "Unknown Album"),
                    "date": str(release.get("date", {}).get("year", "")),
                }
        except Exception:
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
            logging.error(f"Tag Write Error {file_path}: {e}")

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

    # --- Core Processing ---
    def process_library(self):
        self.config.DUP_FOLDER.mkdir(parents=True, exist_ok=True)
        all_files = [
            Path(r) / f
            for r, _, fs in os.walk(self.config.MUSIC_FOLDER)
            for f in fs
            if f.lower().endswith(self.config.VALID_EXTS)
        ]

        with sqlite3.connect(self.config.DB_PATH) as conn:
            for current_path in tqdm(all_files, desc="Syncing Library"):
                try:
                    cursor = conn.execute(
                        "SELECT processed FROM files WHERE path = ?",
                        (str(current_path),),
                    )
                    if (row := cursor.fetchone()) and row[0] == 1:
                        continue

                    duration, fingerprint = acoustid.fingerprint_file(str(current_path))
                    stats = self.get_audio_stats(current_path)
                    if not stats:
                        continue

                    cursor = conn.execute(
                        "SELECT path, score FROM files WHERE fingerprint = ? AND is_duplicate = 0",
                        (fingerprint,),
                    )
                    existing = cursor.fetchone()

                    if existing:
                        existing_path, existing_score = Path(existing[0]), existing[1]
                        if stats["score"] > existing_score:
                            self._move_to_dups(conn, existing_path)
                            self._handle_winner(conn, current_path, fingerprint, stats)
                        else:
                            self._move_to_dups(conn, current_path, fingerprint, stats)
                    else:
                        self._handle_winner(conn, current_path, fingerprint, stats)
                    conn.commit()
                except Exception as e:
                    logging.error(f"Error {current_path}: {e}")

        self.cleanup_empty_folders()

    def _handle_winner(self, conn, path, fingerprint, stats):
        duration, _ = acoustid.fingerprint_file(str(path))
        tags = self.fetch_tags(fingerprint, duration)
        final_path = self.organize_and_tag(path, tags)
        conn.execute(
            """INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 0)""",
            (
                str(final_path),
                fingerprint,
                stats["score"],
                stats["format"],
                stats["bitrate"],
                stats["sample_rate"],
                stats["bits_per_sample"],
                stats["size"],
            ),
        )

    def _move_to_dups(self, conn, file_path, fingerprint=None, stats=None):
        target = self.get_safe_path(self.config.DUP_FOLDER / file_path.name)
        if not self.config.DRY_RUN and file_path.exists():
            shutil.move(str(file_path), str(target))
            if stats:
                conn.execute(
                    "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1)",
                    (
                        str(target),
                        fingerprint,
                        stats["score"],
                        stats["format"],
                        stats["bitrate"],
                        stats["sample_rate"],
                        stats["bits_per_sample"],
                        stats["size"],
                    ),
                )
            else:
                conn.execute(
                    "UPDATE files SET is_duplicate = 1, path = ? WHERE path = ?",
                    (str(target), str(file_path)),
                )

    def cleanup_empty_folders(self):
        for root, dirs, _ in os.walk(self.config.MUSIC_FOLDER, topdown=False):
            for name in dirs:
                try:
                    (Path(root) / name).rmdir()
                except Exception:
                    pass

    def export_library_data(self):
        """Exports the database to a CSV file for spreadsheet analysis."""
        print(f"Exporting library data to {self.config.EXPORT_PATH}...")
        with sqlite3.connect(self.config.DB_PATH) as conn:
            cursor = conn.execute("SELECT * FROM files")
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
            f"\n{'='*30}\nREPORT: {total[0]} Tracks | {(total[1] or 0)/(1024**3):.2f} GB | Dups removed: {dups[0] or 0}\n{'='*30}"
        )


if __name__ == "__main__":
    manager = MusicLibraryManager(Config())
    manager.process_library()
    manager.export_library_data()
    manager.generate_report()
