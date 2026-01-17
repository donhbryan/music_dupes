MUSIC LIBRARY MANAGEMENT

I have added a Time-Aware Syncing mechanism. By tracking the mtime (last modified time) of files, the script can now skip over thousands of files in milliseconds, only calculating fingerprints and querying the API for brand-new or recently edited tracks.

This turns the script from a "one-time fix" into a permanent library daemon that you can run daily.

Finalized Music Manager with Incremental Sync & Indexing

How to use the Conflict Resolver
Above 98%: The script automatically keeps the better file (e.g., it will always pick the FLAC over the MP3 without asking).

95% to 98%: The script will pause and print a comparison table.

Press 1 if the new file is the one you want.

Press 2 if the one already in the library is better.

Press S if they are actually different songs (e.g., a "Radio Edit" vs. "Extended Mix" that are very similar) to keep both.

Different FPCALC fingerprints for the same audio stored in different formats (FLAC vs MP3).

Generally, yes, but not always bit-for-bit identical.
fpcalc (the command-line tool for Chromaprint/AcoustID) is designed to be perceptually robust. Its goal is to generate a fingerprint based on the audio content rather than the file's digital container or encoding method.Here is a breakdown of why they match and why they might occasionally differ.1. How it Works: Perceptual HashingUnlike a standard file hash (like MD5 or SHA-1) which changes if even a single bit of the file is altered, fpcalc analyzes the spectral characteristics of the audio.It looks at the frequencies that are present over time. Because a FLAC file (lossless) and a high-quality MP3 (lossy) of the same song share the same frequency structure, the resulting fingerprints are usually identical or extremely similar.2. When the Fingerprints MatchIn most cases, if you have an MP3 and a FLAC of the same recording:The Fingerprint String: The long encoded string will likely be identical.The AcoustID: When sent to the AcoustID server, they will almost certainly resolve to the same unique ID.3. Why They Might DifferEven though they are the same song, the fingerprints might vary slightly due to these factors:Sample Rate Differences: If the FLAC is 96kHz and the MP3 is 44.1kHz, the resampling process used by fpcalc before analysis can cause minor variations.Mastering Variations: If the MP3 was made from a different "remaster" than the FLAC, the audio energy in certain frequencies will differ, leading to different fingerprints.Silence/Padding: If the MP3 has an extra 200ms of silence at the beginning (common with some MP3 encoders), the "alignment" of the fingerprint shifts.Heavy Compression: At very low bitrates (e.g., 64kbps MP3), the lossy encoder removes so much high-frequency data that the "spectrogram" changes enough to alter the fingerprint.Summary TableFeatureMD5 / SHA-1 Hashfpcalc (Chromaprint)FocusBinary DataAudio ContentMP3 vs. FLACAlways DifferentUsually SameResistant toNothingBitrate, Format, VolumePrimary UseIntegrity / SecurityIdentification / DeduplicationPro-Tip for your Refactored ScriptIn your current script, you are using the fingerprint as a PRIMARY KEY or a unique identifier. Since there is a small chance fingerprints could vary by a few characters while still being the "same" song, AcoustID actually provides a Lookup API that returns a "Clustered ID."Would you like me to update your process_library logic to handle cases where fingerprints are 99% similar rather than just a perfect 100% string match?

Implementing a similarity threshold (like 98%) is a much more robust way to handle the differences between MP3 and FLAC. In the world of audio fingerprinting, these are called "near-collisions."

To do this effectively, we can't just use INSERT OR REPLACE based on a string. Instead, we use the Levenshtein distance (or a simple Jaccard similarity) to compare the new fingerprint against existing ones in your database.

Updated Logic: 98% Similarity Deduplication
I have added a calculate_similarity helper and updated the process_library loop to look for "fuzzy" matches instead of exact ones.

Python

import os
import sqlite3
import logging
import acoustid
import mutagen
import shutil
import re
import time
import csv
import difflib  # Added for similarity comparison
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
from tqdm import tqdm
from mutagen.asf import ASF 

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
    SIMILARITY_THRESHOLD: float = 0.98  # 98% Match requirement

logging.basicConfig(filename=Config.LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class MusicLibraryManager:
    def __init__(self, config: Config):
        self.config = config
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.config.DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    fingerprint TEXT,
                    score INTEGER,
                    format TEXT,
                    bitrate INTEGER,
                    sample_rate INTEGER,
                    bits_per_sample INTEGER,
                    file_size INTEGER,
                    processed INTEGER DEFAULT 0,
                    is_duplicate INTEGER DEFAULT 0
                )
            """)

    def calculate_similarity(self, fp1: str, fp2: str) -> float:
        """Calculates the similarity ratio between two fingerprint strings."""
        if not fp1 or not fp2: return 0.0
        # SequenceMatcher is perfect for finding similarity in long strings
        return difflib.SequenceMatcher(None, fp1, fp2).ratio()

    def find_fuzzy_match(self, conn, new_fp: str) -> Optional[Tuple[str, int, float]]:
        """Scans the DB for a fingerprint that is at least X% similar."""
        cursor = conn.execute("SELECT path, score, fingerprint FROM files WHERE is_duplicate = 0")
        for path, score, existing_fp in cursor.fetchall():
            similarity = self.calculate_similarity(new_fp, existing_fp)
            if similarity >= self.config.SIMILARITY_THRESHOLD:
                return path, score, similarity
        return None

    def get_audio_stats(self, file_path: Path) -> Optional[Dict[str, Any]]:
        try:
            audio = mutagen.File(file_path)
            if audio is None: return None
            info = audio.info
            ext = file_path.suffix.lower()
            
            bitrate = getattr(info, "bitrate", 0) or 0
            sample_rate = getattr(info, "sample_rate", 0) or 0
            bits_per_sample = getattr(info, "bits_per_sample", 16) or 16
            
            fmt_priority = 2 if ext in [".flac", ".wav", ".alac"] else 1
            quality_score = (fmt_priority * 10_000_000) + (bits_per_sample * 100_000) + sample_rate + (bitrate / 1000)

            return {
                "score": int(quality_score),
                "format": ext, "bitrate": int(bitrate),
                "sample_rate": int(sample_rate), "bits_per_sample": int(bits_per_sample),
                "size": file_path.stat().st_size,
            }
        except Exception as e:
            logging.error(f"Stats Error {file_path}: {e}")
            return None

    def process_library(self):
        self.config.DUP_FOLDER.mkdir(parents=True, exist_ok=True)
        all_files = [Path(r)/f for r,_,fs in os.walk(self.config.MUSIC_FOLDER) for f in fs if f.lower().endswith(self.config.VALID_EXTS)]

        with sqlite3.connect(self.config.DB_PATH) as conn:
            for current_path in tqdm(all_files, desc="Processing (Fuzzy Match)"):
                try:
                    # Check if path is already handled
                    cursor = conn.execute("SELECT processed FROM files WHERE path = ?", (str(current_path),))
                    if (row := cursor.fetchone()) and row[0] == 1: continue

                    duration, fingerprint = acoustid.fingerprint_file(str(current_path))
                    stats = self.get_audio_stats(current_path)
                    if not stats: continue

                    # Use FUZZY MATCH instead of exact fingerprint lookup
                    match = self.find_fuzzy_match(conn, fingerprint)

                    if match:
                        existing_path, existing_score, sim_ratio = match
                        logging.info(f"Fuzzy Match Found ({sim_ratio:.1%}): {current_path.name} vs {Path(existing_path).name}")
                        
                        if stats["score"] > existing_score:
                            # New file is higher quality (e.g. FLAC replacing MP3)
                            self._move_to_dups(conn, Path(existing_path))
                            self._handle_winner(conn, current_path, fingerprint, stats)
                        else:
                            # New file is lower quality or same
                            self._move_to_dups(conn, current_path, fingerprint, stats)
                    else:
                        # No similar file found, unique track
                        self._handle_winner(conn, current_path, fingerprint, stats)
                    
                    conn.commit()
                except Exception as e:
                    logging.error(f"Critical error on {current_path}: {e}")

    # --- Support methods (organize_and_tag, _handle_winner, _move_to_dups, fetch_tags) remain the same ---

    def _handle_winner(self, conn, path, fingerprint, stats):
        # We assume fetch_tags and organize_and_tag are defined as in the previous step
        duration, _ = acoustid.fingerprint_file(str(path))
        tags = self.fetch_tags(fingerprint, duration)
        # Note: In a real run, define fetch_tags and organize_and_tag in your class
        final_path = self.organize_and_tag(path, tags) 
        
        conn.execute("""INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 0)""", 
                     (str(final_path), fingerprint, stats["score"], stats["format"], stats["bitrate"], 
                      stats["sample_rate"], stats["bits_per_sample"], stats["size"]))

    def _move_to_dups(self, conn, file_path: Path, fingerprint: str = None, stats: dict = None):
        target = self.get_safe_path(self.config.DUP_FOLDER / file_path.name)
        if not self.config.DRY_RUN and file_path.exists():
            shutil.move(str(file_path), str(target))
            if stats:
                conn.execute("INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1)",
                             (str(target), fingerprint, stats["score"], stats["format"], stats["bitrate"], 
                              stats["sample_rate"], stats["bits_per_sample"], stats["size"]))
            else:
                conn.execute("UPDATE files SET is_duplicate = 1, path = ? WHERE path = ?", (str(target), str(file_path)))

    def fetch_tags(self, fingerprint, duration):
        # Placeholder for the fetch_tags logic from the previous turn
        return {} # Implementation omitted for brevity but should be included

    def organize_and_tag(self, path, tags):
        # Placeholder for the organize_and_tag logic from the previous turn
        return path # Implementation omitted for brevity but should be included

    def export_library_data(self):
        with sqlite3.connect(self.config.DB_PATH) as conn:
            cursor = conn.execute("SELECT * FROM files")
            with open(self.config.EXPORT_PATH, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([d[0] for d in cursor.description])
                writer.writerows(cursor.fetchall())

if __name__ == "__main__":
    manager = MusicLibraryManager(Config())
    manager.process_library()
    manager.export_library_data()
Why this is better for MP3 vs FLAC
** difflib.SequenceMatcher:** This compares the "DNA" of the fingerprint. Even if an MP3 encoder shifted the audio by a few milliseconds, the sequence remains mostly the same, allowing for that 98% match.

Quality Replacement: If the script finds a 99% match between an MP3 and a FLAC, it will check the score. Since FLAC is lossless, it gets a higher score and will move the MP3 to the dups folder automatically, "upgrading" your library.

Efficiency: It only does the heavy fuzzy comparison for files that haven't been processed yet.

Caution: Fuzzy matching requires a full scan of the database for every new file. If your library is over 50,000 tracks, this could be slow. Would you like me to optimize this using a K-D Tree or an Indexing strategy to make the search near-instant?