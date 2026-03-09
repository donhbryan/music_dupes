import os
import sys
import time
import subprocess
import sqlite3
import logging
import shutil
import acoustid
import difflib
import mutagen
import traceback
import json
import hashlib
from mutagen.id3 import ID3, TPE1, TPE2, TRCK, TPOS, TIT2, TALB
from tqdm import tqdm


class MusicLibraryManager:
    def __init__(self, config_file="library_management_config.json"):
        # Load the configuration from the JSON file
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON configuration: {e}")

        # Map the JSON keys to class attributes with fallback defaults
        self.api_key = config.get("api_key")
        self.music_folder = os.path.abspath(config.get("music_folder", ""))
        self.destination_folder = os.path.abspath(config.get("destination_folder", ""))
        self.dup_folder = os.path.abspath(config.get("dup_folder", ""))
        self.unresolved_folder = os.path.abspath(config.get("unresolved_folder", ""))
        self.db_path = config.get("db_path", "library_manager.db")

        # Safely parse booleans to handle strings like "false", "True", or 0
        def parse_bool(val, default=False):
            if isinstance(val, str):
                return val.strip().lower() in ("true", "1", "yes", "t")
            if val is None:
                return default
            return bool(val)

        self.dry_run = parse_bool(config.get("dry_run", False))
        self.prune = parse_bool(config.get("prune", False))
        self.prepopulate = parse_bool(config.get("prepopulate", False))
        self.run_process = parse_bool(config.get("process", True))

        self.player_process = None

        # Tuning for fuzzy matching
        self.BLOCK_SIZE = 16
        self.SIMILARITY_AUTO = 0.98
        self.SIMILARITY_STICKY = 0.95
        self.SIMILARITY_ASK = 0.85
        self.API_SLEEP = 0.4

        # State tracking for sticky album selection
        self.last_selected_album_id = None

        logging.basicConfig(
            filename="library_manager.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        self._setup_database()

        if not os.path.exists(self.dup_folder):
            os.makedirs(self.dup_folder)

        if not os.path.exists(self.destination_folder):
            os.makedirs(self.destination_folder)

        if not os.path.exists(self.unresolved_folder):
            os.makedirs(self.unresolved_folder)

    def _setup_database(self):
        """Creates the normalized database schema."""
        self.cur.execute("PRAGMA foreign_keys = ON")

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS albums (
                            release_id TEXT PRIMARY KEY,
                            album_title TEXT,
                            album_artist TEXT,
                            release_date TEXT,
                            country TEXT
                        )"""
        )

        self.cur.execute(
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
                            date_modified DATETIME DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (album_id) REFERENCES albums (release_id)
                        )"""
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_acoustid ON files(acoustid_id)"
        )

        # --- Safe migration for existing databases ---
        try:
            self.cur.execute(
                "ALTER TABLE files ADD COLUMN date_modified DATETIME DEFAULT CURRENT_TIMESTAMP"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists

        # --- Trigger to auto-update timestamp on modifications ---
        self.cur.execute(
            """CREATE TRIGGER IF NOT EXISTS update_files_modtime
               AFTER UPDATE ON files
               FOR EACH ROW
               BEGIN
                   UPDATE files SET date_modified = CURRENT_TIMESTAMP WHERE path = old.path;
               END;"""
        )
        
        # Fingerprint History
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS known_fingerprints (
                            fingerprint TEXT,
                            acoustid_id TEXT,
                            PRIMARY KEY (fingerprint, acoustid_id)
                        )"""
        )

        # Fingerprint Blocks
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS known_blocks (
                            block TEXT,
                            acoustid_id TEXT
                        )"""
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_known_blocks ON known_blocks(block)"
        )

        # Fingerprint Index (File Path based - for local dedup)
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS fingerprint_index (
                            block TEXT,
                            path TEXT,
                            FOREIGN KEY(path) REFERENCES files(path) ON DELETE CASCADE
                        )"""
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_blocks ON fingerprint_index(block)"
        )

        # Exact File Hash Tracking
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS file_hashes (
                            file_hash TEXT PRIMARY KEY,
                            path TEXT
                        )"""
        )

        self.conn.commit()

    def prune_database(self):
        """Checks DB entries against filesystem and removes non-existent files."""
        if not os.path.exists(self.music_folder):
            logging.error("Music folder not found. Skipping prune.")
            return

        print("Checking for ghost entries in database...")
        removed_count = 0

        with self.conn:
            cursor = self.conn.execute("SELECT path FROM files")
            all_paths = cursor.fetchall()

            for (path_str,) in tqdm(all_paths, desc="Pruning DB"):
                if not os.path.exists(path_str):
                    self.conn.execute("DELETE FROM files WHERE path = ?", (path_str,))
                    self.conn.execute(
                        "DELETE FROM fingerprint_index WHERE path = ?", (path_str,)
                    )
                    self.conn.execute(
                        "DELETE FROM file_hashes WHERE path = ?", (path_str,)
                    )
                    removed_count += 1

        if removed_count > 0:
            logging.info("Pruned %d ghost entries from database.", removed_count)
            print(f"cleaned up {removed_count} missing files from the database.")
        else:
            print("Database is clean.")

    def _get_file_hash(self, filepath):
        """Calculates a fast MD5 hash of the file for exact duplicate detection."""
        hasher = hashlib.md5()
        try:
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096 * 1024), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except OSError as e:
            logging.error(f"Hashing failed for {filepath}: {e}")
            return None

    def prepopulate_hashes(self):
        """Scans the database for already-processed files and retroactively generates their MD5 hashes."""
        print("Checking for existing files that need to be hashed...")

        self.cur.execute("SELECT path FROM files")
        known_paths = [row[0] for row in self.cur.fetchall()]

        if not known_paths:
            print("No existing files found in the database to hash.")
            return

        added_count = 0

        for path in tqdm(known_paths, desc="Hashing existing files"):
            if not os.path.exists(path):
                continue

            self.cur.execute("SELECT 1 FROM file_hashes WHERE path = ?", (path,))
            if self.cur.fetchone():
                continue

            file_hash = self._get_file_hash(path)
            if file_hash:
                self.cur.execute(
                    "INSERT OR REPLACE INTO file_hashes (file_hash, path) VALUES (?, ?)",
                    (file_hash, path),
                )
                added_count += 1

        self.conn.commit()

        if added_count > 0:
            print(f"Successfully added {added_count} new file hashes to the database.")
        else:
            print("All known files are already hashed.")

    # --- FINGERPRINT ENGINE ---
    def _get_blocks(self, fingerprint):
        """Splits fingerprint into chunks for indexing."""
        return [
            fingerprint[i : i + self.BLOCK_SIZE]
            for i in range(0, len(fingerprint), self.BLOCK_SIZE)
        ][:16]

    def _update_fingerprint_cache(self, acoustid_id, fingerprint):
        """Saves the Fingerprint->ID association to history."""
        try:
            self.cur.execute(
                "INSERT OR IGNORE INTO known_fingerprints (fingerprint, acoustid_id) VALUES (?, ?)",
                (fingerprint, acoustid_id),
            )

            self.cur.execute(
                "SELECT 1 FROM known_blocks WHERE acoustid_id = ? LIMIT 1",
                (acoustid_id,),
            )
            if not self.cur.fetchone():
                blocks = [(b, acoustid_id) for b in self._get_blocks(fingerprint)]
                self.cur.executemany(
                    "INSERT INTO known_blocks (block, acoustid_id) VALUES (?, ?)",
                    blocks,
                )
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Failed to update fingerprint cache: {e}")

    def _update_index(self, path, fingerprint):
        """Updates the blocking index for a new file."""
        self.cur.execute("DELETE FROM fingerprint_index WHERE path = ?", (path,))
        blocks = [(b, path) for b in self._get_blocks(fingerprint)]
        self.cur.executemany(
            "INSERT INTO fingerprint_index (block, path) VALUES (?, ?)", blocks
        )

    def _display_local_matches(self, acoustid_id):
        """Displays existing albums in the library that contain this song."""
        try:
            query = """
                SELECT DISTINCT a.album_title, a.album_artist
                FROM files f
                JOIN albums a ON f.album_id = a.release_id
                WHERE f.acoustid_id = ? AND f.processed = 1
            """
            self.cur.execute(query, (acoustid_id,))
            rows = self.cur.fetchall()
            if rows:
                print(f"\n[INFO] You already have this song in your library:")
                for title, artist in rows:
                    print(f"   * {title} ({artist})")
                print("-" * 80)
        except sqlite3.Error as e:
            logging.error(f"Failed to fetch local matches: {e}")

    def _get_owned_release_ids(self, acoustid_id):
        """Returns a set of release IDs for this AcoustID that are already in the library."""
        try:
            query = "SELECT DISTINCT album_id FROM files WHERE acoustid_id = ? AND processed = 1"
            self.cur.execute(query, (acoustid_id,))
            return set(row[0] for row in self.cur.fetchall())
        except sqlite3.Error as e:
            logging.error(f"Failed to fetch local matches: {e}")
            return set()

    def _find_local_fuzzy_match(self, fingerprint):
        blocks = self._get_blocks(fingerprint)
        if not blocks:
            return None, 0.0, None

        placeholders = ",".join(["?"] * len(blocks))
        query = f"SELECT DISTINCT path FROM fingerprint_index WHERE block IN ({placeholders})"
        self.cur.execute(query, blocks)
        candidates = [row[0] for row in self.cur.fetchall()]

        best_path = None
        best_score = 0.0
        best_record = None

        for cand_path in candidates:
            self.cur.execute(
                "SELECT fingerprint, quality_score, format, file_size FROM files WHERE path = ?",
                (cand_path,),
            )
            res = self.cur.fetchone()
            if not res:
                continue

            cand_fp, cand_q, cand_fmt, cand_size = res
            if cand_q is None:
                cand_q = 0.0

            ratio = difflib.SequenceMatcher(None, fingerprint, cand_fp).ratio()

            if ratio > best_score:
                best_score = ratio
                best_path = cand_path
                best_record = {"score": cand_q, "format": cand_fmt, "size": cand_size}

        return best_path, best_score, best_record

    def _identify_locally(self, fingerprint):
        blocks = self._get_blocks(fingerprint)
        if not blocks:
            return None, 0.0

        placeholders = ",".join(["?"] * len(blocks))
        query = f"SELECT DISTINCT acoustid_id FROM known_blocks WHERE block IN ({placeholders})"
        self.cur.execute(query, blocks)
        candidate_ids = [row[0] for row in self.cur.fetchall()]

        if not candidate_ids:
            return None, 0.0

        best_id = None
        best_score = 0.0

        for cid in candidate_ids:
            self.cur.execute(
                "SELECT fingerprint FROM known_fingerprints WHERE acoustid_id = ?",
                (cid,),
            )
            history_fps = self.cur.fetchall()

            for (hist_fp,) in history_fps:
                ratio = difflib.SequenceMatcher(None, fingerprint, hist_fp).ratio()
                if ratio > best_score:
                    best_score = ratio
                    best_id = cid

        return best_id, best_score

    def _calculate_quality(self, file_path):
        """Generates a quality score based on format, bit depth, size."""
        try:
            time.sleep(self.API_SLEEP)
            audio = mutagen.File(file_path)
            if not audio:
                return None

            info = audio.info
            ext = os.path.splitext(file_path)[1].lower()
            file_size = os.path.getsize(file_path)

            is_lossless = ext in [".flac", ".wav", ".m4a"] and hasattr(
                info, "bits_per_sample"
            )
            fmt_score = 2 * 10**15 if is_lossless else 1 * 10**15
            bits = getattr(info, "bits_per_sample", 16)
            bit_score = bits * 10**12
            size_score = file_size
            sample_rate = getattr(info, "sample_rate", 44100)
            bitrate = getattr(info, "bitrate", 0)
            extras = (sample_rate / 10**6) + (bitrate / 10**9)

            final_score = fmt_score + bit_score + size_score + extras

            return {
                "score": final_score,
                "format": ext,
                "size": file_size,
                "bitrate": bitrate,
                "sample_rate": sample_rate,
                "bits": bits,
            }
        except Exception as e:
            logging.error(f"Quality check failed for {file_path}: {e}")
            return None

    def _get_candidates(self, results):
        """Parses API results and returns a list of all potential album matches."""
        candidates = []
        seen_releases = set()

        for result in results:
            match_score = result.get("score", 0) or 0
            recordings = result.get("recordings") or []

            for recording in recordings:
                rec_title = recording.get("title", "Unknown")
                releases = recording.get("releases") or []

                for release in releases:
                    rel_id = release.get("id")
                    if rel_id in seen_releases:
                        continue
                    seen_releases.add(rel_id)

                    candidates.append(
                        {
                            "similarity": match_score,
                            "recording_title": rec_title,
                            "album_title": release.get("title", "Unknown Album"),
                            "artist": release.get("artists", [{}])[0].get(
                                "name", "Unknown Artist"
                            ),
                            "date": str(release.get("date", {}).get("year", "Unknown")),
                            "country": release.get("country", "XX"),
                            "release": release,
                            "recording": recording,
                        }
                    )

        candidates.sort(
            key=lambda x: (x["similarity"], x["country"] == "US", x["date"]),
            reverse=True,
        )
        return candidates

    def _play_audio(self, file_path):
        """Attempts to play audio using available system CLI players."""
        self._stop_audio()
        commands = []
        if sys.platform == "darwin":
            commands.append(["afplay", file_path])
        commands.extend(
            [
                [
                    "ffplay",
                    "-nodisp",
                    "-autoexit",
                    "-hide_banner",
                    "-loglevel",
                    "quiet",
                    file_path,
                ],
                ["mpv", "--no-video", "--quiet", file_path],
                ["cvlc", "--play-and-exit", "--quiet", file_path],
            ]
        )

        for cmd_args in commands:
            if shutil.which(cmd_args[0]):
                try:
                    self.player_process = subprocess.Popen(
                        cmd_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                    )
                    return
                except Exception as e:
                    logging.warning(f"Failed to start player {cmd_args[0]}: {e}")
                    continue
        print("(!) No supported audio player found.")

    def _stop_audio(self):
        if self.player_process:
            try:
                self.player_process.terminate()
                try:
                    self.player_process.wait(timeout=0.5)
                except subprocess.TimeoutExpired:
                    self.player_process.kill()
            except Exception:
                pass
            self.player_process = None

    def close(self):
        self._stop_audio()
        if hasattr(self, "conn") and self.conn:
            try:
                self.conn.close()
                self.conn = None
                print("Database connection closed.")
            except sqlite3.Error as e:
                logging.error(f"Error closing database: {e}")

    def _prompt_dedup_resolution(
        self, new_path, new_qual, old_path, old_qual, similarity
    ):
        print(f"\n[?] Uncertain Local Match ({similarity:.1%}) detected!")
        print(
            f"    NEW: {os.path.basename(new_path)} ({new_qual['format']}, {new_qual['size']//1024}KB)"
        )
        print(
            f"    OLD: {os.path.basename(old_path)} ({old_qual['format']}, {old_qual['size']//1024}KB)"
        )
        print("    (Playing NEW file...)")
        self._play_audio(new_path)

        while True:
            choice = input("    Keep (N)ew, (O)ld, (S)kip, or (Q)uit? ").lower()
            if choice == "n":
                self._stop_audio()
                return "new"
            elif choice == "o":
                self._stop_audio()
                return "old"
            elif choice == "s":
                self._stop_audio()
                return "skip"
            elif choice == "q":
                self._stop_audio()
                print("Exiting and rolling back current operation...")
                return "quit"

    def _prompt_user_selection(self, file_path, candidates):
        filename = os.path.basename(file_path)
        page_size = 10
        current_page = 0
        total_pages = (len(candidates) + page_size - 1) // page_size

        self._play_audio(file_path)

        try:
            while True:
                start_idx = current_page * page_size
                end_idx = start_idx + page_size
                current_batch = candidates[start_idx:end_idx]

                print(f"\n[!] Ambiguous API Match for file: {file_path}")
                print(f"    Page {current_page + 1}/{total_pages}")
                print(
                    f"{'#':<3} {'Own':<3} {'Sim':<5} {'Ctry':<4} {'Date':<6} {'Artist':<25} {'Album'}"
                )
                print("-" * 80)

                for i, c in enumerate(current_batch):
                    global_idx = start_idx + i + 1
                    sim_pct = f"{int(c['similarity'] * 100)}%"
                    own_mark = "*" if c.get("is_owned") else ""

                    print(
                        f"{global_idx:<3} {own_mark:<3} {sim_pct:<5} {c['country']:<4} {c['date']:<6} {c['artist'][:25]:<25} {c['album_title']}"
                    )

                print("-" * 80)

                prompt_options = []
                if current_page < total_pages - 1:
                    prompt_options.append("(N)ext")
                if current_page > 0:
                    prompt_options.append("(P)rev")
                prompt_options.append("(0) Skip")
                prompt_options.append("(Q)uit")

                prompt_str = (
                    f"Select Album # (1-{len(candidates)}, comma-separated for multiple), "
                    + ", ".join(prompt_options)
                )
                choice = input(f"{prompt_str}: ").lower()

                if choice == "0":
                    return []
                elif choice == "q":
                    return "quit"
                elif choice == "n" and current_page < total_pages - 1:
                    current_page += 1
                elif choice == "p" and current_page > 0:
                    current_page -= 1
                else:
                    try:
                        selections = []
                        parts = choice.split(",")
                        valid = True
                        for part in parts:
                            part = part.strip()
                            if not part:
                                continue
                            idx = int(part)
                            if 1 <= idx <= len(candidates):
                                selections.append(candidates[idx - 1])
                            else:
                                valid = False
                                break

                        if valid and selections:
                            return selections
                        print("Invalid selection.")
                    except ValueError:
                        print("Invalid selection.")
        finally:
            self._stop_audio()

    def _safe_move(self, src_path, target_dir, target_filename=None, operation="move"):
        if not os.path.exists(src_path):
            return None
        if not target_filename:
            target_filename = os.path.basename(src_path)
        clean_filename = self._sanitize_name(target_filename)

        dir_created = False
        if not self.dry_run and not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir)
                dir_created = True
            except OSError as e:
                logging.error(f"Failed to create directory {target_dir}: {e}")
                return None

        target_path = os.path.join(target_dir, clean_filename)
        if os.path.abspath(src_path) == os.path.abspath(target_path):
            return target_path

        base, ext = os.path.splitext(clean_filename)
        counter = 1
        while os.path.exists(target_path):
            if os.path.abspath(src_path) == os.path.abspath(target_path):
                return target_path
            target_path = os.path.join(target_dir, f"{base} ({counter}){ext}")
            counter += 1

        if self.dry_run:
            logging.info(f"[DRY RUN] {operation}: {src_path} -> {target_path}")
            return target_path

        try:
            if operation == "move":
                shutil.move(src_path, target_path)
                logging.info(f"Moved: {src_path} -> {target_path}")
            else:
                shutil.copy2(src_path, target_path)
                logging.info(f"Copied: {src_path} -> {target_path}")
            return target_path
        except Exception as e:
            logging.error(f"Failed to {operation} {src_path} -> {target_path}: {e}")
            if (
                dir_created
                and os.path.exists(target_dir)
                and not os.listdir(target_dir)
            ):
                try:
                    os.removedirs(target_dir)
                except OSError:
                    pass
            return None

    def cleanup_empty_folders(self):
        """Recursively removes empty folders in the music dir."""
        if not os.path.exists(self.music_folder):
            return
        print("Cleaning up empty source folders...")
        for root, dirs, _ in os.walk(self.music_folder, topdown=False):
            for name in dirs:
                try:
                    p = os.path.join(root, name)
                    os.rmdir(p)
                except OSError:
                    pass

    def _handle_local_deduplication(self, path, fingerprint, quality):
        match_path, match_score, match_record = self._find_local_fuzzy_match(
            fingerprint
        )
        if not match_path or match_score < self.SIMILARITY_ASK:
            return True
        return True

    def _handle_album_deduplication(
        self, path, acoustid_id, release_id, quality, dispose_source=False
    ):
        self.cur.execute(
            "SELECT path, quality_score FROM files WHERE acoustid_id = ? AND album_id = ? AND processed = 1",
            (acoustid_id, release_id),
        )
        existing = self.cur.fetchone()

        if not existing:
            return True

        existing_path, existing_score = existing
        if existing_score is None:
            existing_score = 0.0

        if quality["score"] > existing_score:
            logging.info(f"Album-Upgrade: {existing_path} -> {path}")
            print(
                f" -> Upgrading existing file in album (Quality: {existing_score} -> {quality['score']})"
            )

            if not self.dry_run:
                self._safe_move(existing_path, self.dup_folder, operation="move")
                self.cur.execute("DELETE FROM files WHERE path = ?", (existing_path,))
                self.conn.commit()
            return True
        else:
            logging.info(f"Album-Duplicate (Worse): {path} < {existing_path}")
            print(f" -> Duplicate found in album (lower/equal quality).")

            if dispose_source and not self.dry_run:
                self._safe_move(path, self.dup_folder, operation="move")
                self.cur.execute(
                    """INSERT OR REPLACE INTO files 
                       (path, processed, acoustid_id, quality_score, format, file_size, date_modified) 
                       VALUES (?, 1, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                    (
                        path,
                        acoustid_id,
                        quality["score"],
                        quality["format"],
                        quality["size"],
                    ),
                )
                self.conn.commit()

            return False

    def _apply_tags(self, file_path, meta):
        if self.dry_run:
            return
        try:
            time.sleep(self.API_SLEEP)
            audio = mutagen.File(file_path)
            if not audio:
                return
            ext = file_path.lower()

            if ext.endswith(".mp3"):
                tags = ID3(file_path)
                tags.add(TIT2(encoding=3, text=meta["title"]))
                tags.add(TALB(encoding=3, text=meta["album"]))
                tags.add(TPE1(encoding=3, text=meta["artist"]))
                tags.add(TPE2(encoding=3, text=meta["album_artist"]))
                tags.add(TRCK(encoding=3, text=str(meta["track_no"])))
                tags.add(TPOS(encoding=3, text=str(meta["disc_no"])))
                tags.save()
            elif ext.endswith((".flac", ".wav")):
                audio["title"], audio["album"] = meta["title"], meta["album"]
                audio["artist"], audio["albumartist"] = (
                    meta["artist"],
                    meta["album_artist"],
                )
                audio["tracknumber"], audio["discnumber"] = str(meta["track_no"]), str(
                    meta["disc_no"]
                )
                audio.save()
            elif ext.endswith((".m4a", ".mp4")):
                audio["\xa9nam"], audio["\xa9alb"] = meta["title"], meta["album"]
                audio["\xa9ART"], audio["aART"] = meta["artist"], meta["album_artist"]
                audio["trkn"], audio["disk"] = [(int(meta["track_no"]), 0)], [
                    (int(meta["disc_no"]), 0)
                ]
                audio.save()
            elif ext.endswith(".wma"):
                audio["Title"], audio["WM/AlbumTitle"] = meta["title"], meta["album"]
                audio["Author"], audio["WM/AlbumArtist"] = (
                    meta["artist"],
                    meta["album_artist"],
                )
                audio["WM/TrackNumber"], audio["WM/PartOfSet"] = str(
                    meta["track_no"]
                ), str(meta["disc_no"])
                audio.save()
        except Exception as e:
            logging.error(f"Tagging Error {file_path}: {e}")

    def _sanitize_name(self, name):
        if not name:
            return "Unknown"
        cleaned = name.replace("/", "-").replace("\\", "-")
        cleaned = "".join(c for c in cleaned if c.isalnum() or c in " -_.")
        return cleaned.strip()

    def _organize_file(
        self, current_path, artist_dir, album_dir, filename, operation="move"
    ):
        target_dir = os.path.join(self.destination_folder, artist_dir, album_dir)
        return self._safe_move(current_path, target_dir, filename, operation=operation)

    def process_library(self):
        files = [
            os.path.join(r, f)
            for r, _, fs in os.walk(self.music_folder)
            for f in fs
            if f.lower().endswith((".mp3", ".flac", ".m4a", ".mp4", ".wma", ".wav"))
        ]

        print(f"Found {len(files)} supported files. Starting process...")

        for i, path in enumerate(files):
            print(f"Processing [{i+1}/{len(files)}]: {os.path.basename(path)}")

            self.cur.execute("SELECT processed FROM files WHERE path = ?", (path,))
            if (row := self.cur.fetchone()) and row[0] == 1:
                print(" -> Already processed. Skipping.")
                continue

            # ==========================================
            # FAST EXACT DUPLICATE CHECK
            # ==========================================
            file_hash = self._get_file_hash(path)
            if file_hash:
                self.cur.execute(
                    "SELECT path FROM file_hashes WHERE file_hash = ?", (file_hash,)
                )
                if dup_row := self.cur.fetchone():
                    existing_path = dup_row[0]
                    print(f" -> Exact binary duplicate of a known file. Skipping API.")
                    print(f"    (Matches: {os.path.basename(existing_path)})")

                    self._safe_move(path, self.dup_folder, operation="move")

                    if not self.dry_run:
                        self.cur.execute(
                            "INSERT OR REPLACE INTO files (path, processed, date_modified) VALUES (?, 1, CURRENT_TIMESTAMP)",
                            (path,),
                        )
                        self.conn.commit()
                    else:
                        print(
                            f" -> [DRY RUN] Would update database to mark as processed duplicate."
                        )

                    continue
            # ==========================================

            try:
                if os.path.getsize(path) == 0:
                    logging.warning(f"Skipping empty file: {path}")
                    continue
            except OSError:
                logging.warning(f"Skipping inaccessible file: {path}")
                continue

            try:
                try:
                    duration, fingerprint = acoustid.fingerprint_file(path)
                except (EOFError, OSError, acoustid.FingerprintGenerationError) as e:
                    logging.warning(
                        f"Skipping corrupt or unreadable audio file {path}: {e}"
                    )
                    continue

                quality = self._calculate_quality(path)
                if not quality:
                    continue

                resp = acoustid.lookup(
                    self.api_key,
                    fingerprint,
                    duration,
                    meta="recordings releases tracks",
                )
                if resp.get("status") != "ok" or not resp.get("results"):
                    logging.warning(f"No match for {path}")
                    print(f" -> No match found. Moving to unresolved.")
                    self._safe_move(path, self.unresolved_folder, operation="move")
                    continue

                candidates = self._get_candidates(resp["results"])

                if not candidates:
                    logging.warning(f"No valid candidates parsed for {path}")
                    print(f" -> No candidates. Moving to unresolved.")
                    self._safe_move(path, self.unresolved_folder, operation="move")
                    continue

                top_match = candidates[0]
                current_acoustid_id = resp["results"][0]["id"]

                self._update_fingerprint_cache(current_acoustid_id, fingerprint)

                owned_ids = self._get_owned_release_ids(current_acoustid_id)
                for c in candidates:
                    c["is_owned"] = c["release"]["id"] in owned_ids

                candidates.sort(
                    key=lambda x: (
                        x["is_owned"],
                        x["similarity"],
                        x["country"] == "US",
                        x["date"],
                    ),
                    reverse=True,
                )

                top_match = candidates[0]
                selected_matches = []
                sticky_match = None

                if self.last_selected_album_id:
                    for c in candidates:
                        if (
                            c["release"]["id"] == self.last_selected_album_id
                            and c["similarity"] >= self.SIMILARITY_STICKY
                        ):
                            sticky_match = c
                            break

                if sticky_match:
                    logging.info(
                        f"Auto-selected sticky album: {sticky_match['album_title']}"
                    )
                    selected_matches = [sticky_match]
                elif len(candidates) == 1 and top_match["similarity"] >= 0.98:
                    selected_matches = [top_match]
                    self.last_selected_album_id = top_match["release"]["id"]
                else:
                    result = self._prompt_user_selection(path, candidates)
                    if result == "quit":
                        logging.info("User initiated quit.")
                        self.close()
                        sys.exit(0)
                    selected_matches = result or []

                    if len(selected_matches) == 1:
                        self.last_selected_album_id = selected_matches[0]["release"][
                            "id"
                        ]
                    else:
                        self.last_selected_album_id = None

                if not selected_matches:
                    logging.info(f"Skipped by user: {path}")
                    continue

                for idx, selected_match in enumerate(selected_matches):
                    is_last_item = idx == len(selected_matches) - 1
                    rel = selected_match["release"]
                    rec = selected_match["recording"]
                    target_recording_id = rec.get("id")
                    target_release_id = rel.get("id")

                    if not self._handle_album_deduplication(
                        path,
                        current_acoustid_id,
                        target_release_id,
                        quality,
                        dispose_source=is_last_item,
                    ):
                        continue

                    artist = (
                        rel.get("artists", [{}])[0].get("name")
                        or rec.get("artists", [{}])[0].get("name")
                        or "Unknown"
                    )

                    track_num = 1
                    disc_num = 1
                    found_track = False

                    def norm(s):
                        return str(s).lower().strip()

                    target_title = norm(rec.get("title", ""))

                    for medium in rel.get("mediums", []):
                        current_disc = medium.get("position", 1)
                        for track in medium.get("tracks", []):
                            track_rec_id = str(track.get("recording", {}).get("id"))
                            if track_rec_id == str(target_recording_id):
                                track_num = track.get("position", 1)
                                disc_num = current_disc
                                found_track = True
                                break
                        if found_track:
                            break

                    if not found_track:
                        logging.warning(
                            f"Exact ID match failed for {path}. Attempting Title match fallback."
                        )
                        for medium in rel.get("mediums", []):
                            current_disc = medium.get("position", 1)
                            for track in medium.get("tracks", []):
                                if norm(track.get("title", "")) == target_title:
                                    track_num = track.get("position", 1)
                                    disc_num = current_disc
                                    found_track = True
                                    break
                            if found_track:
                                break

                    if not found_track:
                        logging.warning(
                            f"Could not find track number for {path}. Defaulting to 1."
                        )

                    meta = {
                        "title": rec.get("title", "Unknown"),
                        "album": rel.get("title", "Unknown Album"),
                        "artist": artist,
                        "album_artist": rel.get("artists", [{}])[0].get("name")
                        or artist,
                        "track_no": track_num,
                        "disc_no": disc_num,
                        "release_date": str(rel.get("date", {}).get("year", "0000")),
                        "release_id": target_release_id,
                    }

                    safe_artist = self._sanitize_name(meta["album_artist"])
                    safe_album = self._sanitize_name(meta["album"])
                    raw_filename = f"{str(meta['track_no']).zfill(2)} - {meta['title']}{quality['format']}"
                    safe_filename = self._sanitize_name(raw_filename)

                    final_path = None
                    if is_last_item:
                        final_path = self._organize_file(
                            path,
                            safe_artist,
                            safe_album,
                            safe_filename,
                            operation="move",
                        )
                    else:
                        final_path = self._organize_file(
                            path,
                            safe_artist,
                            safe_album,
                            safe_filename,
                            operation="copy",
                        )

                    if not final_path:
                        continue

                    self._apply_tags(final_path, meta)

                    self.cur.execute(
                        "INSERT OR IGNORE INTO albums VALUES (?,?,?,?,?)",
                        (
                            meta["release_id"],
                            meta["album"],
                            meta["album_artist"],
                            meta["release_date"],
                            rel.get("country", "XX"),
                        ),
                    )

                    self.cur.execute(
                        """INSERT OR REPLACE INTO files 
                           (path, fingerprint, acoustid_id, title, track_no, disc_no, format, file_size, quality_score, album_id, processed, date_modified) 
                           VALUES (?,?,?,?,?,?,?,?,?,?,?, CURRENT_TIMESTAMP)""",
                        (
                            final_path,
                            fingerprint,
                            current_acoustid_id,
                            meta["title"],
                            meta["track_no"],
                            meta["disc_no"],
                            quality["format"],
                            quality["size"],
                            quality["score"],
                            meta["release_id"],
                            1,
                        ),
                    )

                    self._update_index(final_path, fingerprint)

                    if file_hash:
                        self.cur.execute(
                            "INSERT OR REPLACE INTO file_hashes (file_hash, path) VALUES (?, ?)",
                            (file_hash, final_path),
                        )

                    self.conn.commit()
                    print(
                        f" -> Success: {os.path.join(safe_artist, safe_album, safe_filename)}"
                    )

            except Exception as e:
                logging.error(f"Critical Failure on {path}: {e}")
                logging.error(traceback.format_exc())
                print(f" -> Error: {e}")

        self.cleanup_empty_folders()

    def __del__(self):
        self.close()


if __name__ == "__main__":
    config_filename = "library_management_config.json"

    if not os.path.exists(config_filename):
        print(
            f"[{config_filename}] not found. Generating a default configuration file..."
        )
        default_config = {
            "api_key": "7dlZplmc3N",
            "music_folder": "/mnt/NAS/cleanmusic/music/",
            "destination_folder": "/mnt/NAS/cleanmusic/NewMaster/",
            "dup_folder": "/mnt/NAS/cleanmusic/duplicates/",
            "unresolved_folder": "/mnt/NAS/cleanmusic/unresolved/",
            "db_path": "library_manager.db",
            "dry_run": False,
            "prune": False,
            "prepopulate": False,
            "process": True
        }
        with open(config_filename, "w") as f:
            json.dump(default_config, f, indent=4)
        print(
            f"Default configuration created! Please review '{config_filename}' and run the script again."
        )
        sys.exit(0)

    manager = MusicLibraryManager(config_file=config_filename)

    if manager.dry_run:
        print("\n[!] DRY RUN MODE ACTIVATED [!]\n")

    try:
        if manager.prune:
            manager.prune_database()
            print("-" * 40)

        if manager.prepopulate:
            manager.prepopulate_hashes()
            print("-" * 40)

        if manager.run_process:
            manager.process_library()

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Shutting down gracefully...")
    finally:
        manager.close()