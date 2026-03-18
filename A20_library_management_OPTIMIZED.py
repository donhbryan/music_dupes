import os
import sys
import time
import signal
import subprocess
import sqlite3
import logging
import shutil
#import difflib
import traceback
import json
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore
from queue import Queue, PriorityQueue
import multiprocessing

import acoustid
import mutagen
import musicbrainzngs
from mutagen.id3 import ID3, TPE1, TPE2, TRCK, TPOS, TIT2, TALB

# Global shutdown event used to gracefully stop long-running operations on Ctrl+C
shutdown_event = threading.Event()

# Initialize MusicBrainz API wrapper
musicbrainzngs.set_useragent(
    "MusicLibraryManager", "1.0", "https://github.com/MusicLibraryManager"
)


# --- ISOLATED CPU WORKER (NO MULTIPROCESSING) ---
def _cpu_bound_worker(path):
    """
    Handles heavy lifting: ffmpeg hashing and acoustid fingerprinting.
    NOW RUNS IN THREADS (not processes) to avoid database segfaults.

    The global shutdown_event is consulted so the function can exit quickly if
    the user presses Ctrl+C.
    """
    result = {
        "path": path,
        "hash": None,
        "duration": None,
        "fingerprint": None,
        "error": None,
    }
    if shutdown_event.is_set():
        return result

    try:
        # 1. Hashing with ffmpeg
        hasher = hashlib.md5()
        cmd = [
            "ffmpeg",
            "-threads",
            "1",
            "-v",
            "quiet",
            "-ss",
            "00:00:15",
            "-t",
            "30",
            "-i",
            path,
            "-f",
            "s16le",
            "-acodec",
            "pcm_s16le",
            "-",
        ]
        try:
            with subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
            ) as process:
                for chunk in iter(lambda: process.stdout.read(8192 * 1024), b""):
                    if shutdown_event.is_set():
                        return result
                    if chunk:
                        hasher.update(chunk)
            result["hash"] = hasher.hexdigest()
        except Exception as e:
            logging.warning(f"Hashing failed for {path}: {e}")
            result["hash"] = None

        # 2. Fingerprinting with acoustid (fresh import in thread)
        try:
            import acoustid as acoustid_module
            duration, fingerprint = acoustid_module.fingerprint_file(path)
            result["duration"] = duration
            result["fingerprint"] = fingerprint
        except Exception as e:
            logging.warning(f"Fingerprinting failed for {path}: {e}")
            result["fingerprint"] = None
            result["duration"] = None

    except Exception as e:
        result["error"] = str(e)
        logging.error(f"Worker error on {path}: {e}")
        traceback.print_exc()

    return result


# -----------------------------------------------


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
        self.destination_folder = os.path.abspath(
            config.get("destination_folder", "")
        )
        self.dup_folder = os.path.abspath(config.get("dup_folder", ""))
        self.unresolved_folder = os.path.abspath(config.get("unresolved_folder", ""))
        self.db_path = config.get("db_path", "library_manager.db")

        # Safely parse booleans
        def parse_bool(val, default=False):
            if isinstance(val, str):
                return val.strip().lower() in ("true", "1", "yes", "t")
            if val is None:
                return default
            return bool(val)

        self.dry_run = parse_bool(config.get("dry_run", False))
        self.prune = parse_bool(config.get("prune", False))
        self.hash_audio = parse_bool(config.get("hashAudio", False))
        self.run_process = parse_bool(config.get("process", True))
        self.global_dedup = parse_bool(config.get("global_dedup", False))

        self.player_process = None

        # Tuning for fuzzy matching
        self.BLOCK_SIZE = 16
        self.SIMILARITY_AUTO = 0.98
        self.SIMILARITY_STICKY = 0.95
        self.API_SLEEP = 0.4

        # OPTIMIZATION: Memory-based caching
        self.owned_ids_cache = {}  # acoustid_id -> set of release_ids
        self.quality_cache = {}  # path -> quality dict
        self.fingerprint_cache = {}  # path -> fingerprint
        self.audio_hash_cache = {}  # audio_hash -> path
        self.cache_lock = threading.Lock()

        # Threading/Concurrency Controls
        self.api_lock = threading.Lock()
        self.api_semaphore = Semaphore(1)  # MusicBrainz rate limiting
        self.last_mb_call = 0.0
        self.db_queue = None
        self.last_selected_album_id = None

        # OPTIMIZATION: Increase worker threads for I/O-bound operations
        cpu_count = multiprocessing.cpu_count()
        self.cpu_workers = max(4, cpu_count // 2)  # 4-8 workers for audio processing
        self.api_workers = min(8, cpu_count)  # 8-16 workers for API calls (mostly waiting)
        self.db_batch_size = 200  # Batch more DB operations before committing

        logging.basicConfig(
            filename="library_manager.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        # Main thread connection (only for reads before processing starts)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cur = self.conn.cursor()
        self._setup_database()

        for folder in [
            self.dup_folder,
            self.destination_folder,
            self.unresolved_folder,
        ]:
            if not os.path.exists(folder):
                os.makedirs(folder)

    def _setup_database(self):
        """Creates the normalized database schema with optimizations."""
        self.cur.execute("PRAGMA foreign_keys = ON")

        # SQLite Performance Tuning (WAL Mode + Cache)
        self.cur.execute("PRAGMA journal_mode = WAL")
        self.cur.execute("PRAGMA synchronous = NORMAL")
        # OPTIMIZATION: Increase cache size (negative = KB)
        self.cur.execute("PRAGMA cache_size = -512000")  # 512MB cache
        self.cur.execute("PRAGMA temp_store = MEMORY")  # Use RAM for temp tables

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS albums (
            release_id TEXT PRIMARY KEY, album_title TEXT, album_artist TEXT, release_date TEXT, country TEXT
        )"""
        )

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY, fingerprint TEXT, acoustid_id TEXT, title TEXT, track_no INTEGER, 
            disc_no INTEGER, format TEXT, file_size INTEGER, quality_score REAL, album_id TEXT, 
            processed INTEGER DEFAULT 0, date_modified DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (album_id) REFERENCES albums (release_id)
        )"""
        )

        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_acoustid ON files(acoustid_id)"
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_processed ON files(processed)"
        )

        try:
            self.cur.execute(
                "ALTER TABLE files ADD COLUMN date_modified DATETIME DEFAULT CURRENT_TIMESTAMP"
            )
        except sqlite3.OperationalError:
            pass

        self.cur.execute(
            """CREATE TRIGGER IF NOT EXISTS update_files_modtime
            AFTER UPDATE ON files FOR EACH ROW BEGIN
                UPDATE files SET date_modified = CURRENT_TIMESTAMP WHERE path = old.path;
            END;"""
        )

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS known_fingerprints (
            fingerprint TEXT, acoustid_id TEXT, PRIMARY KEY (fingerprint, acoustid_id)
        )"""
        )

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS known_blocks (
            block TEXT, acoustid_id TEXT
        )"""
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_known_blocks ON known_blocks(block)"
        )

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS fingerprint_index (
            block TEXT, path TEXT, FOREIGN KEY(path) REFERENCES files(path) ON DELETE CASCADE
        )"""
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_blocks ON fingerprint_index(block)"
        )

        self.cur.execute("DROP TABLE IF EXISTS file_hashes")
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS audio_hashes (
            audio_hash TEXT PRIMARY KEY, path TEXT
        )"""
        )

        self.conn.commit()

    def _preload_cache(self):
        """OPTIMIZATION: Preload database into memory at startup."""
        print("Preloading database cache into memory...")
        try:
            # Use a dedicated read-only connection to avoid interfering with the background writer thread.
            with sqlite3.connect(self.db_path) as read_conn:
                read_cur = read_conn.cursor()

                # Load audio hashes
                read_cur.execute("SELECT audio_hash, path FROM audio_hashes")
                for audio_hash, path in read_cur.fetchall():
                    self.audio_hash_cache[audio_hash] = path

                # Load owned release IDs by acoustid
                read_cur.execute(
                    "SELECT DISTINCT acoustid_id, album_id FROM files WHERE processed = 1 AND acoustid_id IS NOT NULL"
                )
                for acoustid_id, album_id in read_cur.fetchall():
                    if acoustid_id not in self.owned_ids_cache:
                        self.owned_ids_cache[acoustid_id] = set()
                    self.owned_ids_cache[acoustid_id].add(album_id)

            print(
                f"Loaded {len(self.audio_hash_cache)} audio hashes and {len(self.owned_ids_cache)} acoustid entries."
            )
        except Exception as e:
            logging.error(f"Error preloading cache: {e}")

    def _db_writer_thread(self):
        """
        Runs in the background, executing queued DB operations sequentially.
        CRITICAL: This thread owns the cursor and connection - no other thread touches the DB directly.
        OPTIMIZATION: Batch operations before committing
        """
        operations_count = 0
        while True:
            task = self.db_queue.get()
            if task is None:  # Poison pill
                self.conn.commit()
                break

            op_type, query, params = task
            attempt = 0
            while True:
                try:
                    if op_type == "execute":
                        self.cur.execute(query, params)
                    elif op_type == "executemany":
                        self.cur.executemany(query, params)

                    operations_count += 1
                    # OPTIMIZATION: Increased batch size for fewer commits
                    if operations_count >= self.db_batch_size:
                        self.conn.commit()
                        operations_count = 0
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower() and attempt < 5:
                        attempt += 1
                        time.sleep(0.1)
                        continue
                    logging.error(f"Database write failed: {e} | Query: {query}")
                    traceback.print_exc()
                    break
                except sqlite3.Error as e:
                    logging.error(f"Database write failed: {e} | Query: {query}")
                    traceback.print_exc()
                    break
            self.db_queue.task_done()

    def prune_database(self):
        """Optimized pruning using set difference to eliminate disk I/O bottlenecks."""
        if not os.path.exists(self.music_folder):
            logging.error("Music folder not found. Skipping prune.")
            return

        print("Gathering database paths...")
        with self.conn:
            cursor = self.conn.execute("SELECT path FROM files")
            db_paths = set(row[0] for row in cursor.fetchall())

        print("Scanning filesystem...")
        disk_paths = set()
        for root, _, files in os.walk(self.music_folder):
            for f in files:
                disk_paths.add(os.path.join(root, f))

        missing_paths = db_paths - disk_paths

        if missing_paths:
            print(
                f"Cleaning up {len(missing_paths)} ghost entries from the database..."
            )
            with self.conn:
                self.cur.executemany(
                    "DELETE FROM files WHERE path = ?", [(p,) for p in missing_paths]
                )
                self.cur.executemany(
                    "DELETE FROM fingerprint_index WHERE path = ?",
                    [(p,) for p in missing_paths],
                )
                self.cur.executemany(
                    "DELETE FROM audio_hashes WHERE path = ?",
                    [(p,) for p in missing_paths],
                )
            logging.info("Pruned %d ghost entries from database.", len(missing_paths))
        else:
            print("Database is clean.")

    def _get_audio_hash(self, filepath):
        """Standalone hash generation for maintenance scripts."""
        hasher = hashlib.md5()
        try:
            cmd = [
                "ffmpeg",
                "-threads",
                "1",
                "-v",
                "quiet",
                "-ss",
                "00:00:15",
                "-t",
                "30",
                "-i",
                filepath,
                "-f",
                "s16le",
                "-acodec",
                "pcm_s16le",
                "-",
            ]
            with subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
            ) as process:
                for chunk in iter(lambda: process.stdout.read(8192 * 1024), b""):
                    if chunk:
                        hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            logging.error(f"Audio hashing failed for {filepath}: {e}")
            return None

    def hash_existing_audio(self):
        """Retroactively generates audio hashes for already-processed files."""
        print("Checking for existing files that need audio hashing...")
        # Use a dedicated connection to avoid clashing with the background writer thread.
        with sqlite3.connect(self.db_path) as read_conn:
            read_cur = read_conn.cursor()
            read_cur.execute("SELECT path FROM files")
            known_paths = [row[0] for row in read_cur.fetchall()]

        added_count = 0
        # Use a separate writer connection for updates, so we don't block the background writer.
        with sqlite3.connect(self.db_path) as write_conn:
            write_cur = write_conn.cursor()
            for path in known_paths:
                if shutdown_event.is_set():
                    break
                if not os.path.exists(path):
                    continue
                write_cur.execute("SELECT 1 FROM audio_hashes WHERE path = ?", (path,))
                if write_cur.fetchone():
                    continue

                audio_hash = self._get_audio_hash(path)
                if audio_hash:
                    write_cur.execute(
                        "INSERT OR REPLACE INTO audio_hashes (audio_hash, path) VALUES (?, ?)",
                        (audio_hash, path),
                    )
                    added_count += 1
            write_conn.commit()

        if added_count > 0:
            print(f"Successfully added {added_count} new audio hashes.")
        else:
            print("All known files already have audio hashes.")

    def _get_blocks(self, fingerprint):
        if not fingerprint:
            return []
        return [
            fingerprint[i : i + self.BLOCK_SIZE]
            for i in range(0, len(fingerprint), self.BLOCK_SIZE)
        ][:16]

    def _update_fingerprint_cache(self, acoustid_id, fingerprint):
        """Saves association via queue. NEVER call self.cur directly here!"""
        if not acoustid_id or not fingerprint:
            return

        # Queue the insert
        self.db_queue.put(
            (
                "execute",
                "INSERT OR IGNORE INTO known_fingerprints (fingerprint, acoustid_id) VALUES (?, ?)",
                (fingerprint, acoustid_id),
            )
        )

        # Check if blocks exist - use a separate read-only query
        # This is a race condition but acceptable for this use case
        try:
            # Use a separate connection for read-only queries
            check_conn = sqlite3.connect(self.db_path)
            check_cur = check_conn.cursor()
            check_cur.execute(
                "SELECT 1 FROM known_blocks WHERE acoustid_id = ? LIMIT 1",
                (acoustid_id,),
            )
            if not check_cur.fetchone():
                blocks = [(b, acoustid_id) for b in self._get_blocks(fingerprint)]
                if blocks:
                    self.db_queue.put(
                        (
                            "executemany",
                            "INSERT INTO known_blocks (block, acoustid_id) VALUES (?, ?)",
                            blocks,
                        )
                    )
            check_conn.close()
        except Exception as e:
            logging.warning(f"Could not check known_blocks: {e}")

    def _update_index(self, path, fingerprint):
        """Updates local index via queue. NEVER call self.cur directly here!"""
        if not fingerprint or not path:
            return

        self.db_queue.put(
            ("execute", "DELETE FROM fingerprint_index WHERE path = ?", (path,))
        )
        blocks = [(b, path) for b in self._get_blocks(fingerprint)]
        if blocks:
            self.db_queue.put(
                (
                    "executemany",
                    "INSERT INTO fingerprint_index (block, path) VALUES (?, ?)",
                    blocks,
                )
            )

    def _get_owned_release_ids(self, acoustid_id):
        """
        OPTIMIZATION: Use in-memory cache instead of database queries.
        """
        with self.cache_lock:
            if acoustid_id in self.owned_ids_cache:
                return self.owned_ids_cache[acoustid_id]
        return set()

    def _calculate_quality(self, file_path):
        # OPTIMIZATION: Cache quality calculations
        with self.cache_lock:
            if file_path in self.quality_cache:
                return self.quality_cache[file_path]

        try:
            audio = mutagen.File(file_path)
            if not audio:
                return None
            info = audio.info
            ext = os.path.splitext(file_path)[1].lower()
            file_size = os.path.getsize(file_path)

            format_hierarchy = {
                ".flac": 3 * 10**15,
                ".m4a": 2.5 * 10**15,
                ".wav": 2 * 10**15,
                ".mp3": 1 * 10**15,
                ".wma": 0.5 * 10**15,
            }
            fmt_score = format_hierarchy.get(ext, 0)
            bits = getattr(info, "bits_per_sample", 16)
            bit_score = bits * 10**12
            size_score = file_size / 1000
            sample_rate = getattr(info, "sample_rate", 44100)
            bitrate = getattr(info, "bitrate", 0)
            extras = (sample_rate / 10**6) + (bitrate / 10**9)

            quality = {
                "score": fmt_score + bit_score + size_score + extras,
                "format": ext,
                "size": file_size,
                "bitrate": bitrate,
                "sample_rate": sample_rate,
                "bits": bits,
            }

            # Cache it
            with self.cache_lock:
                self.quality_cache[file_path] = quality

            return quality
        except Exception as e:
            logging.error(f"Quality check failed for {file_path}: {e}")
            return None

    def _fallback_musicbrainz_search(self, file_path):
        try:
            audio = mutagen.File(file_path, easy=True)
            if not audio:
                return []

            title = audio.get("title", [""])[0]
            artist = audio.get("artist", [""])[0]
            album = audio.get("album", [""])[0]

            if not title or not artist:
                return []
            print(
                f" -> AcoustID failed. Falling back to MB metadata search: {artist} - {title}"
            )

            query = f'artist:"{artist}" AND recording:"{title}"'
            if album:
                query += f' AND release:"{album}"'

            # OPTIMIZATION: Use semaphore for rate limiting instead of sleep
            with self.api_semaphore:
                elapsed = time.time() - self.last_mb_call
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                try:
                    result = musicbrainzngs.search_recordings(query=query, limit=5)
                except Exception as e:
                    logging.error(f"MB API Error: {e}")
                    return []
                finally:
                    self.last_mb_call = time.time()

            candidates = []
            for rec in result.get("recording-list", []):
                rec_title, rec_id = rec.get("title", "Unknown"), rec.get("id")
                for rel in rec.get("release-list", []):
                    rel_id, track_pos, disc_pos = rel.get("id"), 1, 1
                    if "medium-list" in rel:
                        for med in rel["medium-list"]:
                            disc_pos = med.get("position", 1)
                            for trk in med.get("track-list", []):
                                if trk.get("recording", {}).get("id") == rec_id:
                                    track_pos = trk.get("number", 1)
                                    break

                    mock_release = {
                        "id": rel_id,
                        "title": rel.get("title", "Unknown Album"),
                        "artists": [{"name": artist}],
                        "date": {"year": rel.get("date", "0000")[:4]},
                        "country": rel.get("country", "XX"),
                        "mediums": [
                            {
                                "position": disc_pos,
                                "tracks": [
                                    {
                                        "position": track_pos,
                                        "title": rec_title,
                                        "recording": {"id": rec_id},
                                    }
                                ],
                            }
                        ],
                    }
                    candidates.append(
                        {
                            "similarity": float(rec.get("ext:score", 0)) / 100.0,
                            "recording_title": rec_title,
                            "album_title": mock_release["title"],
                            "artist": artist,
                            "date": str(mock_release["date"]["year"]),
                            "country": mock_release["country"],
                            "release": mock_release,
                            "recording": {
                                "id": rec_id,
                                "title": rec_title,
                                "artists": [{"name": artist}],
                            },
                            "is_owned": False,
                        }
                    )

            candidates.sort(key=lambda x: x["similarity"], reverse=True)
            return candidates
        except Exception as e:
            logging.error(f"Fallback search error: {e}")
            traceback.print_exc()
            return []

    def _get_candidates(self, results):
        candidates = []
        seen_releases = set()
        for result in results:
            match_score = result.get("score", 0) or 0
            for recording in result.get("recordings", []):
                rec_title = recording.get("title", "Unknown")
                for release in recording.get("releases", []):
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
                except Exception:
                    continue
        print("(!) No supported audio player found.")

    def _stop_audio(self):
        if self.player_process:
            try:
                self.player_process.terminate()
                self.player_process.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                self.player_process.kill()
            except Exception:
                pass
            self.player_process = None

    def close(self):
        self._stop_audio()

        # Ensure the background DB writer thread shuts down cleanly
        try:
            if hasattr(self, "db_queue") and self.db_queue:
                self.db_queue.put(None)
            if hasattr(self, "writer_thread") and self.writer_thread:
                self.writer_thread.join(timeout=2)
        except Exception:
            pass

        if hasattr(self, "conn") and self.conn:
            try:
                self.conn.close()
                self.conn = None
            except sqlite3.Error as e:
                logging.error(f"Error closing database: {e}")

    def _prompt_user_selection(self, file_path, candidates):
        filename = os.path.basename(file_path)
        page_size = 10
        current_page = 0
        total_pages = (len(candidates) + page_size - 1) // page_size
        self._play_audio(file_path)

        try:
            while True:
                start_idx = current_page * page_size
                current_batch = candidates[start_idx : start_idx + page_size]
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
                prompt_options.extend(["(0) Skip", "(Q)uit"])

                choice = input(
                    f"Select Album # (1-{len(candidates)}, comma-separated for multiple), "
                    + ", ".join(prompt_options)
                    + ": "
                ).lower()

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
                        selections, valid = [], True
                        for part in choice.split(","):
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
            except OSError:
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
            else:
                shutil.copy2(src_path, target_path)
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
        if not os.path.exists(self.music_folder):
            return
        print("Cleaning up empty source folders...")
        for root, dirs, _ in os.walk(self.music_folder, topdown=False):
            for name in dirs:
                try:
                    os.rmdir(os.path.join(root, name))
                except OSError:
                    pass

    def _handle_album_deduplication(
        self, path, acoustid_id, release_id, quality, dispose_source=False
    ):
        """
        Check for duplicates using a separate read-only connection.
        OPTIMIZATION: Cache frequently checked data
        """
        try:
            read_conn = sqlite3.connect(self.db_path)
            read_cur = read_conn.cursor()

            if self.global_dedup:
                read_cur.execute(
                    "SELECT path, quality_score FROM files WHERE acoustid_id = ? AND processed = 1",
                    (acoustid_id,),
                )
            else:
                read_cur.execute(
                    "SELECT path, quality_score FROM files WHERE acoustid_id = ? AND album_id = ? AND processed = 1",
                    (acoustid_id, release_id),
                )

            existing = read_cur.fetchone()
            read_conn.close()
        except sqlite3.Error as e:
            logging.error(f"Failed to check for duplicates: {e}")
            return True

        if not existing:
            return True

        existing_path, existing_score = existing
        if existing_score is None:
            existing_score = 0.0

        if quality["score"] > existing_score:
            print(
                f" -> Upgrading existing file (Quality: {existing_score} -> {quality['score']})"
            )
            if not self.dry_run:
                self._safe_move(existing_path, self.dup_folder, operation="move")
                self.db_queue.put(
                    ("execute", "DELETE FROM files WHERE path = ?", (existing_path,))
                )
            return True
        else:
            print(f" -> Duplicate found (lower/equal quality).")
            if dispose_source and not self.dry_run:
                self._safe_move(path, self.dup_folder, operation="move")
                self.db_queue.put(
                    (
                        "execute",
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
                )
            return False

    def _apply_tags(self, file_path, meta):
        if self.dry_run:
            return
        try:
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
            traceback.print_exc()

    def _sanitize_name(self, name):
        if not name:
            return "Unknown"
        cleaned = name.replace("/", "-").replace("\\", "-")
        return "".join(c for c in cleaned if c.isalnum() or c in " -_.").strip()

    def _organize_file(
        self, current_path, artist_dir, album_dir, filename, operation="move"
    ):
        target_dir = os.path.join(self.destination_folder, artist_dir, album_dir)
        return self._safe_move(current_path, target_dir, filename, operation=operation)

    def _query_audio_hash_safely(self, audio_hash):
        """
        OPTIMIZATION: Use in-memory cache first, fall back to DB
        """
        with self.cache_lock:
            if audio_hash in self.audio_hash_cache:
                return (self.audio_hash_cache[audio_hash],)

        try:
            read_conn = sqlite3.connect(self.db_path)
            read_cur = read_conn.cursor()
            read_cur.execute(
                "SELECT path FROM audio_hashes WHERE audio_hash = ?", (audio_hash,)
            )
            result = read_cur.fetchone()
            read_conn.close()

            # Cache the result
            if result:
                with self.cache_lock:
                    self.audio_hash_cache[audio_hash] = result[0]

            return result
        except sqlite3.Error as e:
            logging.error(f"Failed to query audio hash: {e}")
            return None

    def process_library(self):
        if shutdown_event.is_set():
            return

        # --- INITIALIZE BACKGROUND DB WRITER ---
        self.db_queue = Queue()
        self.writer_thread = threading.Thread(target=self._db_writer_thread, daemon=True)
        self.writer_thread.start()

        # --- PRELOAD CACHE ---
        self._preload_cache()

        # --- GATHER & FILTER ---
        print("Scanning directories...")
        all_files = [
            os.path.join(r, f)
            for r, _, fs in os.walk(self.music_folder)
            for f in fs
            if f.lower().endswith((".mp3", ".flac", ".m4a", ".mp4", ".wma", ".wav"))
        ]

        # Read from a separate connection so this select cannot hold a read-lock during writer activity.
        with sqlite3.connect(self.db_path) as read_conn:
            read_cur = read_conn.cursor()
            read_cur.execute("SELECT path FROM files WHERE processed = 1")
            processed_set = {row[0] for row in read_cur.fetchall()}

        pending_files = [f for f in all_files if f not in processed_set]
        if shutdown_event.is_set():
            self.db_queue.put(None)
            writer_thread.join()
            return
        print(f"Found {len(pending_files)} files needing processing.\n")

        if not pending_files:
            self.db_queue.put(None)
            writer_thread.join()
            return

        ambiguous_queue = []

        # --- PHASE 1: CPU-BOUND CRUNCHING (OPTIMIZED ThreadPool) ---
        print(
            f"Stage 1: Crunching audio data (Hashing & Fingerprinting) - {self.cpu_workers} workers..."
        )
        cpu_results = []
        # OPTIMIZATION: Increased worker count for I/O-bound audio operations
        with ThreadPoolExecutor(max_workers=self.cpu_workers) as executor:
            futures = [executor.submit(_cpu_bound_worker, path) for path in pending_files]
            for future in as_completed(futures):
                if shutdown_event.is_set():
                    break
                try:
                    result = future.result()
                    if result.get("error"):
                        logging.warning(
                            f"Worker error on {result['path']}: {result['error']}"
                        )
                        self._safe_move(
                            result["path"], self.unresolved_folder, operation="move"
                        )
                    else:
                        cpu_results.append(result)
                except Exception as e:
                    logging.error(f"Future error: {e}")
                    traceback.print_exc()

        if shutdown_event.is_set():
            self.db_queue.put(None)
            self.writer_thread.join()
            return

        # --- PHASE 2: NETWORK & API RESOLUTION (OPTIMIZED ThreadPool) ---
        print(
            f"\nStage 2: Fetching API Metadata & Organizing - {self.api_workers} workers..."
        )

        def _api_worker(file_data):
            path = file_data["path"]
            quality = self._calculate_quality(path)
            if not quality:
                return {"status": "skip"}

            audio_hash = file_data["hash"]
            if audio_hash:
                dup_row = self._query_audio_hash_safely(audio_hash)
                if dup_row:
                    existing_path = dup_row[0]
                    try:
                        read_conn = sqlite3.connect(self.db_path)
                        read_cur = read_conn.cursor()
                        read_cur.execute(
                            "SELECT quality_score FROM files WHERE path = ?",
                            (existing_path,),
                        )
                        existing_score_row = read_cur.fetchone()
                        read_conn.close()
                    except sqlite3.Error as e:
                        logging.error(f"Failed to query quality score: {e}")
                        existing_score_row = None

                    existing_score = (
                        existing_score_row[0]
                        if existing_score_row and existing_score_row[0] is not None
                        else 0.0
                    )

                    if quality["score"] > existing_score:
                        if not self.dry_run:
                            self._safe_move(
                                existing_path, self.dup_folder, operation="move"
                            )
                            self.db_queue.put(
                                (
                                    "execute",
                                    "DELETE FROM files WHERE path = ?",
                                    (existing_path,),
                                )
                            )
                            self.db_queue.put(
                                (
                                    "execute",
                                    "UPDATE audio_hashes SET path = ? WHERE audio_hash = ?",
                                    (path, audio_hash),
                                )
                            )
                    else:
                        self._safe_move(path, self.dup_folder, operation="move")
                        if not self.dry_run:
                            self.db_queue.put(
                                (
                                    "execute",
                                    "INSERT OR REPLACE INTO files (path, processed, date_modified) VALUES (?, 1, CURRENT_TIMESTAMP)",
                                    (path,),
                                )
                            )
                    return {"status": "duplicate_handled"}

            time.sleep(self.API_SLEEP)
            try:
                if not file_data.get("fingerprint"):
                    return {"status": "unresolved", "path": path}

                import acoustid as acoustid_module

                resp = acoustid_module.lookup(
                    self.api_key,
                    file_data["fingerprint"],
                    file_data["duration"],
                    meta="recordings releases tracks",
                )
            except Exception as e:
                logging.error(f"API failed for {path}: {e}")
                traceback.print_exc()
                return {"status": "error", "path": path}

            candidates = (
                self._get_candidates(resp["results"])
                if resp.get("status") == "ok" and resp.get("results")
                else []
            )
            if not candidates:
                candidates = self._fallback_musicbrainz_search(path)
            if not candidates:
                return {"status": "unresolved", "path": path}

            current_acoustid_id = (
                resp["results"][0]["id"]
                if (resp.get("status") == "ok" and resp.get("results"))
                else candidates[0]["recording"]["id"]
            )
            if resp.get("status") == "ok" and resp.get("results"):
                self._update_fingerprint_cache(
                    current_acoustid_id, file_data["fingerprint"]
                )

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

            owned_candidates = [c for c in candidates if c.get("is_owned")]
            if owned_candidates:
                return {
                    "status": "auto_resolved",
                    "path": path,
                    "match": owned_candidates,
                    "data": file_data,
                    "acoustid": current_acoustid_id,
                    "quality": quality,
                }

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
                return {
                    "status": "auto_resolved",
                    "path": path,
                    "match": [sticky_match],
                    "data": file_data,
                    "acoustid": current_acoustid_id,
                    "quality": quality,
                }
            elif len(candidates) == 1 and candidates[0]["similarity"] >= 0.98:
                self.last_selected_album_id = candidates[0]["release"]["id"]
                return {
                    "status": "auto_resolved",
                    "path": path,
                    "match": [candidates[0]],
                    "data": file_data,
                    "acoustid": current_acoustid_id,
                    "quality": quality,
                }
            else:
                return {
                    "status": "needs_user",
                    "path": path,
                    "candidates": candidates,
                    "data": file_data,
                    "acoustid": current_acoustid_id,
                    "quality": quality,
                }

        # OPTIMIZATION: Increased worker count for I/O-bound API operations
        with ThreadPoolExecutor(max_workers=self.api_workers) as executor:
            futures = [executor.submit(_api_worker, data) for data in cpu_results]
            for future in as_completed(futures):
                if shutdown_event.is_set():
                    break
                try:
                    res = future.result()
                    if res["status"] == "unresolved":
                        self._safe_move(
                            res["path"], self.unresolved_folder, operation="move"
                        )
                    elif res["status"] == "needs_user":
                        ambiguous_queue.append(res)
                    elif res["status"] == "auto_resolved":
                        for idx, match in enumerate(res["match"]):
                            self._process_match_for_file(
                                res["path"],
                                res["acoustid"],
                                res["data"]["fingerprint"],
                                res["quality"],
                                res["data"]["hash"],
                                match,
                                idx == len(res["match"]) - 1,
                            )
                except Exception as e:
                    logging.error(f"API worker error: {e}")
                    traceback.print_exc()

        # --- PHASE 3: INTERACTIVE RESOLUTION ---
        if ambiguous_queue:
            print(
                f"\nStage 3: Manual Resolution ({len(ambiguous_queue)} files require attention)"
            )
            for item in ambiguous_queue:
                result = self._prompt_user_selection(item["path"], item["candidates"])
                if result == "quit":
                    break
                selected_matches = result or []

                if len(selected_matches) == 1:
                    self.last_selected_album_id = selected_matches[0]["release"]["id"]
                else:
                    self.last_selected_album_id = None

                for idx, match in enumerate(selected_matches):
                    self._process_match_for_file(
                        item["path"],
                        item["acoustid"],
                        item["data"]["fingerprint"],
                        item["quality"],
                        item["data"]["hash"],
                        match,
                        idx == len(selected_matches) - 1,
                    )

        # --- CLEANUP ---
        print("\nFinalizing database writes...")
        self.db_queue.put(None)
        self.writer_thread.join()

        self.cleanup_empty_folders()
        print("\nProcessing complete!")

    def _process_match_for_file(
        self,
        path,
        current_acoustid_id,
        fingerprint,
        quality,
        audio_hash,
        selected_match,
        is_last_item,
    ):
        rel, rec = selected_match["release"], selected_match["recording"]
        if not self._handle_album_deduplication(
            path,
            current_acoustid_id,
            rel.get("id"),
            quality,
            dispose_source=is_last_item,
        ):
            return

        artist = (
            rel.get("artists", [{}])[0].get("name")
            or rec.get("artists", [{}])[0].get("name")
            or "Unknown"
        )
        track_num, disc_num, found_track = 1, 1, False
        target_title = str(rec.get("title", "")).lower().strip()

        for medium in rel.get("mediums", []):
            for track in medium.get("tracks", []):
                if (
                    str(track.get("recording", {}).get("id")) == str(rec.get("id"))
                    or str(track.get("title", "")).lower().strip() == target_title
                ):
                    track_num, disc_num, found_track = (
                        track.get("position", 1),
                        medium.get("position", 1),
                        True,
                    )
                    break
            if found_track:
                break

        meta = {
            "title": rec.get("title", "Unknown"),
            "album": rel.get("title", "Unknown Album"),
            "artist": artist,
            "album_artist": rel.get("artists", [{}])[0].get("name") or artist,
            "track_no": track_num,
            "disc_no": disc_num,
            "release_date": str(rel.get("date", {}).get("year", "0000")),
            "release_id": rel.get("id"),
        }

        safe_artist, safe_album = self._sanitize_name(
            meta["album_artist"]
        ), self._sanitize_name(meta["album"])
        safe_filename = self._sanitize_name(
            f"{str(meta['track_no']).zfill(2)} - {meta['title']}{quality['format']}"
        )

        final_path = self._organize_file(
            path,
            safe_artist,
            safe_album,
            safe_filename,
            operation="move" if is_last_item else "copy",
        )
        if not final_path:
            return

        self._apply_tags(final_path, meta)

        self.db_queue.put(
            (
                "execute",
                "INSERT OR IGNORE INTO albums VALUES (?,?,?,?,?)",
                (
                    meta["release_id"],
                    meta["album"],
                    meta["album_artist"],
                    meta["release_date"],
                    rel.get("country", "XX"),
                ),
            )
        )

        self.db_queue.put(
            (
                "execute",
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
        )

        if audio_hash:
            self.db_queue.put(
                (
                    "execute",
                    "INSERT OR REPLACE INTO audio_hashes (audio_hash, path) VALUES (?, ?)",
                    (audio_hash, final_path),
                )
            )

        # Update in-memory cache
        with self.cache_lock:
            self.audio_hash_cache[audio_hash] = final_path
            if current_acoustid_id not in self.owned_ids_cache:
                self.owned_ids_cache[current_acoustid_id] = set()
            self.owned_ids_cache[current_acoustid_id].add(rel.get("id"))

        self._update_index(final_path, fingerprint)
        print(f" -> Success: {os.path.join(safe_artist, safe_album, safe_filename)}")

    def __del__(self):
        self.close()


if __name__ == "__main__":
    # Ensure Ctrl+C triggers graceful shutdown via the global event
    signal.signal(signal.SIGINT, lambda sig, frame: shutdown_event.set())
    config_filename = "library_management_config.json"

    if not os.path.exists(config_filename):
        print(
            f"[{config_filename}] not found. Generating a default configuration file..."
        )
        default_config = {
            "api_key": "7dlZplmc3N",
            "music_folder": "/mnt/NAS/cleanmusic/music2/",
            "destination_folder": "/mnt/NAS/cleanmusic/NewMaster/",
            "dup_folder": "/mnt/NAS/cleanmusic/duplicates/",
            "unresolved_folder": "/mnt/NAS/cleanmusic/unresolved/",
            "db_path": "library_manager.db",
            "dry_run": False,
            "prune": False,
            "hashAudio": False,
            "global_dedup": False,
            "process": True,
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

        if manager.hash_audio:
            manager.hash_existing_audio()
            print("-" * 40)

        if manager.run_process and not shutdown_event.is_set():
            manager.process_library()

    except KeyboardInterrupt:
        shutdown_event.set()
        print("\nProcess interrupted by user. Shutting down gracefully...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        traceback.print_exc()
    finally:
        shutdown_event.set()
        manager.close()