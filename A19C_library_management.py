import os
import sys
import time
import subprocess
import sqlite3
import logging
import shutil
import difflib
import mutagen
import traceback
import json
import hashlib
import threading
import socket
import musicbrainzngs
import acoustid
from mutagen.id3 import ID3, TPE1, TPE2, TRCK, TPOS, TIT2, TALB
from tqdm import tqdm
import concurrent.futures

# Set global timeout to prevent APIs from permanently hanging threads
socket.setdefaulttimeout(15)

# Initialize MusicBrainz API wrapper
musicbrainzngs.set_useragent(
    "MusicLibraryManager",
    "2.0",
    "https://github.com/MusicLibraryManager"
)

class MusicLibraryManager:
    def __init__(self, config_file="library_management_config.json"):
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

        self.api_key = config.get("api_key")
        self.music_folder = os.path.abspath(config.get("music_folder", ""))
        self.destination_folder = os.path.abspath(config.get("destination_folder", ""))
        self.dup_folder = os.path.abspath(config.get("dup_folder", ""))
        self.unresolved_folder = os.path.abspath(config.get("unresolved_folder", ""))
        self.db_path = config.get("db_path", "library_manager.db")

        def parse_bool(val, default=False):
            if isinstance(val, str): return val.strip().lower() in ("true", "1", "yes", "t")
            if val is None: return default
            return bool(val)

        self.dry_run = parse_bool(config.get("dry_run", False))
        self.prune = parse_bool(config.get("prune", False))
        self.hash_audio = parse_bool(config.get("hashAudio", False))
        self.run_process = parse_bool(config.get("process", True))
        self.global_dedup = parse_bool(config.get("global_dedup", False))

        self.player_process = None

        self.BLOCK_SIZE = 16
        self.SIMILARITY_AUTO = 0.98
        self.SIMILARITY_STICKY = 0.95
        self.SIMILARITY_ASK = 0.85

        self.db_lock = threading.RLock()
        self.mb_api_lock = threading.RLock()
        self.last_mb_call = 0
        self.acoustid_api_lock = threading.RLock()
        self.last_acoustid_call = 0

        self.last_selected_album_id = None
        self.processed_files = set()
        self.known_hashes = {}

        logging.basicConfig(
            filename="library_manager.log", level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cur = self.conn.cursor()
        self._setup_database()

        for folder in [self.dup_folder, self.destination_folder, self.unresolved_folder]:
            if not os.path.exists(folder): os.makedirs(folder)

    def _setup_database(self):
        with self.db_lock:
            self.cur.execute("PRAGMA foreign_keys = ON")
            self.cur.execute("PRAGMA journal_mode = WAL")
            self.cur.execute("PRAGMA busy_timeout = 10000")
            self.cur.execute("PRAGMA synchronous = NORMAL")

            self.cur.execute("""CREATE TABLE IF NOT EXISTS albums (release_id TEXT PRIMARY KEY, album_title TEXT, album_artist TEXT, release_date TEXT, country TEXT)""")
            self.cur.execute("""CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY, fingerprint TEXT, acoustid_id TEXT, title TEXT, track_no INTEGER, disc_no INTEGER, format TEXT, file_size INTEGER, quality_score REAL, album_id TEXT, processed INTEGER DEFAULT 0, date_modified DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (album_id) REFERENCES albums (release_id))""")
            
            try: self.cur.execute("ALTER TABLE files ADD COLUMN date_modified DATETIME DEFAULT CURRENT_TIMESTAMP")
            except sqlite3.OperationalError: pass

            self.cur.execute("""CREATE TRIGGER IF NOT EXISTS update_files_modtime AFTER UPDATE ON files FOR EACH ROW BEGIN UPDATE files SET date_modified = CURRENT_TIMESTAMP WHERE path = old.path; END;""")
            self.cur.execute("""CREATE TABLE IF NOT EXISTS known_fingerprints (fingerprint TEXT, acoustid_id TEXT, PRIMARY KEY (fingerprint, acoustid_id))""")
            self.cur.execute("""CREATE TABLE IF NOT EXISTS known_blocks (block TEXT, acoustid_id TEXT)""")
            self.cur.execute("""CREATE TABLE IF NOT EXISTS fingerprint_index (block TEXT, path TEXT, FOREIGN KEY(path) REFERENCES files(path) ON DELETE CASCADE)""")
            self.cur.execute("""CREATE TABLE IF NOT EXISTS audio_hashes (audio_hash TEXT PRIMARY KEY, path TEXT)""")
            self.cur.execute("""CREATE TABLE IF NOT EXISTS ambiguous_files (path TEXT PRIMARY KEY, candidates_json TEXT, acoustid_id TEXT, fingerprint TEXT, quality_json TEXT, audio_hash TEXT)""")

            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_acoustid ON files(acoustid_id)")
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_known_blocks ON known_blocks(block)")
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_file_blocks ON fingerprint_index(block)")
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_files_processed ON files(processed)")
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_files_dedup ON files(acoustid_id, album_id, processed)")
            self.cur.execute("CREATE INDEX IF NOT EXISTS idx_audio_hashes_path ON audio_hashes(path)")

            self.conn.commit()

    def _preload_state(self):
        print("Preloading database state into memory...")
        with self.db_lock:
            self.cur.execute("SELECT path FROM files WHERE processed = 1")
            self.processed_files = set(row[0] for row in self.cur.fetchall())
            
            self.known_hashes = {}
            self.cur.execute('SELECT a.audio_hash, a.path, f.quality_score FROM audio_hashes a LEFT JOIN files f ON a.path = f.path')
            for row in self.cur.fetchall():
                self.known_hashes[row[0]] = {"path": row[1], "score": row[2] if row[2] is not None else 0.0}
        print(f"Loaded {len(self.processed_files)} processed files and {len(self.known_hashes)} hashes.")

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
                    self.conn.execute("DELETE FROM fingerprint_index WHERE path = ?", (path_str,))
                    self.conn.execute("DELETE FROM audio_hashes WHERE path = ?", (path_str,))
                    removed_count += 1

        if removed_count > 0:
            logging.info("Pruned %d ghost entries from database.", removed_count)
            print(f"Cleaned up {removed_count} missing files from the database.")
        else:
            print("Database is clean.")

    def hash_existing_audio(self):
        """Retroactively generates 30-second audio hashes for files already in the database."""
        print("Scanning database for files missing audio hashes...")
        with self.db_lock:
            self.cur.execute('''
                SELECT f.path FROM files f 
                LEFT JOIN audio_hashes a ON f.path = a.path 
                WHERE a.path IS NULL AND f.processed = 1
            ''')
            paths = [row[0] for row in self.cur.fetchall()]

        if not paths:
            print("All processed files already have audio hashes.")
            return

        print(f"Generating hashes for {len(paths)} files...")
        success_count = 0

        for path in tqdm(paths, desc="Hashing Audio"):
            if not os.path.exists(path):
                continue
            
            audio_hash = self._get_audio_hash(path)
            if audio_hash:
                with self.db_lock:
                    self.cur.execute("INSERT OR REPLACE INTO audio_hashes (audio_hash, path) VALUES (?, ?)", (audio_hash, path))
                    self.conn.commit()
                success_count += 1

        print(f"Successfully generated {success_count} new audio hashes.")


    def _get_audio_hash(self, filepath):
        hasher = hashlib.md5()
        cmd = [
            "ffmpeg", "-v", "fatal", "-ss", "15", "-i", filepath,
            "-t", "5", "-map", "0:a:0", "-ac", "1", "-ar", "22050",
            "-f", "s16le", "-acodec", "pcm_s16le", "-"
        ]
        try:
            proc = subprocess.run(cmd, capture_output=True, stdin=subprocess.DEVNULL, close_fds=True, timeout=15)
            if proc.returncode == 0:
                hasher.update(proc.stdout)
                return hasher.hexdigest()
        except subprocess.TimeoutExpired:
            logging.warning(f"Audio hash FFMPEG timeout on {filepath}")
        except Exception:
            pass
        return None

    def _get_fingerprint(self, filepath):
        try:
            proc = subprocess.run(
                ["fpcalc", "-json", "-length", "120", filepath],
                capture_output=True, stdin=subprocess.DEVNULL, close_fds=True, timeout=30, text=True
            )
            if proc.returncode == 0:
                data = json.loads(proc.stdout)
                return data.get("duration"), data.get("fingerprint")
        except subprocess.TimeoutExpired:
            logging.warning(f"fpcalc timeout on {filepath}")
        except Exception:
            pass
        return None, None

    def _get_blocks(self, fingerprint):
        return [fingerprint[i : i + self.BLOCK_SIZE] for i in range(0, len(fingerprint), self.BLOCK_SIZE)][:16]

    def _update_fingerprint_cache(self, acoustid_id, fingerprint):
        try:
            with self.db_lock:
                self.cur.execute("INSERT OR IGNORE INTO known_fingerprints (fingerprint, acoustid_id) VALUES (?, ?)", (fingerprint, acoustid_id))
                self.cur.execute("SELECT 1 FROM known_blocks WHERE acoustid_id = ? LIMIT 1", (acoustid_id,))
                if not self.cur.fetchone():
                    blocks = [(b, acoustid_id) for b in self._get_blocks(fingerprint)]
                    self.cur.executemany("INSERT INTO known_blocks (block, acoustid_id) VALUES (?, ?)", blocks)
                self.conn.commit()
        except sqlite3.Error: pass

    def _update_index(self, path, fingerprint):
        with self.db_lock:
            self.cur.execute("DELETE FROM fingerprint_index WHERE path = ?", (path,))
            blocks = [(b, path) for b in self._get_blocks(fingerprint)]
            self.cur.executemany("INSERT INTO fingerprint_index (block, path) VALUES (?, ?)", blocks)
            self.conn.commit()

    def _get_owned_release_ids(self, acoustid_id):
        try:
            with self.db_lock:
                self.cur.execute("SELECT DISTINCT album_id FROM files WHERE acoustid_id = ? AND processed = 1", (acoustid_id,))
                return set(row[0] for row in self.cur.fetchall())
        except sqlite3.Error: return set()

    def _calculate_quality(self, file_path):
        try:
            audio = mutagen.File(file_path)
            if not audio: return None
            info = audio.info
            ext = os.path.splitext(file_path)[1].lower()
            file_size = os.path.getsize(file_path)

            format_hierarchy = {".flac": 3 * 10**15, ".m4a": 2.5 * 10**15, ".wav": 2 * 10**15, ".mp3": 1 * 10**15, ".wma": 0.5 * 10**15}
            fmt_score = format_hierarchy.get(ext, 0)
            bits = getattr(info, "bits_per_sample", 16)
            bit_score = bits * 10**12
            size_score = file_size / 1000 
            sample_rate = getattr(info, "sample_rate", 44100)
            bitrate = getattr(info, "bitrate", 0)

            final_score = fmt_score + bit_score + size_score + (sample_rate / 10**6) + (bitrate / 10**9)
            return {"score": final_score, "format": ext, "size": file_size, "bitrate": bitrate, "sample_rate": sample_rate, "bits": bits}
        except Exception: return None

    def _fallback_musicbrainz_search(self, file_path):
        try:
            audio = mutagen.File(file_path, easy=True)
            if not audio: return []
            title = audio.get("title", [""])[0]
            artist = audio.get("artist", [""])[0]
            album = audio.get("album", [""])[0]
            if not title or not artist: return []

            with self.mb_api_lock:
                now = time.time()
                elapsed = now - self.last_mb_call
                wait_time = max(0, 1.0 - elapsed)
                self.last_mb_call = now + wait_time
                
            if wait_time > 0: time.sleep(wait_time)
                
            query = f'artist:"{artist}" AND recording:"{title}"'
            if album: query += f' AND release:"{album}"'
            
            musicbrainzngs.set_timeout(10)
            result = musicbrainzngs.search_recordings(query=query, limit=5)
            
            candidates = []
            for rec in result.get("recording-list", []):
                rec_title = rec.get("title", "Unknown")
                rec_id = rec.get("id")
                for rel in rec.get("release-list", []):
                    rel_id = rel.get("id")
                    track_pos, disc_pos = 1, 1
                    if "medium-list" in rel:
                        for med in rel["medium-list"]:
                            disc_pos = med.get("position", 1)
                            for trk in med.get("track-list", []):
                                if trk.get("recording", {}).get("id") == rec_id:
                                    track_pos = trk.get("number", 1)
                                    break

                    mock_release = {"id": rel_id, "title": rel.get("title", "Unknown Album"), "artists": [{"name": artist}], "date": {"year": rel.get("date", "0000")[:4]}, "country": rel.get("country", "XX"), "mediums": [{"position": disc_pos, "tracks": [{"position": track_pos, "title": rec_title, "recording": {"id": rec_id}}]}]}
                    candidates.append({"similarity": float(rec.get("ext:score", 0)) / 100.0, "recording_title": rec_title, "album_title": mock_release["title"], "artist": artist, "date": str(mock_release["date"]["year"]), "country": mock_release["country"], "release": mock_release, "recording": {"id": rec_id, "title": rec_title, "artists": [{"name": artist}]}, "is_owned": False})
            candidates.sort(key=lambda x: x["similarity"], reverse=True)
            return candidates
        except Exception: return []

    def _get_candidates(self, results):
        candidates, seen_releases = [], set()
        for result in results:
            match_score = result.get("score", 0) or 0
            for recording in (result.get("recordings") or []):
                rec_title = recording.get("title", "Unknown")
                for release in (recording.get("releases") or []):
                    rel_id = release.get("id")
                    if rel_id in seen_releases: continue
                    seen_releases.add(rel_id)
                    candidates.append({"similarity": match_score, "recording_title": rec_title, "album_title": release.get("title", "Unknown Album"), "artist": release.get("artists", [{}])[0].get("name", "Unknown Artist"), "date": str(release.get("date", {}).get("year", "Unknown")), "country": release.get("country", "XX"), "release": release, "recording": recording})
        candidates.sort(key=lambda x: (x["similarity"], x["country"] == "US", x["date"]), reverse=True)
        return candidates

    def _play_audio(self, file_path):
        self._stop_audio()
        commands = []
        if sys.platform == "darwin": commands.append(["afplay", file_path])
        commands.extend([
            ["ffplay", "-nodisp", "-autoexit", "-hide_banner", "-loglevel", "quiet", file_path], 
            ["mpv", "--no-video", "--quiet", file_path], 
            ["cvlc", "--play-and-exit", "--quiet", file_path]
        ])
        for cmd_args in commands:
            if shutil.which(cmd_args[0]):
                try:
                    self.player_process = subprocess.Popen(
                        cmd_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                    )
                    return
                except Exception: continue
        print("(!) No supported audio player found.")

    def _stop_audio(self):
        if self.player_process:
            try:
                self.player_process.terminate()
                self.player_process.wait(timeout=0.5)
            except subprocess.TimeoutExpired: self.player_process.kill()
            except Exception: pass
            self.player_process = None

    def close(self):
        self._stop_audio()
        if hasattr(self, "conn") and self.conn:
            try:
                with self.db_lock:
                    self.conn.commit()
                    self.conn.close()
                self.conn = None
            except sqlite3.Error: pass

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
                print(f"{'#':<3} {'Own':<3} {'Sim':<5} {'Ctry':<4} {'Date':<6} {'Artist':<25} {'Album'}")
                print("-" * 80)

                for i, c in enumerate(current_batch):
                    global_idx = start_idx + i + 1
                    sim_pct = f"{int(c['similarity'] * 100)}%"
                    own_mark = "*" if c.get("is_owned") else ""
                    print(f"{global_idx:<3} {own_mark:<3} {sim_pct:<5} {c['country']:<4} {c['date']:<6} {c['artist'][:25]:<25} {c['album_title']}")

                print("-" * 80)

                prompt_options = []
                if current_page < total_pages - 1: prompt_options.append("(N)ext")
                if current_page > 0: prompt_options.append("(P)rev")
                prompt_options.append("(R)eplay")
                prompt_options.append("(0) Skip")
                prompt_options.append("(Q)uit")

                prompt_str = f"Select Album # (1-{len(candidates)}, comma-separated), " + ", ".join(prompt_options)
                choice = input(f"{prompt_str}: ").lower().strip()

                if choice == "0": return []
                elif choice == "q": return "quit"
                elif choice == "r": self._play_audio(file_path); continue
                elif choice == "n" and current_page < total_pages - 1: current_page += 1
                elif choice == "p" and current_page > 0: current_page -= 1
                else:
                    try:
                        selections = []
                        valid = True
                        for part in choice.split(","):
                            part = part.strip()
                            if not part: continue
                            idx = int(part)
                            if 1 <= idx <= len(candidates): selections.append(candidates[idx - 1])
                            else: valid = False; break
                        if valid and selections: return selections
                        print("Invalid selection.")
                    except ValueError: print("Invalid selection.")
        finally:
            self._stop_audio()

    def _safe_move(self, src_path, target_dir, target_filename=None, operation="move"):
        if not os.path.exists(src_path): return None
        if not target_filename: target_filename = os.path.basename(src_path)
        clean_filename = self._sanitize_name(target_filename)
        dir_created = False
        
        if not self.dry_run and not os.path.exists(target_dir):
            try: os.makedirs(target_dir); dir_created = True
            except OSError: return None

        target_path = os.path.join(target_dir, clean_filename)
        if os.path.abspath(src_path) == os.path.abspath(target_path): return target_path

        base, ext = os.path.splitext(clean_filename)
        counter = 1
        while os.path.exists(target_path):
            if os.path.abspath(src_path) == os.path.abspath(target_path): return target_path
            target_path = os.path.join(target_dir, f"{base} ({counter}){ext}")
            counter += 1

        if self.dry_run: return target_path

        try:
            if operation == "move": shutil.move(src_path, target_path)
            else: shutil.copy2(src_path, target_path)
            return target_path
        except Exception:
            if dir_created and os.path.exists(target_dir) and not os.listdir(target_dir):
                try: os.removedirs(target_dir)
                except OSError: pass
            return None

    def cleanup_empty_folders(self):
        if not os.path.exists(self.music_folder): return
        print("Cleaning up empty source folders...")
        for root, dirs, _ in os.walk(self.music_folder, topdown=False):
            for name in dirs:
                try: os.rmdir(os.path.join(root, name))
                except OSError: pass

    def _handle_album_deduplication(self, path, acoustid_id, release_id, quality, dispose_source=False):
        with self.db_lock:
            if self.global_dedup: self.cur.execute("SELECT path, quality_score FROM files WHERE acoustid_id = ? AND processed = 1", (acoustid_id,))
            else: self.cur.execute("SELECT path, quality_score FROM files WHERE acoustid_id = ? AND album_id = ? AND processed = 1", (acoustid_id, release_id))
            existing = self.cur.fetchone()

        if not existing: return True

        existing_path, existing_score = existing
        existing_score = existing_score or 0.0

        if quality["score"] > existing_score:
            print(f" -> Upgrading existing file (Quality: {existing_score} -> {quality['score']})")
            if not self.dry_run:
                self._safe_move(existing_path, self.dup_folder, operation="move")
                with self.db_lock:
                    self.cur.execute("DELETE FROM files WHERE path = ?", (existing_path,))
                    self.conn.commit()
            return True
        else:
            print(f" -> Duplicate found (lower/equal quality). Moving to duplicates.")
            if dispose_source and not self.dry_run:
                self._safe_move(path, self.dup_folder, operation="move")
                with self.db_lock:
                    self.cur.execute("""INSERT OR REPLACE INTO files (path, processed, acoustid_id, quality_score, format, file_size, date_modified) VALUES (?, 1, ?, ?, ?, ?, CURRENT_TIMESTAMP)""", (path, acoustid_id, quality["score"], quality["format"], quality["size"]))
                    self.processed_files.add(path)
                    self.conn.commit()
            return False

    def _apply_tags(self, file_path, meta):
        if self.dry_run: return
        try:
            audio = mutagen.File(file_path)
            if not audio: return
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
                audio["artist"], audio["albumartist"] = meta["artist"], meta["album_artist"]
                audio["tracknumber"], audio["discnumber"] = str(meta["track_no"]), str(meta["disc_no"])
                audio.save()
            elif ext.endswith((".m4a", ".mp4")):
                audio["\xa9nam"], audio["\xa9alb"] = meta["title"], meta["album"]
                audio["\xa9ART"], audio["aART"] = meta["artist"], meta["album_artist"]
                audio["trkn"], audio["disk"] = [(int(meta["track_no"]), 0)], [(int(meta["disc_no"]), 0)]
                audio.save()
            elif ext.endswith(".wma"):
                audio["Title"], audio["WM/AlbumTitle"] = meta["title"], meta["album"]
                audio["Author"], audio["WM/AlbumArtist"] = meta["artist"], meta["album_artist"]
                audio["WM/TrackNumber"], audio["WM/PartOfSet"] = str(meta["track_no"]), str(meta["disc_no"])
                audio.save()
        except Exception: pass

    def _sanitize_name(self, name):
        if not name: return "Unknown"
        cleaned = name.replace("/", "-").replace("\\", "-")
        return "".join(c for c in cleaned if c.isalnum() or c in " -_.").strip()

    def _organize_file(self, current_path, artist_dir, album_dir, filename, operation="move"):
        return self._safe_move(current_path, os.path.join(self.destination_folder, artist_dir, album_dir), filename, operation=operation)

    # --- PHASE 1: Concurrent Processing ---
    def process_library(self):
        self._preload_state()

        files = [os.path.join(r, f) for r, _, fs in os.walk(self.music_folder) for f in fs if f.lower().endswith((".mp3", ".flac", ".m4a", ".mp4", ".wma", ".wav"))]
        print(f"Found {len(files)} supported files. Starting multithreaded phase 1...")

        io_threads = min(8, (os.cpu_count() or 1) + 2) 

        with concurrent.futures.ThreadPoolExecutor(max_workers=io_threads) as executor:
            futures = []
            for path in files:
                if path in self.processed_files: continue
                futures.append(executor.submit(self._process_single_file_concurrent, path))

            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing DB & API"):
                try: future.result()
                except Exception as e: logging.error(f"Thread Error: {e}")

        with self.db_lock: self.conn.commit()
        self.cleanup_empty_folders()
        time.sleep(1)
        self.resolve_ambiguous_files()

    def _process_single_file_concurrent(self, path):
        try:
            if os.path.getsize(path) == 0: return
        except OSError: return

        quality = self._calculate_quality(path)
        if not quality: return

        audio_hash = self._get_audio_hash(path)
        if audio_hash:
            upgrade_path = None
            is_duplicate = False

            with self.db_lock:
                if audio_hash in self.known_hashes:
                    existing_info = self.known_hashes[audio_hash]
                    existing_path, existing_score = existing_info["path"], existing_info["score"]

                    if quality["score"] > existing_score:
                        upgrade_path = existing_path
                        self.known_hashes[audio_hash] = {"path": path, "score": quality["score"]}
                    else: is_duplicate = True

            if upgrade_path:
                if not self.dry_run:
                    self._safe_move(upgrade_path, self.dup_folder, operation="move")
                    with self.db_lock:
                        self.cur.execute("DELETE FROM files WHERE path = ?", (upgrade_path,))
                        self.cur.execute("UPDATE audio_hashes SET path = ? WHERE audio_hash = ?", (path, audio_hash))
                        self.conn.commit()
            elif is_duplicate:
                self._safe_move(path, self.dup_folder, operation="move")
                if not self.dry_run:
                    with self.db_lock:
                        self.cur.execute("INSERT OR REPLACE INTO files (path, processed, date_modified) VALUES (?, 1, CURRENT_TIMESTAMP)", (path,))
                        self.processed_files.add(path)
                        self.conn.commit()
                return

        duration, fingerprint = self._get_fingerprint(path)
        if not fingerprint:
            self._safe_move(path, self.unresolved_folder, operation="move")
            return

        try:
            with self.acoustid_api_lock:
                now = time.time()
                elapsed = now - self.last_acoustid_call
                wait_time = max(0, 0.35 - elapsed)
                self.last_acoustid_call = now + wait_time
                
            if wait_time > 0: time.sleep(wait_time)
                
            resp = acoustid.lookup(self.api_key, fingerprint, duration, meta="recordings releases tracks")

            candidates = []
            if resp.get("status") == "ok" and resp.get("results"): candidates = self._get_candidates(resp["results"])
            if not candidates: candidates = self._fallback_musicbrainz_search(path)
            if not candidates:
                self._safe_move(path, self.unresolved_folder, operation="move")
                return

            current_acoustid_id = resp["results"][0]["id"] if (resp.get("status") == "ok" and resp.get("results")) else candidates[0]["recording"]["id"]
            if resp.get("status") == "ok" and resp.get("results"): self._update_fingerprint_cache(current_acoustid_id, fingerprint)

            owned_ids = self._get_owned_release_ids(current_acoustid_id)
            for c in candidates: c["is_owned"] = c["release"]["id"] in owned_ids

            candidates.sort(key=lambda x: (x["is_owned"], x["similarity"], x["country"] == "US", x["date"]), reverse=True)
            top_match = candidates[0]
            
            selected_matches = []
            owned_candidates = [c for c in candidates if c.get("is_owned")]

            if owned_candidates: selected_matches = owned_candidates
            else:
                sticky_match = None
                if self.last_selected_album_id:
                    for c in candidates:
                        if c["release"]["id"] == self.last_selected_album_id and c["similarity"] >= self.SIMILARITY_STICKY:
                            sticky_match = c; break

                if sticky_match: selected_matches = [sticky_match]
                elif len(candidates) == 1 and top_match["similarity"] >= 0.98:
                    selected_matches = [top_match]
                    self.last_selected_album_id = top_match["release"]["id"]
                else:
                    self._save_ambiguous_to_db(path, candidates, current_acoustid_id, fingerprint, quality, audio_hash)
                    return

            if selected_matches:
                for idx, match in enumerate(selected_matches):
                    self._process_match_for_file(path, current_acoustid_id, fingerprint, quality, audio_hash, match, idx == len(selected_matches) - 1)

        except Exception as e: logging.error(f"Critical Failure on {path}: {e}")

    def _save_ambiguous_to_db(self, path, candidates, current_acoustid_id, fingerprint, quality, audio_hash):
        if self.dry_run: return
        with self.db_lock:
            self.cur.execute("INSERT OR REPLACE INTO ambiguous_files VALUES (?, ?, ?, ?, ?, ?)", (path, json.dumps(candidates), current_acoustid_id, fingerprint, json.dumps(quality), audio_hash))
            self.conn.commit()

    # --- PHASE 2: Interactive Disambiguation ---
    def resolve_ambiguous_files(self):
        with self.db_lock:
            self.cur.execute("SELECT path, candidates_json, acoustid_id, fingerprint, quality_json, audio_hash FROM ambiguous_files")
            rows = self.cur.fetchall()

        if not rows:
            print("\nAll files successfully processed automatically. No ambiguous files require review.")
            return

        total_files = len(rows)
        print(f"\n--- Phase 2: Resolving {total_files} Ambiguous Files ---")
        
        for i, row in enumerate(rows):
            path, cand_json, acoustid_id, fingerprint, qual_json, audio_hash = row
            candidates, quality = json.loads(cand_json), json.loads(qual_json)

            if not os.path.exists(path):
                if not self.dry_run:
                    with self.db_lock:
                        self.cur.execute("DELETE FROM ambiguous_files WHERE path = ?", (path,))
                        self.conn.commit()
                continue

            print(f"\n--- Resolving File {i + 1} of {total_files} ---")
            
            sticky_match = None
            if self.last_selected_album_id:
                for c in candidates:
                    if c["release"]["id"] == self.last_selected_album_id and c["similarity"] >= self.SIMILARITY_STICKY:
                        sticky_match = c
                        break

            if sticky_match:
                print(f" -> Auto-selected sticky album: {sticky_match['album_title']}")
                result = [sticky_match]
            else:
                result = self._prompt_user_selection(path, candidates)
            
            if result == "quit":
                print("Exiting interactive resolution. Remaining files will be saved for next time.")
                break
            elif result:
                if len(result) == 1:
                    self.last_selected_album_id = result[0]["release"]["id"]
                else:
                    self.last_selected_album_id = None
                
                for idx, selected_match in enumerate(result): 
                    self._process_match_for_file(path, acoustid_id, fingerprint, quality, audio_hash, selected_match, idx == len(result) - 1)
            
            if not self.dry_run:
                with self.db_lock:
                    self.cur.execute("DELETE FROM ambiguous_files WHERE path = ?", (path,))
                    self.conn.commit()

    def _process_match_for_file(self, path, current_acoustid_id, fingerprint, quality, audio_hash, selected_match, is_last_item):
        rel, rec = selected_match["release"], selected_match["recording"]
        target_recording_id, target_release_id = rec.get("id"), rel.get("id")

        if not self._handle_album_deduplication(path, current_acoustid_id, target_release_id, quality, dispose_source=is_last_item): return

        artist = rel.get("artists", [{}])[0].get("name") or rec.get("artists", [{}])[0].get("name") or "Unknown"
        track_num, disc_num, found_track = 1, 1, False
        target_title = str(rec.get("title", "")).lower().strip()

        for medium in rel.get("mediums", []):
            current_disc = medium.get("position", 1)
            for track in medium.get("tracks", []):
                if str(track.get("recording", {}).get("id")) == str(target_recording_id): track_num, disc_num, found_track = track.get("position", 1), current_disc, True; break
            if found_track: break

        if not found_track:
            for medium in rel.get("mediums", []):
                current_disc = medium.get("position", 1)
                for track in medium.get("tracks", []):
                    if str(track.get("title", "")).lower().strip() == target_title: track_num, disc_num, found_track = track.get("position", 1), current_disc, True; break
                if found_track: break

        meta = {"title": rec.get("title", "Unknown"), "album": rel.get("title", "Unknown Album"), "artist": artist, "album_artist": rel.get("artists", [{}])[0].get("name") or artist, "track_no": track_num, "disc_no": disc_num, "release_date": str(rel.get("date", {}).get("year", "0000")), "release_id": target_release_id}

        safe_artist, safe_album = self._sanitize_name(meta["album_artist"]), self._sanitize_name(meta["album"])
        safe_filename = self._sanitize_name(f"{str(meta['track_no']).zfill(2)} - {meta['title']}{quality['format']}")

        final_path = self._organize_file(path, safe_artist, safe_album, safe_filename, operation="move" if is_last_item else "copy")
        if not final_path: return

        if self.dry_run:
            print(f" [Dry Run] Would tag and process to: {safe_artist}/{safe_album}/{safe_filename}")
            return

        self._apply_tags(final_path, meta)

        with self.db_lock:
            self.cur.execute("INSERT OR IGNORE INTO albums VALUES (?,?,?,?,?)", (meta["release_id"], meta["album"], meta["album_artist"], meta["release_date"], rel.get("country", "XX")))
            self.cur.execute("""INSERT OR REPLACE INTO files (path, fingerprint, acoustid_id, title, track_no, disc_no, format, file_size, quality_score, album_id, processed, date_modified) VALUES (?,?,?,?,?,?,?,?,?,?,?, CURRENT_TIMESTAMP)""", (final_path, fingerprint, current_acoustid_id, meta["title"], meta["track_no"], meta["disc_no"], quality["format"], quality["size"], quality["score"], meta["release_id"], 1))
            self.processed_files.add(final_path)
            
            self.cur.execute("DELETE FROM fingerprint_index WHERE path = ?", (final_path,))
            blocks = [(b, final_path) for b in self._get_blocks(fingerprint)]
            self.cur.executemany("INSERT INTO fingerprint_index (block, path) VALUES (?, ?)", blocks)
            
            if audio_hash:
                self.cur.execute("INSERT OR REPLACE INTO audio_hashes (audio_hash, path) VALUES (?, ?)", (audio_hash, final_path))
                self.known_hashes[audio_hash] = {"path": final_path, "score": quality["score"]}
                
            self.conn.commit()
            
            print(f" -> Success: Processed to {safe_artist}/{safe_album}/{safe_filename}")

    def __del__(self): self.close()

if __name__ == "__main__":
    config_filename = "A18b.json"
    if not os.path.exists(config_filename):
        with open(config_filename, "w") as f: json.dump({"api_key": "7dlZplmc3N", "music_folder": "/mnt/NAS/cleanmusic/music2/", "destination_folder": "/mnt/NAS/cleanmusic/NewMaster/", "dup_folder": "/mnt/NAS/cleanmusic/duplicates/", "unresolved_folder": "/mnt/NAS/cleanmusic/unresolved/", "db_path": "library_manager.db", "dry_run": False, "prune": False, "hashAudio": False, "global_dedup": False, "process": True}, f, indent=4)
        sys.exit(0)

    manager = MusicLibraryManager(config_file=config_filename)
    if manager.dry_run: print("\n[!] DRY RUN MODE ACTIVATED [!]\n")
    try:
        if manager.prune: manager.prune_database()
        if manager.hash_audio: manager.hash_existing_audio()
        if manager.run_process: manager.process_library()
    except KeyboardInterrupt: print("\nProcess interrupted by user. Shutting down gracefully...")
    finally: manager.close()