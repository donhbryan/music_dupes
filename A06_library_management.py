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
from mutagen.id3 import ID3, TPE1, TPE2, TRCK, TPOS, TIT2, TALB
from tqdm import tqdm


class MusicLibraryManager:
    def __init__(
        self,
        api_key,
        music_folder,
        destination_folder,
        dup_folder,
        db_path="library.db",
        dry_run=False,
    ):
        self.api_key = api_key
        self.music_folder = os.path.abspath(music_folder)
        self.destination_folder = os.path.abspath(destination_folder)
        self.dup_folder = os.path.abspath(dup_folder)
        self.db_path = db_path
        self.dry_run = dry_run
        self.player_process = None

        # Tuning for fuzzy matching
        self.BLOCK_SIZE = 16
        self.SIMILARITY_AUTO = 0.98  # Automatically handle matches > 98%
        self.SIMILARITY_ASK = 0.85  # Ask user for matches > 85%
        self.API_SLEEP = 0.25  # Throttle API calls

        logging.basicConfig(
            filename="library_manager.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        self._setup_database()

        # Pruning MUST happen after DB init
        self.prune_database()

        if not os.path.exists(self.dup_folder):
            os.makedirs(self.dup_folder)

        if not os.path.exists(self.destination_folder):
            os.makedirs(self.destination_folder)

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
                            FOREIGN KEY (album_id) REFERENCES albums (release_id)
                        )"""
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_acoustid ON files(acoustid_id)"
        )

        # --- NEW: Blocking Strategy Table ---
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS fingerprint_index (
                            block TEXT,
                            path TEXT,
                            FOREIGN KEY(path) REFERENCES files(path) ON DELETE CASCADE
                        )"""
        )
        self.cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_block ON fingerprint_index(block)"
        )
        self.conn.commit()

    def prune_database(self):
        """Checks DB entries against filesystem and removes non-existent files."""
        if not os.path.exists(self.music_folder):
            logging.error(
                "Music folder %s not found. Aborting prune to prevent DB wipe.",
                self.music_folder,
            )
            print("(!) Music folder not found. Skipping prune safety check.")
            return

        print("Checking for ghost entries in database...")
        removed_count = 0

        # Use the existing database connection from DatabaseHandler
        with self.conn:
            cursor = self.conn.execute("SELECT path FROM files")
            all_paths = cursor.fetchall()

            for (path_str,) in tqdm(all_paths, desc="Pruning DB"):
                if not os.path.exists(path_str):
                    # File is missing, delete from both tables
                    self.conn.execute("DELETE FROM files WHERE path = ?", (path_str,))
                    # self.conn.execute(
                    #     "DELETE FROM fingerprint_index WHERE path = ?", (path_str,)
                    # )
                    removed_count += 1

        if removed_count > 0:
            logging.info("Pruned %d ghost entries from database.", removed_count)
            print(f"cleaned up {removed_count} missing files from the database.")
        else:
            print("Database is clean.")

    # --- NEW: Fingerprint Indexing Helpers ---
    def _get_blocks(self, fingerprint):
        """Splits fingerprint into chunks for indexing."""
        # We take up to 16 blocks of size 16 to keep the index efficient
        return [
            fingerprint[i : i + self.BLOCK_SIZE]
            for i in range(0, len(fingerprint), self.BLOCK_SIZE)
        ][:16]

    def _update_index(self, path, fingerprint):
        """Updates the blocking index for a new file."""
        self.cur.execute("DELETE FROM fingerprint_index WHERE path = ?", (path,))
        blocks = [(b, path) for b in self._get_blocks(fingerprint)]
        self.cur.executemany(
            "INSERT INTO fingerprint_index (block, path) VALUES (?, ?)", blocks
        )

    def _find_local_fuzzy_match(self, fingerprint):
        """
        Uses blocking strategy to find candidates, then difflib for fuzzy matching.
        Returns: (best_match_path, match_score, match_record)
        """
        blocks = self._get_blocks(fingerprint)
        if not blocks:
            return None, 0.0, None

        # 1. Block Search: Find files sharing fingerprint blocks
        placeholders = ",".join(["?"] * len(blocks))
        query = f"SELECT DISTINCT path FROM fingerprint_index WHERE block IN ({placeholders})"
        self.cur.execute(query, blocks)
        candidates = [row[0] for row in self.cur.fetchall()]

        best_path = None
        best_score = 0.0
        best_record = None

        # 2. Fuzzy Matching: Detailed comparison
        for cand_path in candidates:
            self.cur.execute(
                "SELECT fingerprint, quality_score, format, file_size FROM files WHERE path = ?",
                (cand_path,),
            )
            res = self.cur.fetchone()
            if not res:
                continue

            cand_fp, cand_q, cand_fmt, cand_size = res

            # Calculate similarity
            ratio = difflib.SequenceMatcher(None, fingerprint, cand_fp).ratio()

            if ratio > best_score:
                best_score = ratio
                best_path = cand_path
                best_record = {"score": cand_q, "format": cand_fmt, "size": cand_size}

        return best_path, best_score, best_record

    def _calculate_quality(self, file_path):
        """Generates a quality score based on format, bit depth, size."""
        try:
            time.sleep(self.API_SLEEP)  # Pause to let file system settle
            audio = mutagen.File(file_path)
            if not audio:
                return None

            info = audio.info
            ext = os.path.splitext(file_path)[1].lower()
            file_size = os.path.getsize(file_path)

            # Rank 1: Format (Quadrillions)
            is_lossless = ext in [".flac", ".wav", ".m4a"] and hasattr(
                info, "bits_per_sample"
            )
            fmt_score = 2 * 10**15 if is_lossless else 1 * 10**15

            # Rank 2: Bit Depth (Trillions)
            bits = getattr(info, "bits_per_sample", 16)
            bit_score = bits * 10**12

            # Rank 3: File Size (Raw Bytes)
            size_score = file_size

            # Rank 4: Tie Breakers
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
            match_score = result.get("score", 0)
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
        """Terminates the background audio process if running."""
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

    def _prompt_dedup_resolution(
        self, new_path, new_qual, old_path, old_qual, similarity
    ):
        """Interactive prompt for resolving uncertain local duplicates."""
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
            choice = input("    Keep (N)ew, (O)ld, or (S)kip? ").lower()
            if choice == "n":
                self._stop_audio()
                return "new"
            elif choice == "o":
                self._stop_audio()
                return "old"
            elif choice == "s":
                self._stop_audio()
                return "skip"

    def _prompt_user_selection(self, file_path, candidates):
        """Interactive terminal menu for ambiguous API matches."""
        filename = os.path.basename(file_path)
        print(f"\n[!] Ambiguous API Match for file: {filename}")
        print(f"    (Playing audio preview...)")
        print(f"{'#':<3} {'Sim':<5} {'Ctry':<4} {'Date':<6} {'Artist':<20} {'Album'}")
        print("-" * 80)

        top_candidates = candidates[:10]
        for idx, c in enumerate(top_candidates):
            sim_pct = f"{int(c['similarity'] * 100)}%"
            print(
                f"{idx+1:<3} {sim_pct:<5} {c['country']:<4} {c['date']:<6} {c['artist'][:19]:<20} {c['album_title']}"
            )
        print(f"{0:<3} SKIP THIS FILE")
        print("-" * 80)

        self._play_audio(file_path)
        try:
            while True:
                try:
                    choice = input("Select Album # (or 0 to skip): ")
                    choice_idx = int(choice)
                    if choice_idx == 0:
                        return None
                    if 1 <= choice_idx <= len(top_candidates):
                        return top_candidates[choice_idx - 1]
                except ValueError:
                    pass
                print("Invalid selection. Try again.")
        finally:
            self._stop_audio()

    def _safe_move(self, src_path, target_dir, target_filename=None):
        """
        Moves a file to a target directory with sanitization, collision handling, and creation.
        """
        if not os.path.exists(src_path):
            logging.warning(f"Cannot move missing file: {src_path}")
            return None

        # 1. Determine Filename
        if not target_filename:
            target_filename = os.path.basename(src_path)

        # 2. Sanitize Filename
        clean_filename = self._sanitize_name(target_filename)
        
        # 3. Ensure Target Directory Exists
        if not self.dry_run and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # 4. Construct Target Path & Handle Collisions
        target_path = os.path.join(target_dir, clean_filename)

        # If src and dest are same, we are done
        if os.path.abspath(src_path) == os.path.abspath(target_path):
            return target_path

        base, ext = os.path.splitext(clean_filename)
        counter = 1
        while os.path.exists(target_path):
            # Check if it's the same file (e.g. case sensitivity issues or hardlinks)
            if os.path.abspath(src_path) == os.path.abspath(target_path):
                return target_path

            target_path = os.path.join(target_dir, f"{base} ({counter}){ext}")
            counter += 1

        # 5. Move
        if self.dry_run:
            logging.info(f"[DRY RUN] Move: {src_path} -> {target_path}")
            return target_path

        try:
            shutil.move(src_path, target_path)
            logging.info(f"Moved: {src_path} -> {target_path}")
            return target_path
        except Exception as e:
            logging.error(f"Failed to move {src_path} -> {target_path}: {e}")
            return None

    def _handle_local_deduplication(self, path, fingerprint, quality):
        """
        Performs fuzzy matching against local DB.
        Returns True if we should proceed with this file (it's new or an upgrade).
        Returns False if we should discard this file (it's a worse duplicate).
        """
        match_path, match_score, match_record = self._find_local_fuzzy_match(
            fingerprint
        )

        if not match_path or match_score < self.SIMILARITY_ASK:
            return True  # No significant match, treat as new

        # Check for Auto-Match or User Prompt
        decision = "old"  # Default to keeping existing

        if match_score >= self.SIMILARITY_AUTO:
            # Auto-decide based on quality
            if quality["score"] > match_record["score"]:
                decision = "new"
                logging.info(
                    f"Auto-Upgrade (Sim {match_score:.2f}): {match_path} -> {path}"
                )
            else:
                decision = "old"
                logging.info(
                    f"Auto-Discard (Sim {match_score:.2f}): {path} is not better than {match_path}"
                )
        else:
            # Ask User
            decision = self._prompt_dedup_resolution(
                path, quality, match_path, match_record, match_score
            )

        # Execute Decision
        if decision == "new":
            # Remove old file from DB and Disk
            try:
                if not self.dry_run:
                    # Use Safe Move to duplicates folder
                    self._safe_move(match_path, self.dup_folder)

                self.cur.execute("DELETE FROM files WHERE path = ?", (match_path,))
                self.cur.execute(
                    "DELETE FROM fingerprint_index WHERE path = ?", (match_path,)
                )
                self.conn.commit()
                return True
            except Exception as e:
                logging.error(f"Error removing old file {match_path}: {e}")
                return True

        elif decision == "old":
            # Move current file to dups
            if not self.dry_run:
                self._safe_move(path, self.dup_folder)

            # Mark processed but don't add to main index
            self.cur.execute(
                "INSERT OR REPLACE INTO files (path, processed, fingerprint) VALUES (?, 1, ?)",
                (path, fingerprint),
            )
            self.conn.commit()
            return False

        return False  # Skip

    def _handle_id_deduplication(self, path, acoustid_id, quality):
        """
        Handles strict deduplication based on AcoustID (exact song match).
        Returns True if we should proceed (new file is unique or an upgrade).
        Returns False if we should discard (new file is a duplicate/downgrade).
        """
        # Find any existing processed file with this AcoustID
        self.cur.execute(
            "SELECT path, quality_score FROM files WHERE acoustid_id = ? AND processed = 1",
            (acoustid_id,),
        )
        existing = self.cur.fetchone()

        if not existing:
            return True  # No conflict, proceed

        existing_path, existing_score = existing

        # Logic: Compare Quality Score
        if quality["score"] > existing_score:
            logging.info(f"ID-Upgrade: {existing_path} -> {path}")
            print(
                f" -> Upgrading existing file (Quality: {existing_score} -> {quality['score']})"
            )

            # Remove the OLD file from DB and Disk (move to dups)
            try:
                if not self.dry_run:
                    self._safe_move(existing_path, self.dup_folder)

                # Clean up DB
                self.cur.execute("DELETE FROM files WHERE path = ?", (existing_path,))
                self.cur.execute(
                    "DELETE FROM fingerprint_index WHERE path = ?", (existing_path,)
                )
                self.conn.commit()
                return True
            except Exception as e:
                logging.error(f"Error removing old ID duplicate {existing_path}: {e}")
                return True

        else:
            logging.info(f"ID-Duplicate (Worse): {path} < {existing_path}")
            print(f" -> Duplicate found (lower/equal quality). Moving to duplicates.")

            # Move the NEW file to dups
            if not self.dry_run:
                self._safe_move(path, self.dup_folder)

            # Mark processed in DB so we don't scan it again, but don't index it
            self.cur.execute(
                "INSERT OR REPLACE INTO files (path, processed, acoustid_id) VALUES (?, 1, ?)",
                (path, acoustid_id),
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

    def _organize_file(self, current_path, artist_dir, album_dir, filename):
        target_dir = os.path.join(self.destination_folder, artist_dir, album_dir)
        return self._safe_move(current_path, target_dir, filename)

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

            try:
                duration, fingerprint = acoustid.fingerprint_file(path)
                quality = self._calculate_quality(path)
                if not quality:
                    continue

                # --- STEP 1: Local Fuzzy Deduplication (Fingerprint) ---
                if not self._handle_local_deduplication(path, fingerprint, quality):
                    continue
                # -------------------------------------------------------

                resp = acoustid.lookup(
                    self.api_key,
                    fingerprint,
                    duration,
                    meta="recordings releases tracks",
                )

                if resp.get("status") != "ok" or not resp.get("results"):
                    logging.warning(f"No match for {path}")
                    continue

                candidates = self._get_candidates(resp["results"])
                if not candidates:
                    continue

                current_acoustid_id = resp["results"][0]["id"]

                # --- STEP 2: Strict ID Deduplication (AcoustID) ---
                if not self._handle_id_deduplication(
                    path, current_acoustid_id, quality
                ):
                    continue
                # --------------------------------------------------

                selected_match = None
                top_match = candidates[0]

                if top_match["similarity"] >= 0.98:
                    selected_match = top_match
                else:
                    selected_match = self._prompt_user_selection(path, candidates)

                if not selected_match:
                    logging.info(f"Skipped by user: {path}")
                    continue

                rel = selected_match["release"]
                rec = selected_match["recording"]
                target_recording_id = rec.get("id")

                artist = (
                    rel.get("artists", [{}])[0].get("name")
                    or rec.get("artists", [{}])[0].get("name")
                    or "Unknown"
                )

                track_num = 1
                disc_num = 1
                found_track = False
                for medium in rel.get("mediums", []):
                    for track in medium.get("tracks", []):
                        track_rec_id = track.get("recording", {}).get("id")
                        if track_rec_id == target_recording_id:
                            track_num = track.get("position", 1)
                            disc_num = medium.get("position", 1)
                            found_track = True
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

                self._apply_tags(path, meta)

                safe_artist = self._sanitize_name(meta["album_artist"])
                safe_album = self._sanitize_name(meta["album"])
                raw_filename = f"{str(meta['track_no']).zfill(2)} - {meta['title']}{quality['format']}"
                safe_filename = self._sanitize_name(raw_filename)

                final_path = self._organize_file(
                    path, safe_artist, safe_album, safe_filename
                )

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
                    "INSERT OR REPLACE INTO files VALUES (?,?,?,?,?,?,?,?,?,?,?)",
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

                # --- NEW: Update Fingerprint Index ---
                self._update_index(final_path, fingerprint)
                # -------------------------------------

                self.conn.commit()
                print(
                    f" -> Success: {os.path.join(safe_artist, safe_album, safe_filename)}"
                )

            except Exception as e:
                logging.error(f"Critical Failure on {path}: {e}")
                print(f" -> Error: {e}")

    def __del__(self):
        self._stop_audio()
        if hasattr(self, "conn"):
            self.conn.close()


if __name__ == "__main__":
    CONFIG = {
        "api_key": "7dlZplmc3N",
        "music_folder": "/mnt/ssk/music/",
        "destination_folder": "/mnt/ssk/NewMaster",
        "dup_folder": "/mnt/ssk/duplicates",
        "db_path": "library_manager.db",
        "dry_run": False,
    }

    manager = MusicLibraryManager(**CONFIG)
    manager.process_library()
    # manager.prune_database()