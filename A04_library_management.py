import os
import sys
import time
import subprocess
import sqlite3
import logging
import shutil
import acoustid
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
        self.conn.commit()

    def _calculate_quality(self, file_path):
        """Generates a quality score based on format, bit depth, size."""
        try:
            time.sleep(0.5)  # Pause to let file system settle
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
        """
        Parses API results and returns a list of all potential album matches.
        """
        candidates = []
        seen_releases = set()  # To avoid showing the exact same album ID twice

        for result in results:
            match_score = result.get("score", 0)
            for recording in result.get("recordings", []):
                rec_title = recording.get("title", "Unknown")

                for release in recording.get("releases", []):
                    rel_id = release.get("id")
                    if rel_id in seen_releases:
                        continue
                    seen_releases.add(rel_id)

                    # Gather metadata for display/selection
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

        # Sort candidates: High similarity > US Country > Date
        candidates.sort(
            key=lambda x: (x["similarity"], x["country"] == "US", x["date"]),
            reverse=True,
        )
        return candidates

    def _play_audio(self, file_path):
        """Attempts to play audio using available system CLI players."""
        self._stop_audio()  # Ensure no previous track is playing

        # Priority list of players and their silent/CLI flags
        # ffplay (ffmpeg), mpv, cvlc (vlc), afplay (macOS)
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
                    # Start process and detach (don't block)
                    self.player_process = subprocess.Popen(
                        cmd_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                    )
                    return  # Player started successfully
                except Exception as e:
                    logging.warning(f"Failed to start player {cmd_args[0]}: {e}")
                    continue

        print("(!) No supported audio player found (tried ffplay, mpv, vlc, afplay).")

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

    def _prompt_user_selection(self, file_path, candidates):
        """Interactive terminal menu for ambiguous matches with playback."""
        filename = os.path.basename(file_path)
        print(f"\n[!] Ambiguous Match for file: {filename}")
        print(f"    (Playing audio preview...)")
        print(f"{'#':<3} {'Sim':<5} {'Ctry':<4} {'Date':<6} {'Artist':<20} {'Album'}")
        print("-" * 80)

        # Limit to top 10 to keep UI clean
        top_candidates = candidates[:10]

        for idx, c in enumerate(top_candidates):
            sim_pct = f"{int(c['similarity'] * 100)}%"
            print(
                f"{idx+1:<3} {sim_pct:<5} {c['country']:<4} {c['date']:<6} {c['artist'][:19]:<20} {c['album_title']}"
            )

        print(f"{0:<3} SKIP THIS FILE")
        print("-" * 80)

        # Start playback
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
            # Always stop audio when leaving this menu (success or error)
            self._stop_audio()

    def _handle_deduplication(self, path, fingerprint, quality):
        """
        Returns True if current file is the winner.
        Updates DB to remove 'ghost' entries when upgrading quality.
        """
        self.cur.execute(
            "SELECT path, quality_score FROM files WHERE fingerprint = ? AND processed = 1",
            (fingerprint,),
        )
        existing = self.cur.fetchone()

        if existing:
            existing_path, existing_score = existing

            # Scenario A: The NEW file is better (Upgrade)
            if quality["score"] > existing_score:
                logging.info(f"Upgrading quality: {existing_path} -> {path}")
                try:
                    # 1. Move the old physical file to dups
                    if not self.dry_run and os.path.exists(existing_path):
                        shutil.move(
                            existing_path,
                            os.path.join(
                                self.dup_folder, os.path.basename(existing_path)
                            ),
                        )

                    # 2. REMOVE the old DB entry to prevent 'ghost' data
                    self.cur.execute(
                        "DELETE FROM files WHERE path = ?", (existing_path,)
                    )
                    self.conn.commit()

                    return True  # Proceed to process the new file
                except Exception as e:
                    logging.error(f"Error moving old duplicate {existing_path}: {e}")
                    return True  # Proceed anyway, priority is the new file

            # Scenario B: The NEW file is worse (Duplicate)
            else:
                logging.info(f"Duplicate found (lower quality): {path}")
                if not self.dry_run:
                    shutil.move(
                        path, os.path.join(self.dup_folder, os.path.basename(path))
                    )

                # Mark this specific path as processed (and discarded)
                self.cur.execute(
                    "INSERT OR REPLACE INTO files (path, processed) VALUES (?, 1)",
                    (path,),
                )
                self.conn.commit()
                return False

        return True

    def _apply_tags(self, file_path, meta):
        if self.dry_run:
            return
        try:
            time.sleep(0.5)  # Pause to ensure file is ready for writing
            audio = mutagen.File(file_path)
            if not audio:
                return
            ext = file_path.lower()

            # Map simplified meta to format-specific tags
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
        """Removes special characters from file and directory names."""
        if not name:
            return "Unknown"
        # Replace common separators with hyphens first
        cleaned = name.replace("/", "-").replace("\\", "-")
        # Keep only alphanumeric and standard safe delimiters
        cleaned = "".join(c for c in cleaned if c.isalnum() or c in " -_.")
        return cleaned.strip()

    def _organize_file(self, current_path, artist_dir, album_dir, filename):
        """Moves file to Destination/Artist/Album/Filename."""
        # Define target directory
        target_dir = os.path.join(self.destination_folder, artist_dir, album_dir)

        if not self.dry_run and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        target_path = os.path.join(target_dir, filename)

        # Handle file collisions (if file exists, append counter)
        if os.path.exists(target_path):
            if os.path.abspath(current_path) == os.path.abspath(target_path):
                return target_path

            base, ext = os.path.splitext(filename)
            counter = 1
            while os.path.exists(target_path):
                target_path = os.path.join(target_dir, f"{base} ({counter}){ext}")
                counter += 1

        if not self.dry_run:
            try:
                shutil.move(current_path, target_path)
            except OSError as e:
                logging.error(f"Failed to move {current_path} -> {target_path}: {e}")
                return current_path

        return target_path

    def process_library(self):
        files = [
            os.path.join(r, f)
            for r, _, fs in os.walk(self.music_folder)
            for f in fs
            if f.lower().endswith((".mp3", ".flac", ".m4a", ".mp4", ".wma", ".wav"))
        ]

        print(f"Found {len(files)} supported files. Starting process...")

        # Use manual loop instead of tqdm for cleaner input prompts
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

                if not self._handle_deduplication(path, fingerprint, quality):
                    continue

                resp = acoustid.lookup(
                    self.api_key, fingerprint, duration, meta="recordings releases"
                )

                if resp.get("status") != "ok" or not resp.get("results"):
                    logging.warning(f"No match for {path}")
                    continue

                # 1. Get all potential candidates
                candidates = self._get_candidates(resp["results"])
                if not candidates:
                    continue

                selected_match = None
                top_match = candidates[0]

                # 2. Similarity Check Algorithm
                if top_match["similarity"] >= 0.98:
                    # High Confidence: Auto-select the top result
                    selected_match = top_match
                else:
                    # Low Confidence: Prompt User and PLAY AUDIO
                    # Note: We now pass the full 'path', not just basename
                    selected_match = self._prompt_user_selection(path, candidates)

                if not selected_match:
                    logging.info(f"Skipped by user: {path}")
                    continue

                # 3. Extract Meta from Selection
                rel = selected_match["release"]
                rec = selected_match["recording"]

                artist = (
                    rel.get("artists", [{}])[0].get("name")
                    or rec.get("artists", [{}])[0].get("name")
                    or "Unknown"
                )

                meta = {
                    "title": rec.get("title", "Unknown"),
                    "album": rel.get("title", "Unknown Album"),
                    "artist": artist,
                    "album_artist": rel.get("artists", [{}])[0].get("name") or artist,
                    "track_no": rel.get("mediums", [{}])[0]
                    .get("tracks", [{}])[0]
                    .get("position", 1),
                    "disc_no": rel.get("mediums", [{}])[0].get("position", 1),
                    "release_date": str(rel.get("date", {}).get("year", "0000")),
                    "release_id": rel.get("id"),
                }

                # 4. Apply Changes
                self._apply_tags(path, meta)

                # Prepare Clean Filenames & Directories
                safe_artist = self._sanitize_name(meta["album_artist"])
                safe_album = self._sanitize_name(meta["album"])

                raw_filename = f"{str(meta['track_no']).zfill(2)} - {meta['title']}{quality['format']}"
                safe_filename = self._sanitize_name(raw_filename)

                # Move to: MusicFolder / Artist / Album / File
                final_path = self._organize_file(
                    path, safe_artist, safe_album, safe_filename
                )

                # 5. Update DB
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
                    "INSERT OR REPLACE INTO files VALUES (?,?,?,?,?,?,?,?,?,1)",
                    (
                        final_path,
                        fingerprint,
                        meta["title"],
                        meta["track_no"],
                        meta["disc_no"],
                        quality["format"],
                        quality["size"],
                        quality["score"],
                        meta["release_id"],
                    ),
                )
                self.conn.commit()
                print(
                    f" -> Success: {os.path.join(safe_artist, safe_album, safe_filename)}"
                )

            except Exception as e:
                logging.error(f"Critical Failure on {path}: {e}")
                print(f" -> Error: {e}")

    def __del__(self):
        # Ensure cleanup if script exits abruptly
        self._stop_audio()
        if hasattr(self, "conn"):
            self.conn.close()


if __name__ == "__main__":
    CONFIG = {
        "api_key": "7dlZplmc3N",
        "music_folder": "/mnt/ssk/music/",
        "destination_folder": "/mnt/ssk/NewMaster",
        "dup_folder": "/mnt/ssk/music/duplicates",
        "db_path": "library_manager.db",
        "dry_run": False,
    }

    manager = MusicLibraryManager(**CONFIG)
    manager.process_library()
