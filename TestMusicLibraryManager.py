import unittest
import sqlite3
import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from dataclasses import dataclass

# Import the class and Config from your script
# (Assuming your script is named 'f.py')
from music_manager import MusicLibraryManager, Config


class TestMusicLibraryManager(unittest.TestCase):

    def setUp(self):
        """Set up an in-memory database and a test configuration."""
        self.config = Config(
            DB_PATH=":memory:",  # Use RAM, not disk
            MUSIC_FOLDER=Path("./test_music"),
            DUP_FOLDER=Path("./test_dups"),
            DRY_RUN=False,
            LOG_FILE="/dev/null",  # Suppress logs during tests
            SIMILARITY_AUTO=0.98,
            SIMILARITY_ASK=0.95,
        )
        self.manager = MusicLibraryManager(self.config)

    def tearDown(self):
        """Close DB connection."""
        # SQLite :memory: closes automatically, but good practice if using file
        pass

    # --- Helper to Generate Sample Data ---
    def create_mock_stats(self, fmt="mp3", bitrate=320000, score=None):
        """Generates sample audio statistics dictionary."""
        # Auto-calculate score if not provided, based on logic in main script
        if score is None:
            is_lossless = fmt in ["flac", "wav"]
            base = 20_000_000 if is_lossless else 10_000_000
            score = base + bitrate

        return {
            "score": score,
            "format": fmt,
            "bitrate": bitrate,
            "sample_rate": 44100,
            "bits_per_sample": 16,
            "size": 1024 * 1024 * 5,  # 5 MB
            "mtime": 123456.0,
        }

    # --- TESTS ---

    def test_audio_scoring_logic(self):
        """Test if get_audio_stats correctly calculates higher scores for better formats."""
        # Mock mutagen.File to return specific attributes
        with patch("mutagen.File") as mock_file:
            # Case 1: MP3 320kbps
            mock_obj_mp3 = MagicMock()
            mock_obj_mp3.info.bitrate = 320000
            mock_obj_mp3.info.sample_rate = 44100
            mock_obj_mp3.info.bits_per_sample = 16

            # Case 2: FLAC (Lossless)
            mock_obj_flac = MagicMock()
            mock_obj_flac.info.bitrate = (
                900000  # Bitrate matters less for FLAC but still exists
            )
            mock_obj_flac.info.sample_rate = 44100
            mock_obj_flac.info.bits_per_sample = 16

            # Apply mocks
            mock_file.side_effect = [mock_obj_mp3, mock_obj_flac]

            # Use dummy paths (pathlib objects)
            path_mp3 = Path("song.mp3")
            path_flac = Path("song.flac")

            # We need to mock os.path.getsize or stat().st_size
            with patch.object(Path, "stat") as mock_stat:
                mock_stat.return_value.st_size = 5000000
                mock_stat.return_value.st_mtime = 1000.0

                stats_mp3 = self.manager.get_audio_stats(path_mp3)
                stats_flac = self.manager.get_audio_stats(path_flac)

            # FLAC should strictly beat MP3 based on your formula
            self.assertTrue(
                stats_flac["score"] > stats_mp3["score"],
                f"FLAC score {stats_flac['score']} should be > MP3 {stats_mp3['score']}",
            )

    def test_indexing_and_fuzzy_match(self):
        """Test if the block-based index finds a match."""
        # 1. Insert a "fake" file into the DB manually
        existing_fp = "A" * 100  # Fingerprint is 100 'A's
        existing_path = "/music/song_a.mp3"

        # Populate DB with this file
        with sqlite3.connect(self.manager.config.DB_PATH) as conn:
            # Insert File Record
            conn.execute(
                "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)",
                (existing_path, existing_fp, 1000, "mp3", 128000, 44100, 16, 5000, 0),
            )
            # Insert Blocks (Index)
            blocks = self.manager._get_blocks(existing_fp)
            conn.executemany(
                "INSERT INTO fingerprint_index VALUES (?, ?)",
                [(b, existing_path) for b in blocks],
            )

        # 2. Query with a "Similar" fingerprint (99 'A's and 1 'B')
        # This simulates a slight difference in audio decoding
        new_fp = "A" * 99 + "B"

        with sqlite3.connect(self.manager.config.DB_PATH) as conn:
            match_path, match_stats, sim_ratio = self.manager.find_match(conn, new_fp)

        # Should find the match because they share almost all blocks
        self.assertEqual(match_path, existing_path)
        self.assertGreater(sim_ratio, 0.98)

    @patch("builtins.input", return_value="1")  # Simulate User pressing '1'
    def test_conflict_resolution_user_picks_new(self, mock_input):
        """Test manual conflict resolution when similarity is in the 'ASK' zone."""
        new_path = Path("new_song.flac")
        old_path = "old_song.mp3"

        new_stats = self.create_mock_stats("flac", 0)
        old_stats = self.create_mock_stats("mp3", 320000)

        # Similarity 0.96 (Between 0.95 and 0.98)
        decision = self.manager.resolve_conflict(
            new_path, new_stats, old_path, old_stats, 0.96
        )

        self.assertEqual(decision, "1")  # Logic should return '1' based on mock_input

    @patch("music_manager.shutil.move")
    @patch("music_manager.acoustid.fingerprint_file")
    @patch("music_manager.MusicLibraryManager.get_audio_stats")
    def test_process_library_replacement(self, mock_stats, mock_fp, mock_move):
        """
        Integration Test: A new FLAC replaces an old MP3.
        """
        # SETUP: DB has an old MP3
        old_fp = "C" * 100
        old_path = str(self.config.MUSIC_FOLDER / "song.mp3")
        with sqlite3.connect(self.config.DB_PATH) as conn:
            conn.execute(
                "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)",
                (old_path, old_fp, 100, "mp3", 128, 44100, 16, 100, 0),
            )
            # Index it
            conn.executemany(
                "INSERT INTO fingerprint_index VALUES (?, ?)",
                [(b, old_path) for b in self.manager._get_blocks(old_fp)],
            )

        # ACTION: Process a new FLAC file
        new_file_path = self.config.MUSIC_FOLDER / "song.flac"

        # Mocks
        mock_fp.return_value = (200.0, "C" * 100)  # Same fingerprint (Exact Match)
        mock_stats.return_value = self.create_mock_stats(
            "flac", score=2000
        )  # Higher score than 100

        # We need to trick os.walk to "find" our new file
        with patch("os.walk") as mock_walk:
            mock_walk.return_value = [
                (str(self.config.MUSIC_FOLDER), [], ["song.flac"])
            ]
            # Trick Path.exists and stat to work
            with patch.object(Path, "exists", return_value=True):
                with patch.object(Path, "stat") as mock_stat:
                    mock_stat.return_value.st_mtime = 999999  # Newer time

                    self.manager.process_library()

        # ASSERTION:
        # 1. Old MP3 should have been moved to dups
        # Check if shutil.move was called with old_path
        moved_paths = [args[0][0] for args in mock_move.call_args_list]
        self.assertIn(old_path, moved_paths)

        # 2. DB should now list the FLAC as the valid file (is_duplicate=0)
        with sqlite3.connect(self.config.DB_PATH) as conn:
            cursor = conn.execute(
                "SELECT path, is_duplicate FROM files WHERE path = ?",
                (str(new_file_path),),
            )
            row = cursor.fetchone()
            self.assertEqual(row[1], 0)  # New file is NOT duplicate


if __name__ == "__main__":
    unittest.main()
