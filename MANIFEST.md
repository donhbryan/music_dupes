Here is the updated `MANIFEST.md` based on the current state of your `library_management.py` and `README.md` files. I have integrated the new features (like the API rate limiting, expanded database schema, and robust rebuild mechanisms) to ensure the manifest accurately reflects version 19E.

---

# Project Manifest: Music Library Manager

**Version:** 19E | **Date:** June 17, 2026

## A. Core Identity & Objectives

**Project:** Music Library Deduplication & Management Tool

**Objective:** An automated, high-performance tool to scan, identify, deduplicate, and organize a raw music folder into a standardized `Artist / Album / Track - Title` structure. Use audio quality metrics to determine the optimal file versions to keep.

**Core Mechanisms:** Uses 30-second `ffmpeg` audio snippet hashing for exact matches and AcoustID/Chromaprint fingerprinting for untagged files. Standardizes metadata via the MusicBrainz API.

**Target Environments:** The in-memory state cache architecture is designed to fully leverage the high memory capacity of the primary deployment machines (KAHLESS with 64GB RAM and BORG with 32GB RAM), while remaining fully compatible for execution on MBP (MacBook Pro 10,1 running MX 25.1 Linux XFCE with X11).

**Tech Stack:** Python 3.8+, SQLite, FFmpeg, Chromaprint (`fpcalc`), Mutagen, PyAcoustID.

---

## B. Architecture & Module Map

`library_management.py`: Orchestrates the workflow, CLI arguments, and manages the worker thread pool.

* **`main()` / Initialization:** Application entry point; orchestrates the multithreaded detection script, manages the thread pool, and sets global network timeouts to prevent API hangs.
* **Data State (In-Memory Cache):** At startup via `_preload_state()`, all essential database states (`processed_files`, `known_hashes`, `library_state`, `known_acoustids`) are preloaded into RAM. Worker threads perform dictionary checks to eliminate Phase 1 disk I/O bottlenecks.
* **Database Concurrency (Async Write-Behind Queue):** Worker threads do not interact with SQLite directly. A dedicated background `db_writer_thread` handles all `INSERT`, `UPDATE`, and `DELETE` commands drawn from a thread-safe `queue.Queue()`.
* **Authoritative Metadata Architecture:** The SQLite schema has been expanded (`mb_release_id`, `mb_artist_id`, `mb_recording_id`) to tightly bind files to their definitive MusicBrainz entities.
* **State Recovery (DB Rebuild Sync):** The `rebuild_database()` and `_rebuild_single_file_worker()` mechanisms reconstruct a lost or corrupted database by backwards-syncing from existing directory structures. Extracts MusicBrainz/AcoustID tags natively via Mutagen to preserve API limits, falling back to APIs only when necessary.
* **Audio Hashing Engine:** `hash_existing_audio()`, `_get_audio_hash()`, and `_get_fingerprint()` handle rapid identification.
* **Interactive Ambiguity Resolution:** `resolve_ambiguous_files()` and `_prompt_user_selection()` employ "Sticky Matching" for highly probable album matches. Falls back to a CLI audio player (`afplay`, `ffplay`, `mpv`, or `cvlc`) to prompt the user for manual selection.

**Database Schema Snapshot** 

BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS albums (release_id TEXT PRIMARY KEY, album_title TEXT, album_artist TEXT, release_date TEXT, country TEXT, mb_release_id TEXT, mb_artist_id TEXT);
CREATE TABLE IF NOT EXISTS ambiguous_files (path TEXT PRIMARY KEY, candidates_json TEXT, acoustid_id TEXT, fingerprint TEXT, quality_json TEXT, audio_hash TEXT);
CREATE TABLE IF NOT EXISTS audio_hashes (audio_hash TEXT PRIMARY KEY, path TEXT, FOREIGN KEY(path) REFERENCES files(path) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY, fingerprint TEXT, acoustid_id TEXT, title TEXT, track_no INTEGER, disc_no INTEGER, format TEXT, file_size INTEGER, quality_score REAL, album_id TEXT, processed INTEGER DEFAULT 0, date_modified DATETIME DEFAULT CURRENT_TIMESTAMP, mb_recording_id TEXT, mb_track_id TEXT, FOREIGN KEY (album_id) REFERENCES albums (release_id));
CREATE TABLE IF NOT EXISTS fingerprint_index (block TEXT, path TEXT, FOREIGN KEY(path) REFERENCES files(path) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE IF NOT EXISTS known_blocks (block TEXT, acoustid_id TEXT);
CREATE TABLE IF NOT EXISTS known_fingerprints (fingerprint TEXT, acoustid_id TEXT, PRIMARY KEY (fingerprint, acoustid_id));
CREATE INDEX idx_acoustid ON files(acoustid_id);
CREATE INDEX idx_audio_hashes_path ON audio_hashes(path);
CREATE INDEX idx_file_blocks ON fingerprint_index(block);
CREATE INDEX idx_files_dedup ON files(acoustid_id, album_id, processed);
CREATE INDEX idx_files_processed ON files(processed);
CREATE INDEX idx_known_blocks ON known_blocks(block);
CREATE TRIGGER update_files_modtime AFTER UPDATE ON files FOR EACH ROW BEGIN UPDATE files SET date_modified = CURRENT_TIMESTAMP WHERE path = old.path; END;
COMMIT;
---

## C. Immutable Constraints & Rules

* **Rule 1: Strict Deduplication Hierarchy:** When evaluating duplicates, the script must strictly prioritize lossless, metadata-friendly formats and gracefully quarantine inferior files. Hierarchy: FLAC > M4A/ALAC > WAV > MP3 > WMA.
* **Rule 2: Absolute Thread Safety:** Worker threads must never use `threading.RLock()` for SQLite. All database modification commands must be passed through the `db_queue` to the dedicated writer thread.
* **Rule 3: True Read-Only Dry Runs:** When `dry_run: true` is set, the script must explicitly bypass the `db_queue` for all file-modifying operations to prevent database corruption with dummy paths.
* **Rule 4: File Exclusion Adherence:** Files must be bypassed if they are unsupported (e.g., `.ogg`), locked by the OS, 0 bytes, unreadable by mutagen, or if the user manually selects "Skip/Quit" during ambiguity resolution.
* **Rule 5: API Rate Limit Compliance:** Concurrent API calls must be throttled using dedicated threading locks (`mb_api_lock` and `acoustid_api_lock`) and global socket timeouts to prevent IP bans and permanent thread hangs.
* **Rule 6:** Never modify the database schema without explicit permission.

* **Rule 7:** All new functions must include type hints and docstrings.

* **Rule 8:** Do not refactor existing working code unless explicitly requested. Provide only the additions or specific modifications.

---

## D. Current State & Next Steps (The Changelog)

**Working:** * **High-Performance Architecture:** Implemented RAM-speed reads via in-memory state cache and lock-free writes via the async write-behind queue.

* **Database Rebuild Synchronization:** Fully functional backwards-syncing capability to reconstruct the SQLite database from existing directory structures, pulling existing tags to minimize API calls.
* **Global & Granular Deduplication:** Support for both strict library-wide deduplication (`global_dedup: true`) and album-specific deduplication to keep compilations intact.
* **Configuration Management:** Controlled via `library_management_config.json`, seamlessly supporting maintenance workflows (`prune`, `hashAudio`, `global_dedup`, `rebuild_db`).
* **API Stability:** Thread-safe timing locks and standard socket timeouts prevent AcoustID and MusicBrainz from hanging worker threads.

**Currently Implementing:** * *[Update this when starting a new session - e.g., Further optimization of audio hashing speeds or advanced tag extraction]*

**Known Issues:** * *[Update this when starting a new session]*