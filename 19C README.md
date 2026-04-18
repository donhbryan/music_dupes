Here is the completely rewritten `README.md` file based on the analysis of the corrected `A19b` script:

```markdown
# Music Library Manager
## 19C
An automated, high-performance music library organizer and deduplicator. This tool scans a raw music folder, identifies tracks using exact audio hashing and AcoustID audio fingerprinting, standardizes metadata via the MusicBrainz API, and safely organizes files into a clean `Artist / Album / Track - Title` directory structure.

## 🌟 Key Features

* **Intelligent Deduplication:** Uses 30-second audio snippet hashing (`ffmpeg`) to quickly identify exact audio matches without relying on brittle metadata or full-file processing.
* **Quality-Based Upgrades:** Automatically evaluates duplicates and retains the highest quality file. Strictly prioritizes metadata-friendly lossless formats (FLAC > ALAC > WAV > MP3 > WMA).
* **AcoustID Fingerprinting:** Uses Chromaprint to identify songs even if they have no tags or incorrect filenames.
* **Crash Resilience:** Gracefully handles corrupted files by moving them to an `unresolved` folder, allowing continuous processing.
* **Interactive Ambiguity Resolution:** Auto-selects based on prior library associations (sticky matching) or prompts the user with playback options (`ffplay`, `afplay`, `mpv`, `cvlc`) and pagination.
* **Automated Tagging:** Applies standardized ID3, Vorbis, and MP4 tags using the `mutagen` library.
* **High-Performance Database:** Uses an optimized SQLite3 database with Write-Ahead Logging (WAL) and memory preloading.

## 🌟 New Features in A19b

* **Concurrent Processing:** Leverages multithreading to process files concurrently, reducing API latency bottlenecks.
* **Memory Preloading:** Loads processed files and hashes into memory at startup for instant lookups.
* **Safe Dry Run:** Completely simulates the workflow without modifying physical files or altering the SQLite database.
* **Retroactive Audio Hashing:** Newly implemented `hashAudio` flag safely retrofits existing database entries with fast-match audio hashes.

## 📋 Prerequisites

* **Python 3.8+**
* **FFmpeg:** Required for audio snippet extraction.
* **Chromaprint / fpcalc:** Required for fingerprinting (`sudo apt install libchromaprint-tools` or `brew install chromaprint`).
* **CLI Audio Player:** `afplay`, `ffplay`, `mpv`, or `cvlc`.

## 🚀 Installation

```bash
git clone <repository_url>
cd MusicLibraryManager
pip install pyacoustid mutagen tqdm
```

## ⚙️ Configuration

On the first run, the script generates `library_management_config.json`:

```json
{
    "api_key": "YOUR_ACOUSTID_API_KEY",
    "music_folder": "/path/to/raw/music/",
    "destination_folder": "/path/to/organized/master/",
    "dup_folder": "/path/to/duplicates/",
    "unresolved_folder": "/path/to/unresolved/",
    "db_path": "library_manager.db",
    "dry_run": false,
    "prune": false,
    "hashAudio": false,
    "global_dedup": false,
    "process": true
}
```

### Configuration Flags

* **`api_key`**: Your AcoustID API key.
* **`dry_run`**: Set to `true` to simulate the process. *Note: As of A19b, this is strictly read-only and will not write to your database.*
* **`prune`**: Set to `true` to clean the database of records for files deleted from your drive.
* **`hashAudio`**: Set to `true` to retroactively generate audio hashes for processed files missing them.
* **`global_dedup`**: 
    * `false` (Default): Deduplication only happens within the context of the same Album ID (keeps albums intact).
    * `true`: Enforces one copy of a song across the entire library (breaks compilation albums, saves space).
* **`process`**: Set to `true` to run the main workflow.

## 💻 Maintenance Workflows

1. **Standard Processing:** Set `"process": true`.
2. **Database Cleanup (Pruning):** Set `"prune": true` and `"process": false`.
3. **Retroactive Hashing:** Set `"hashAudio": true` and `"process": false`.

## 🧠 How the Deduplication Hierarchy Works

Quality score hierarchy (size and bit-depth are tie-breakers):
1. **FLAC** (Lossless + Robust Metadata)
2. **M4A / ALAC** (Lossless + Robust Metadata)
3. **WAV** (Lossless + Poor Metadata Support)
4. **MP3** (Lossy)
5. **WMA** (Lossy)

## 🛑 Bypassed Files (When the script ignores a file)
The script will leave files untouched in the source directory if:
1. `"dry_run": true` is active.
2. The file format is unsupported (e.g., `.ogg`, `.jpg`).
3. The database marks the file as already processed.
4. The file is 0 bytes or locked by the OS.
5. Audio headers are entirely unreadable by `mutagen`.
6. You manually select `0 (Skip)` or `Q (Quit)` during ambiguity resolution.
```