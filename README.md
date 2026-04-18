# Music Library Manager

An automated, high-performance music library organizer and deduplicator. This tool scans a raw music folder, identifies tracks using exact audio hashing and AcoustID audio fingerprinting, standardizes metadata via the MusicBrainz API, and safely organizes files into a clean `Artist / Album / Track - Title` directory structure.

## 🌟 Key Features

* **Intelligent Deduplication:** Uses 30-second audio snippet hashing (`ffmpeg`) to quickly identify exact audio matches without relying on brittle metadata or full-file processing.
* **Quality-Based Upgrades:** Automatically evaluates duplicates and retains the highest quality file. Strictly prioritizes metadata-friendly lossless formats (FLAC > ALAC > WAV > MP3 > WMA) and gracefully moves inferior duplicates to a designated folder.
* **AcoustID Fingerprinting:** Uses Chromaprint to identify songs even if they have no tags or incorrect filenames.
* **Interactive Ambiguity Resolution:** If the API returns multiple highly probable album matches, the script attempts to auto-select based on prior library associations (Sticky Matching). If still ambiguous, it plays the audio clip and prompts the user to select the correct album.
* **Automated Tagging:** Applies standardized ID3, Vorbis, and MP4 tags (Title, Artist, Album, Album Artist, Track No, Disc No) using the `mutagen` library.

---

## ⚡ What's New: The High-Performance Architecture
*This section highlights the major architectural upgrades from the previous version of the script to the current, optimized version.*

### 1. In-Memory State Cache (RAM-Speed Reads)
* **Previous Version:** Multithreaded workers constantly queried the SQLite database (disk I/O) to check if a file was processed, owned, or a duplicate.
* **New Version:** At startup, the script preloads all essential database states (`processed_files`, `known_hashes`, `library_state`) directly into RAM. Worker threads now make split-second dictionary checks, entirely eliminating disk read bottlenecks during Phase 1 processing.

### 2. Asynchronous Write-Behind Queue (Lock-Free Writes)
* **Previous Version:** 8 worker threads fought over a single SQLite `threading.RLock()`. Because SQLite only permits one concurrent writer, threads were forced to wait sequentially, neutralizing the benefits of parallel processing.
* **New Version:** A dedicated background `db_writer_thread` handles all database `INSERT`, `UPDATE`, and `DELETE` commands. Worker threads simply drop their database commands into a thread-safe `queue.Queue()` and instantly move on to the next file.

### 3. Deferred Audio Hashing (CPU Optimization)
* **Previous Version:** The script calculated a CPU-heavy 30-second `ffmpeg` audio hash for *every* file immediately before checking the AcoustID API.
* **New Version:** The script now defers the heavy `ffmpeg` processing until *after* the AcoustID API confirms an ambiguity exists and a strict hash-check is required, drastically reducing CPU load on large libraries.

### 4. Fully Read-Only Dry Runs
* **Previous Version:** The `dry_run` flag prevented physical file moves but still inadvertently wrote dummy paths to the database, causing corruption.
* **New Version:** The script explicitly bypasses the `db_queue` for all file-modifying operations when `dry_run` is active, keeping the database perfectly intact.

---

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

### Configuration Flags Explained

* **`api_key`**: Your AcoustID API key.
* **`dry_run`**: Set to `true` to simulate the process. Completely read-only.
* **`prune`**: Set to `true` to clean the database of ghost records for files that have been deleted from your physical drive.
* **`hashAudio`**: Set to `true` to retroactively generate audio hashes for processed files that are currently missing them.
* **`global_dedup`**: 
    * `false` *(Default)*: Deduplication only happens within the same Album ID. Keeps full albums intact.
    * `true`: Enforces strict library-wide deduplication. Breaks compilation albums but saves maximum disk space.
* **`process`**: Set to `true` to run the main library organization workflow.

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

## 🛑 Bypassed Files (Exclusions)
The script leaves files untouched in the source directory if:
1. `"dry_run": true` is active.
2. The file format is unsupported (e.g., `.ogg`, `.jpg`).
3. The database (in-memory cache) marks the file as already processed.
4. The file is 0 bytes or locked by the OS.
5. Audio headers are entirely unreadable by `mutagen`.
6. You manually select `0 (Skip)` or `Q (Quit)` during ambiguity resolution.
