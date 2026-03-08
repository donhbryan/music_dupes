# AcoustID Music Library Manager & Deduplicator

---
An intelligent, automated Python tool designed to sort, tag, organize, and deduplicate massive music libraries. Built for NAS environments and large messy collections, this script uses audio fingerprinting (AcoustID) and fast MD5 hashing to ensure your master library contains only the highest-quality version of every track, perfectly tagged and organized.

## ✨ Key Features

Audio Fingerprinting:
    * Uses AcoustID and MusicBrainz to identify tracks by their actual audio profile, ignoring incorrect or missing ID3 tags.

Smart Quality Deduplication: If duplicate tracks are found, the script calculates a "quality score" (based on lossless formats, bit depth, and file size) and strictly keeps the best version, moving the inferior file to a duplicates folder.

Ultra-Fast Exact Matches: Uses MD5 hashing to instantly detect exact file copies, bypassing slow API calls and audio decoding entirely.

Automated Organization: Renames and moves files into a clean Artist/Album/Track - Title.ext folder structure.

Universal Auto-Tagging: Automatically applies accurate metadata to MP3 (ID3), FLAC, M4A/MP4, WMA, and WAV files using mutagen.

"Sticky" Album Context: When resolving ambiguous tracks, the script remembers the last selected album to intelligently group multi-track imports automatically.

Interactive CLI Audio Player: If a track requires manual intervention, the script can play the audio file in the terminal using system audio players (ffplay, mpv, cvlc, or afplay) so you know exactly what you're sorting.

JSON Configuration: Easily manage paths and API keys without touching the core Python code.

Robust SQLite Tracking: Maintains a local database of processed files, known fingerprints, and file hashes to speed up future runs and handle interruptions gracefully.

### 🛠️ Prerequisites

Python Dependencies
You will need Python 3.8+ and the following libraries:

Bash
pip install pyacoustid mutagen tqdm
System Dependencies
For AcoustID fingerprinting to work, you must have the fpcalc utility installed on your system.

Debian/Ubuntu/Linux Mint: sudo apt install fpcalc

macOS: brew install chromaprint

For the terminal audio preview feature to work during ambiguous matches, ensure you have at least one of these CLI players installed: ffplay (part of ffmpeg), mpv, or vlc.

API Key
You will need a free AcoustID API key. Register an application at acoustid.org to get one.

### ⚙️ Configuration

On the first run, the script will automatically generate a config.json file in the same directory. Edit this file with your specific paths and API key:

```JSON
{
    "api_key": "YOUR_ACOUSTID_API_KEY",
    "music_folder": "/MUSIC_SOURCE_FOLDER/",
    "destination_folder": "/TARGET_FOLDER_WITH_BEST_QUALITY/",
    "dup_folder": "/FOLDER_FOR_DUPLICATES/",
    "unresolved_folder": "/FOLDER_FOR_UNPROCESSED_FILES/",
    "db_path": "library_manager.db",
    "dry_run": "false"
}
```

music_folder: Where your unsorted, messy music is located.

destination_folder: Where your organized, tagged master library will be built.

dup_folder: Where lower-quality duplicates and exact copies are sent.

unresolved_folder: Where files with no AcoustID match are safely moved.

## 🚀 Usage

The script is executed via the command line and includes several flags for precise control over your library management.

Basic Processing
To run the script using the default settings in your config.json:

```
python library_manager.py
```

(You can also explicitly pass --process to achieve the same result).

Safe Testing (Dry Run)
Want to see what the script would do without actually moving files or updating the database?

```Bash
python library_manager.py --dry-run
```

Using a Custom Config
Useful if you want to test on a small subset of files or manage multiple distinct libraries:

```Bash
python library_manager.py -c test_config.json
```

#### 🧽 Database Maintenance Options

Because the script uses a local SQLite database to track processed files and hashes, you may occasionally need to perform maintenance—especially if you delete or manually move files in your destination_folder outside of the script.

1. Prune Ghost Entries
If you manually delete files from your NAS, the database will still think they exist. Use --prune to check all database entries against your actual filesystem and remove dead links:

```Bash
python library_manager.py --prune
```

2. Retroactive Hashing (Prepopulate)
If you update the script to a new version supporting MD5 hashing, but already have thousands of files processed in your database, use --prepopulate. This will scan your existing master library and generate MD5 hashes for ultra-fast future duplicate detection:

```Bash
python library_manager.py --prepopulate
```

3. The Full Maintenance Pipeline
You can chain commands together. To clean up dead database links, update all file hashes, and then process any new incoming music in one go:

```Bash
python library_manager.py --prune --prepopulate --process
```

### 🧠 How Deduplication Works

The Fast Pass (Exact Match): The script reads the incoming file and generates an MD5 hash. If that hash already exists in the database, the file is instantly moved to the dup_folder. No API calls, no audio decoding.

The Smart Pass (Audio Quality Match): If the file is technically distinct (different bitrates, different encodings), it generates an AcoustID fingerprint. If the API confirms the song belongs to an Album you already have, the script calculates a quality score.

The Outcome: The script strictly enforces quality. If the new file is better (e.g., FLAC replacing a 128kbps MP3), it upgrades your master library and sends the old file to the duplicates folder. If the quality of the new file **is not better**, it goes straight to the duplicates folder.
