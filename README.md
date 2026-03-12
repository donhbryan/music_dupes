
# AcoustID Music Library Manager & Deduplicator

---

# current version A16

An intelligent, automated Python tool designed to sort, tag, organize, and deduplicate massive music libraries. Built for NAS environments and large messy collections, this script uses audio fingerprinting (AcoustID) and fast MD5 hashing to ensure your master library contains only the highest-quality version of every track, perfectly tagged and organized.

## ✨ Key Features

* **Audio Fingerprinting:** Uses AcoustID and MusicBrainz to identify tracks by their actual audio profile, ignoring incorrect or missing ID3 tags.
* **Smart Quality Deduplication:** If duplicate tracks are found, the script calculates a "quality score" (based on lossless formats, bit depth, and file size) and strictly keeps the best version, moving the inferior file to a duplicates folder.
* **Ultra-Fast Exact Matches:** Uses MD5 hashing to instantly detect exact file copies, bypassing slow API calls and audio decoding entirely.
* **Prior Association Auto-Select:** If the database recognizes a file's audio fingerprint as belonging to an album you already have, it bypasses user prompts and automatically groups it with the existing album to evaluate for a quality upgrade.
* **"Sticky" Album Context:** When resolving ambiguous new tracks, the script remembers the last selected album to intelligently group multi-track imports automatically.
* **Automated Organization:** Renames and moves files into a clean `Artist/Album/Track - Title.ext` folder structure.
* **Universal Auto-Tagging:** Automatically applies accurate metadata to MP3 (ID3), FLAC, M4A/MP4, WMA, and WAV files using `mutagen`.
* **Interactive CLI Audio Player:** If a track requires manual intervention, the script can play the audio file in the terminal using system audio players (`ffplay`, `mpv`, `cvlc`, or `afplay`) so you know exactly what you're sorting.
* **Centralized JSON Configuration:** Easily manage all paths, API keys, and execution commands (like dry runs and database maintenance) from a single configuration file without touching the core Python code or passing complex CLI arguments. Includes safe parsing so `false`, `"false"`, or `"0"` are all handled correctly.
* **Robust SQLite Tracking:** Maintains a local database of processed files, known fingerprints, and file hashes to speed up future runs and handle interruptions gracefully.
* **Modification Tracking:** The database automatically generates and updates a `date_modified` timestamp for every file record using built-in SQLite triggers.

### 🛠️ Prerequisites

**Python Dependencies**
You will need Python 3.8+ and the following libraries:

```bash
pip install pyacoustid mutagen tqdm
```

**System Dependencies**
For AcoustID fingerprinting to work, you must have the `fpcalc` utility installed on your system.

* **Debian/Ubuntu/Linux Mint:** `sudo apt install fpcalc`
* **macOS:** `brew install chromaprint`

For the terminal audio preview feature to work during ambiguous matches, ensure you have at least one of these CLI players installed: `ffplay` (part of ffmpeg), `mpv`, or `vlc`.

**API Key**
You will need a free AcoustID API key. Register an application at [acoustid.org](https://acoustid.org/) to get one.

### ⚙️ Configuration

On the first run, the script will automatically generate a `library_management_config.json` file in the same directory. Edit this file with your specific paths, API key, and desired execution flags:

```json
{
    "api_key": "YOUR_ACOUSTID_API_KEY",
    "music_folder": "/MUSIC_SOURCE_FOLDER/",
    "destination_folder": "/TARGET_FOLDER_WITH_BEST_QUALITY/",
    "dup_folder": "/FOLDER_FOR_DUPLICATES/",
    "unresolved_folder": "/FOLDER_FOR_UNPROCESSED_FILES/",
    "db_path": "library_manager.db",
    "dry_run": false,
    "prune": false,
    "prepopulate": false,
    "process": true
}
```

* **music_folder:** Where your unsorted, messy music is located.
* **destination_folder:** Where your organized, tagged master library will be built.
* **dup_folder:** Where lower-quality duplicates and exact copies are sent.
* **unresolved_folder:** Where files with no AcoustID match are safely moved.

## 🚀 Usage & Execution Modes

Because the script manages large libraries, all operational commands are managed safely via the JSON configuration file. To run the script, simply execute it in your terminal:

```bash
python library_management.py
```

The script will look at the true/false flags at the bottom of your `library_management_config.json` file to determine what actions to take. You can toggle multiple flags on at the same time to chain operations.

#### Basic Processing

To process your incoming `music_folder` and organize it into your `destination_folder`, ensure your config is set to:

```json
    "process": true
```

#### Safe Testing (Dry Run)

Want to see what the script *would* do without actually moving files or updating the database? Set the dry run flag:

```json
    "dry_run": true
```

#### 🧽 Database Maintenance Options

Because the script uses a local SQLite database to track processed files and hashes, you may occasionally need to perform maintenance—especially if you delete or manually move files in your `destination_folder` outside of the script.

**1. Prune Ghost Entries**
If you manually delete files from your NAS, the database will still think they exist. To check all database entries against your actual filesystem and remove dead links, enable pruning:

```json
    "prune": true
```

**2. Retroactive Hashing (Prepopulate)**
If you update the script to a new version supporting MD5 hashing, but already have thousands of files processed in your database, enable prepopulation. This will scan your existing master library and generate MD5 hashes for ultra-fast future duplicate detection:

```json
    "prepopulate": true
```

**3. The Full Maintenance Pipeline**
You can chain commands together. To clean up dead database links, update all file hashes, and then process any new incoming music in one go, set your configuration like this:

```json
    "prune": true,
    "prepopulate": true,
    "process": true
```

### 🧠 How Deduplication Works

* **The Fast Pass (Exact Match):** The script reads the incoming file and generates an MD5 hash. If that hash already exists in the database, the file is instantly moved to the `dup_folder`. No API calls, no audio decoding.
* **The Smart Pass (Audio Quality Match):** If the file is technically distinct (different bitrates, different encodings), it generates an AcoustID fingerprint. The script first checks if this fingerprint is already associated with an album in your database. If it is, it auto-selects that album. If not, it uses sticky context or prompts you to select the correct album. Once an album is selected, it calculates a quality score.
* **The Outcome:** The script strictly enforces quality. If the new file is better (e.g., FLAC replacing a 128kbps MP3), it upgrades your master library and sends the old file to the duplicates folder. If the quality of the new file **is not better**, it goes straight to the duplicates folder.
