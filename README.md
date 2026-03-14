# Music Library Manager

An automated, high-performance music library organizer and deduplicator. This tool scans a raw music folder, identifies tracks using exact audio hashing and AcoustID audio fingerprinting, standardizes metadata via the MusicBrainz API, and safely organizes files into a clean `Artist / Album / Track - Title` directory structure.

## 🌟 Key Features

* **Intelligent Deduplication:** Uses 30-second audio snippet hashing (`ffmpeg`) to quickly identify exact audio matches without relying on brittle metadata or full-file processing.
* **Quality-Based Upgrades:** Automatically evaluates duplicates and retains the highest quality file. It strictly prioritizes metadata-friendly lossless formats (FLAC > ALAC > WAV > MP3 > WMA) and gracefully moves inferior duplicates to a designated folder.
* **AcoustID Fingerprinting:** Uses Chromaprint to identify songs even if they have no tags or incorrect filenames.
* **Crash Resilience:** Audio fingerprinting is isolated in a separate multiprocessing worker (`fork`/`spawn`). If a severely corrupted audio file causes the underlying C++ library to crash, the main script safely catches it, moves the file to an `unresolved` folder, and continues processing.
* **Interactive Ambiguity Resolution:** If the API returns multiple highly probable album matches, the script attempts to auto-select based on prior library associations. If still ambiguous, it plays the audio clip (via `ffplay`, `afplay`, `mpv`, or `cvlc`) and prompts the user to select the correct album.
* **Automated Tagging:** Applies standardized ID3, Vorbis, and MP4 tags (Title, Artist, Album, Album Artist, Track No, Disc No) using the `mutagen` library.
* **High-Performance Database:** Uses an optimized SQLite3 database with Write-Ahead Logging (WAL) to track known fingerprints, audio blocks, and file histories, drastically speeding up subsequent runs.

## 📋 Prerequisites

Ensure you have the following system dependencies installed before running the script:

* **Python 3.8+**
* **FFmpeg:** Required for high-speed audio snippet extraction.
  * *Ubuntu/Debian:* `sudo apt install ffmpeg`
  * *macOS:* `brew install ffmpeg`
  * *Windows:* Download from [ffmpeg.org](https://ffmpeg.org/download.html) and add to PATH.
* **Chromaprint / fpcalc:** Required by the `acoustid` library for fingerprinting.
  * *Ubuntu/Debian:* `sudo apt install libchromaprint-tools`
  * *macOS:* `brew install chromaprint`
* **CLI Audio Player (Optional but Recommended):** For interactive match resolution. The script looks for `afplay` (macOS default), `ffplay`, `mpv`, or `cvlc`.

## 🚀 Installation

Clone this repository or download the script to your local machine.

Next, install the required Python packages by running the following command in your terminal:

```bash
pip install pyacoustid mutagen tqdm
```

## ⚙️ Configuration

On the first run, the script will automatically generate a `library_management_config.json` file in the root directory. Edit this file to match your environment:

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

### Configuration Options

**api_key:** Your personal AcoustID API key (get one at acoustid.org).

**music_folder:** The source directory containing your unorganized audio files.

**destination_folder:** Where successfully tagged and organized files will be placed.

**dup_folder:** Where lower-quality exact duplicates are moved.

**unresolved_folder:** Where corrupt, unreadable, or completely unmatched files are moved.

**dry_run:** Set to true to simulate the process without actually moving or modifying any files.

**prune:** Set to true to clean the database of records for files that have been deleted from your drive.

**hashAudio:** Set to true to retroactively generate 30-second audio hashes for files already in your database. (Useful after a major script update).

**global_dedup:** Set to true to purge identical audio across your entire library. If false, deduplication only happens within the context of the same Album ID.

**process:** Set to true to run the main library organization workflow.

## 💻 Usage

Once your configuration file is set, simply run the script:

```Bash
python library_management.py
```

### Maintenance Workflows

1. Database Cleanup (Pruning):  
If you manually delete files from your library and want to remove their ghost entries from the database:

   1. Edit library_management_config.json:  
 set "prune": true and "process": false.

1. Retroactive Audio Hashing:
If you need to generate fast-match audio hashes for an existing database:

    Edit library_management_config.json: set "hashAudio": true and "process": false.

## Run the script

### 🧠 How the Deduplication Hierarchy Works

When the script evaluates two copies of the same song, it assigns a quality score based on the following format hierarchy, using file size and bit-depth only as tie-breakers:

**FLAC** (Lossless + Robust Metadata)

**M4A / ALAC** (Lossless + Robust Metadata)

**WAV** (Lossless + Poor Metadata Support)

**MP3** (Lossy)

**WMA** (Lossy)

If a new file has a higher score than the existing file in your database, the old file is evicted to the duplicates folder, and the new superior file takes its place in the master library.

# Music Library Manager

An automated, high-performance music library organizer and deduplicator. This tool scans a raw music folder, identifies tracks using exact audio hashing and AcoustID audio fingerprinting, standardizes metadata via the MusicBrainz API, and safely organizes files into a clean **Artist / Album / Track - Title** directory structure.

## 🌟 Key Features

* **Intelligent Deduplication:** Uses 30-second audio snippet hashing (`ffmpeg`) to quickly identify exact audio matches without relying on brittle metadata or full-file processing.
* **Quality-Based Upgrades:** Automatically evaluates duplicates and retains the highest quality file. It strictly prioritizes metadata-friendly lossless formats (FLAC > ALAC > WAV > MP3 > WMA) and gracefully moves inferior duplicates to a designated folder.
* **AcoustID Fingerprinting:** Uses Chromaprint to identify songs even if they have no tags or incorrect filenames.
* **Crash Resilience:** Audio fingerprinting is isolated in a separate multiprocessing worker (`fork`/`spawn`). If a severely corrupted audio file causes the underlying C++ library to crash, the main script safely catches it, moves the file to an `unresolved` folder, and continues processing.
* **Interactive Ambiguity Resolution:** If the API returns multiple highly probable album matches, the script attempts to auto-select based on prior library associations. If still ambiguous, it plays the audio clip (via `ffplay`, `afplay`, `mpv`, or `cvlc`) and prompts the user to select the correct album.
* **Automated Tagging:** Applies standardized ID3, Vorbis, and MP4 tags (Title, Artist, Album, Album Artist, Track No, Disc No) using the `mutagen` library.
* **High-Performance Database:** Uses an optimized SQLite3 database with Write-Ahead Logging (WAL) to track known fingerprints, audio blocks, and file histories, drastically speeding up subsequent runs.

## 📋 Prerequisites

Ensure you have the following system dependencies installed before running the script:

* **Python 3.8+**
* **FFmpeg:** Required for high-speed audio snippet extraction.
  * *Ubuntu/Debian:* `sudo apt install ffmpeg`
  * *macOS:* `brew install ffmpeg`
  * *Windows:* Download from [ffmpeg.org](https://ffmpeg.org/download.html) and add to PATH.
* **Chromaprint / fpcalc:** Required by the `acoustid` library for fingerprinting.
  * *Ubuntu/Debian:* `sudo apt install libchromaprint-tools`
  * *macOS:* `brew install chromaprint`
* **CLI Audio Player (Optional but Recommended):** For interactive match resolution. The script looks for `afplay` (macOS default), `ffplay`, `mpv`, or `cvlc`.

## 🚀 Installation

Clone this repository or download the script to your local machine.

Next, install the required Python packages by running the following command in your terminal:

```bash
pip install pyacoustid mutagen tqdm
```

## ⚙️ Configuration

On the first run, the script will automatically generate a `library_management_config.json` file in the root directory. Edit this file to match your environment:

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

## Configuration Options

**api_key:** Your personal AcoustID API key (get one at acoustid.org).

**music_folder:** The source directory containing your unorganized audio files.

**destination_folder:** Where successfully tagged and organized files will be placed.

**dup_folder:** Where lower-quality exact duplicates are moved.

**unresolved_folder:** Where corrupt, unreadable, or completely unmatched files are moved.

**dry_run:** Set to true to simulate the process without actually moving or modifying any files.

**prune:** Set to true to clean the database of records for files that have been deleted from your drive.

**hashAudio:** Set to true to retroactively generate 30-second audio hashes for files already in your database. (Useful after a major script update).

**global_dedup:** Set to true to purge identical audio across your entire library. If false, deduplication only happens within the context of the same Album ID.

**process:** Set to true to run the main library organization workflow.

## 💻 Usage

Once your configuration file is set, simply run the script:

```Bash
python library_management.py
```

Maintenance Workflows

1. Database Cleanup (Pruning):
If you manually delete files from your library and want to remove their ghost entries from the database:

    Edit library_management_config.json: set "*prune*": true and "process": false. Run the script.

1. Retroactive Audio Hashing:
If you need to generate fast-match audio hashes for an existing database:

    Edit library_management_config.json: set "*hashAudio*": true and "process": false. Run the script.

# Library Management Workflow

In the music library management workflow, files are usually routed to the master directory, the duplicates folder, or the unresolved folder. However, there are several specific conditions where an audio file will be completely bypassed and left untouched in its original source directory:

1. "*Dry Run*" Mode is Active
If "dry_run": true is set in the configuration JSON, the script simulates the entire fingerprinting, database, and deduplication logic, but explicitly bypasses all physical file operations (shutil.move and shutil.copy2). No files are actually moved.

2. Unsupported File Extensions
The script's main os.walk loop filters strictly for supported formats. Any file that does not end with .mp3, .flac, .m4a, .mp4, .wma, or .wav (such as .ogg, .ape, .jpg cover art, or .txt logs) is completely ignored by the scanner.

3. The File is "Already Processed"
At the very beginning of the per-file loop, the script queries the database for the exact file path. If the database returns processed = 1 for that path, the script assumes the file was handled in a previous run and instantly skips it.

4. Zero-Byte or Inaccessible Files
Before any fingerprinting begins, the script checks the file size using os.path.getsize(path).

    * If the file is exactly 0 bytes (an empty shell file), it logs a warning and skips it.

    * If the operating system throws a permission error (e.g., the file is locked by another program or owned by a restricted user), it logs an inaccessible warning and skips it.

5. Mutagen Parsing Failure (Pre-API)
If the mutagen library completely fails to read the file's audio headers to generate the bit-depth/sample-rate quality score, the _calculate_quality function returns None. Because this happens before the AcoustID API lookup or the C++ crash handler, the script aborts the loop early and leaves the file in the source directory rather than moving it to unresolved.

6. Manual User Skip
When the AcoustID API returns ambiguous album matches, the script pauses and prompts the user for a selection in the CLI. If the user inputs 0 (Skip) or q (Quit), the script honors the skip and abandons the file, leaving it exactly where it is.

7. OS-Level Transfer Failures
If a file successfully makes it through all logic checks, but the final _safe_move operation fails (e.g., the destination drive runs out of storage space, or a read-only permission blocks the transfer), the exception is caught, logged, and the original file remains in the source directory.

# Global Deduplcation  

In your MusicLibraryManager script, global_dedup is a configuration setting that determines how strictly the script searches for and removes duplicate songs in your library.

It changes the boundary of the deduplication check in the **_handle_album_deduplication** function. Here is how the two modes work:

1. When "**global_dedup**": false (Per-Album Deduplication)
This is the default behavior. When set to false, the script will only look for duplicates of a song within the exact same album.

How it works: It queries the database using both the acoustid_id (the song's fingerprint) AND the album_id.

## Example

If you have the song "Under Pressure" saved under Queen's original studio album Hot Space, and you also have a copy of "Under Pressure" saved under the compilation album Greatest Hits III, the script will keep both files. It views them as two distinct entities because their Album IDs do not match.

### Best for

People who want to maintain complete, intact albums. If you play an album from start to finish, you want every track to be there.

1. When "**global_dedup**": true (Library-Wide Deduplication)
When set to true, the script completely ignores album boundaries. It enforces a strict rule: There can only be one copy of a song in the entire master library.

How it works: It queries the database using only the acoustid_id.

Example: Using the "Under Pressure" example above, the script will notice that you have two copies of the same song in your library. It will then compare their quality scores. If the Hot Space version is a 320kbps MP3 and the Greatest Hits III version is a Lossless FLAC, the script will keep the FLAC version, update its tags, and throw the MP3 into your duplicates folder.

### Best for

People who listen to music primarily via shuffled playlists or single tracks and want to save as much hard drive space as possible by ruthlessly purging identical audio.

### The Drawback of global_dedup

If you turn this feature on, you will save a lot of disk space, but you will "break" compilation and greatest hits albums. If you try to listen to a Greatest Hits album straight through, it will be missing several tracks because the script determined the highest-quality master versions of those songs actually belonged to their original studio albums, and it deleted the duplicate copies from the Greatest Hits folder.
