# Music Library Manager

This Python script is a sophisticated music library organizer and deduplicator. It scans a music directory, identifies songs based on their actual **audio content** (audio fingerprinting), automatically retains the highest-quality version of duplicate tracks, fetches correct metadata tags, and reorganizes the files into a clean folder structure.

---

## 1. Core Capabilities

* **Audio-Based Deduplication**
Unlike simple scripts that compare file names or sizes, this tool uses **AcoustID (Chromaprint)** to "listen" to the audio. It can identify that `track01.mp3` and `Queen - Bohemian Rhapsody.flac` are the same song.
* **Quality Scoring**
When duplicates are found, the script calculates a "Quality Score" for each file based on:
* Format (FLAC > MP3)
* Bit Depth
* Sample Rate
* Bitrate
It automatically preserves the higher-quality file and moves the lower-quality version to a `duplicates` folder.

* **Automatic Tagging**
It queries the **AcoustID API** to fetch the correct Title, Artist, Album, and Year for unidentified tracks and writes these tags to the file metadata (ID3, Vorbis, etc.).
* **File Organization**
It renames and moves files into a standardized structure: `Artist / Album / Title.ext`.
* **Incremental Sync**
It uses a local **SQLite database** to track processed files. If a file hasn't been modified since the last run, the script skips it to save processing time.

---

## 2. Logic Workflow (Step-by-Step)

### A. Initialization

The script sets up a SQLite database (`library_manager.db`) to store file paths, fingerprints, and processing status. It relies on a "blocking" strategy for indexing fingerprints to speed up search operations.

### B. Scanning & Fingerprinting

It walks through your `MUSIC_FOLDER`. For every new or modified file:

1. **Fingerprinting:** Generates a unique audio fingerprint using the `acoustid` library.
2. **Stats Extraction:** Extracts technical specs (Bitrate, Sample Rate, Format) using `mutagen`.

### C. The "Find Match" Process

Before assuming a file is unique, it searches the database:

1. **Block Search:** Looks for existing fingerprints that share similar data blocks (a speed optimization).
2. **Fuzzy Matching:** Uses `difflib.SequenceMatcher` to compare the new fingerprint against candidates.

* **Auto-Match (>98% similarity):** Automatically handled.
* **Uncertain Match (95-98%):** Pauses and prompts the user via CLI to choose which file to keep.

### D. Decision & Organization

* **If it's a unique song:**

1. Fetch tags from AcoustID.
2. Write tags to file.
3. Move file to `Music/Artist/Album/` folder.
4. Add record to Database.

* **If it's a duplicate:**

1. Compare the "Score" (calculated in `get_audio_stats`).
2. The **Winner** (higher quality) stays in the library.
3. The **Loser** is moved to `data/dups`.

---

## 3. Technical Highlights

### The "Score" Formula

The script attempts to objectively rate audio quality using the following logic inside `get_audio_stats`:

> **Note:** Lossless formats (FLAC/WAV) get a massive generic boost (2x multiplier) to ensure they always beat lossy formats like MP3.

### Fingerprint Indexing

Instead of comparing a new song against *every* song in the DB (which is slow), it breaks the fingerprint string into 16-character "blocks" and stores them in a separate table (`fingerprint_index`). It only performs detailed comparison against files that share a block.

---

## 4. Requirements

To run this script, you need the following:

**1. Python Libraries**

```bash
pip install acoustid mutagen tqdm

```

**2. System Binary**
You must have the **Chromaprint (`fpcalc`)** binary installed and accessible in your system `PATH`. This is required by the `acoustid` library to generate fingerprints.

**3. API Key**
The script uses a hardcoded AcoustID API key (`7dlZplmc3N`). If this key is invalid or rate-limited, the tagging feature will fail.

---

## 5. Configuration Summary

The `@dataclass class Config` at the top of the script controls behavior:

| Setting | Value | Description |
| --- | --- | --- |
| **`DRY_RUN`** | `False` | If `True`, simulates operations without moving files. |
| **`SIMILARITY_AUTO`** | `0.98` | Matches above 98% similarity are processed automatically. |
| **`SIMILARITY_ASK`** | `0.95` | Matches between 95% and 98% trigger a user prompt. |

---

## 6. Refactoring Improvements

The code has been refactored for better performance and maintainability. Key highlights include:

* **Modular Architecture:** Broken down into `DatabaseHandler`, `AudioProcessor`, and `LibraryManager` for separation of concerns.
* **Robustness:** Added a startup check to ensure the required `fpcalc` binary is present.
* **Performance:** The database connection is now persistent for the session rather than re-opening for every file.
* **Type Safety:** Improved type hinting and removed the need for repeated `type: ignore`.
* **Readability:** Logic for scoring, sanitization, and conflict resolution is now isolated.
