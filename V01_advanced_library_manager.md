

# Advanced Library Manager

This script is a great start for managing a music library, but it currently has several overlapping functions, inconsistent error handling, and hardcoded logic that makes it difficult to maintain.

I have refactored the code to use Object-Oriented Programming (OOP), consolidated the duplicate quality checking logic, and improved the database management.

Key Improvements
Centralized Configuration: All settings are moved to a Config class.

Consolidated Logic: Merged get_file_stats and get_file_quality into a single, robust method.

Context Management: Used with statements for database connections to ensure they always close properly.

Decoupled Renaming: Separated "Sanitization" from "File System Operations."

Pathlib Usage: Used pathlib for modern, cross-platform path handling.

Logic Changes Explained
pathlib.Path Integration: Instead of manual os.path.join and os.path.splitext, I used Path objects. They are more readable and handle extensions/filenames much more cleanly (e.g., path.stem vs path.suffix).

State Management: By putting everything in the MusicLibraryManager class, the database connection and configuration are shared across methods without needing global variables or passing 10 arguments to every function.

Refined Quality Score: I kept your scoring logic but cleaned it up. I ensured that "bitrate" and "sample rate" are always integers to prevent SQLite insertion errors.

Collision Handling: The get_safe_path function is now a standalone utility that can be used whenever you move or rename a file, preventing accidental overwrites.

Sanitization: I used a more standard Regex for file sanitization to ensure compatibility across Windows, Mac, and Linux.

I have updated the script to include the MusicBrainz metadata lookup logic.

This version uses the acoustid lookup to find the most likely match, extracts the artist, album, and year, and writes them to the file using mutagen. I also added a MetadataMapping helper to keep the tag handling clean.

Refactored Code with Tagging Support

Improvements in this version:
Intelligent Tagging: It uses mutagen.File(..., easy=True) to handle common tags across different formats (MP3, FLAC, M4A) without needing specific ID3 code for each.

Automatic Match Picking: It takes the top result from the AcoustID lookup and maps the recording title, artist, album name, and release year.

API Rate Limit Protection: Added a configurable SLEEP_TIME (0.5s) to avoid being blocked by AcoustID during large library scans.

Integrated Workflow: The tagging only happens for "winning" files (unique files or higher-quality versions), so you don't waste API calls on duplicates that are being moved anyway.

Would you like me to add a folder organization feature that moves these files into Artist/Album/Track.mp3 subdirectories based on their new tags?


I've updated the script to include a robust Folder Organizer. It now moves files into a structured Artist/Album/Track.ext hierarchy. I've also ensured full support for .wav and .mp4 files, including metadata handling for these formats.

Key Additions
Hierarchical Organization: Automatically creates directories based on Artist and Album tags.

WAV/MP4 Support: Integrated mutagen.wave.WAVE and mutagen.mp4.MP4 for metadata writing.

Tag Fallbacks: If a tag (like Artist or Album) is missing from the API, it defaults to "Unknown Artist" or "Unknown Album" to prevent folder errors.



Key Workflow Changes
Tag-Based Routing: The file is no longer just "renamed"; it is moved into a directory structure like ./data/music/Daft Punk/Discovery/One More Time.mp3.

Safe File Handling: Because .wav files can be huge, the script checks if the file actually exists before attempting a shutil.move.

MP4 Specifics: Mutagen's easy=True mode handles the atom structures of .mp4 and .m4a files automatically, so you don't have to worry about different tagging standards between MP3 (ID3) and MP4 (iTunes style).

I have finalized the script to include WMA support and a Post-Processing Cleanup routine.

Supporting .wma (Windows Media Audio) is slightly different because it uses the ASF (Advanced Systems Format) metadata structure. I have integrated mutagen.asf to handle these specific tags while keeping the rest of the logic unified.

Refactored Music Manager (Final Version)

What's New?
Post-Process Cleanup: After all files are organized, cleanup_empty_folders scans the music directory from the "bottom up" and deletes folders that no longer contain files.

WMA (ASF) Support: Added specific handling for WMA files. Unlike MP3s, WMA uses keys like WM/AlbumTitle and Author. The script now detects the .wma extension and applies the correct mapping.

Comprehensive Formatting: * Lossless Support: .wav, .flac, .alac (via .m4a).

Lossy Support: .mp3, .mp4, .m4a, .wma.

I have updated the script to include sample_rate, bits_per_sample, and file_size in the SQLite database schema and insertion logic. This ensures your library report and duplicate comparison are as accurate as possible.

Final Refactored Script

Key Data Tracking Updates:
Database Schema: The files table now contains columns for sample_rate, bits_per_sample, and file_size.

Bitrate Accuracy: Bitrate is captured as an integer (bps) to ensure smooth database operations.

WMA/WAV/MP4 Awareness: The get_audio_stats method correctly extracts these parameters regardless of the file container.

Duplicate Storage: When a duplicate is moved, its technical stats (like size and sample rate) are recorded in the database, allowing you to see exactly how much space was "saved" in the final report.

I have added a new method, export_library_data, which generates a comprehensive CSV export of your entire music collection. This includes all the technical specs we just added to the database.

I have also ensured that the CSV export includes a "Status" column so you can easily filter between your Main Library and the Duplicates in Excel or Google Sheets.

Final Music Manager with Excel/CSV Export

Bitrate & Sample Rate: You can now sort your library in Excel to find "low-quality" files that need replacing.

File Size: Use this to see which albums are taking up the most space (useful for .wav and .flac collections).

Duplicate Tracking: The is_duplicate column (0 or 1) allows you to see exactly which files were moved to the duplicate folder and why.
