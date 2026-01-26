This version includes:

Multiple Album Support: Allows selecting multiple albums (comma-separated) and saves the song to all of them.

Album-Scoped Deduplication: Allows the same song (AcoustID) to exist in different albums, but enforces unique high-quality files within the same album.

Safety Fixes: Includes the NoneType fix for quality scores, empty directory cleanup, and graceful Quit logic.

Local Caching: Uses the fingerprint history to speed up matches for previously seen songs.

## Modification

I have updated library_manager.py to include a new_display_local_matches function.

This function queries your database for any existing entries with the same AcoustID and displays the Album Title and Artist before asking you to select a match from the API. This helps you decide if you are adding a duplicate from a different album or a new track.

I also commented out the global _handle_id_deduplication calls in the main loop to correctly support the "Multiple Albums" feature you requested previously (allowing the same song to exist in different albums).

## Bug Fix
I have updated library_manager.py to:

Fix the TypeError: Explicitly handle None values in _handle_album_deduplication by treating them as 0.0.

Improve Error Logging: Added traceback logging so if a "Critical Failure" happens again, you will see the full error details in the log file, not just an empty message.

Preserve Source File on Multi-Album Duplicate: Refined the logic to ensure the source file isn't prematurely moved to the "duplicates" folder if you selected multiple albums and it was a duplicate in the first one but might be needed for the second.

## Enhancement
Changes:

Replaced _display_local_matches with_get_owned_release_ids: This helper now returns the IDs instead of printing them, allowing programmatic sorting.

Updated process_library:

Queries for owned albums immediately after fetching candidates.

Marks matches as is_owned.

Re-sorts the candidate list so owned albums appear at the very top (prioritized over similarity score).

Updated _prompt_user_selection: Added an "Own" column to the display table. Owned albums are marked with a * for clear visibility.

## Bug Fix
Empty File Check: Added a check at the start of the processing loop to skip 0-byte files.

Specific Error Handling: Wrapped the acoustid.fingerprint_file call in a nested try...except block to catch EOFError, OSError, and FingerprintGenerationError specifically.
