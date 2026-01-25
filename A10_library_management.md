# A10 Improve local matching

## can storing multiple fingerprints for an acoustid improve local matching of the same song in multiple formats?

Analyzing Local Matching

I'm currently focused on how the script manages fingerprints and AcoustIDs. I'm exploring the implications of storing multiple fingerprints per AcoustID to enhance local matching for songs in different formats. My primary goal is to assess if and how this strategy can improve the script's performance in matching audio files.

Evaluating Multi-Fingerprint Strategy

I'm now considering the benefits of storing multiple fingerprints per AcoustID. My initial focus is on improving local fuzzy matching. If I store both a FLAC and an MP3 fingerprint for the same song, a new MP3 will have a higher chance of matching locally without needing an API lookup, improving performance. This is promising, as it means faster local matching, but I need to consider storage implications and the complexity of the matching algorithm.

Refining Database Schema

I'm now zeroing in on the database schema. The current setup, with a files table having one fingerprint per physical file, hinders efficient fuzzy matching when I remove duplicates. If I discard an MP3 in favor of a FLAC, the MP3 fingerprint is lost, causing future MP3s of the same song to require API calls or hard matches. I'm leaning toward a new table that links acoustid_id to fingerprints, allowing multiple fingerprints per AcoustID. This avoids losing valuable MP3 data.

Implementing Alternate Fingerprint Storage

I'm now implementing the auxiliary fingerprint table. I plan to modify the deduplication process. Instead of deleting the fingerprint of a duplicate file, I'll store it in a new "alternate_fingerprints" table linked to the AcoustID. This involves altering the_handle_id_deduplication function to preserve the losing fingerprint, not delete it. Then, the _find_local_fuzzy_match function will be modified to check this auxiliary table, so it won't require an external API lookup, which is more performant.

Optimizing Fuzzy Matching

I'm now zeroing in on optimizing fuzzy matching by leveraging alternate fingerprints. My plan involves a known_fingerprints table to hold fingerprints, even those from "discarded" files. This will enhance local matching, reducing API calls. This new table (known_fingerprints(acoustid_id, fingerprint, quality_score, source_path)) will hold alternate fingerprints. I'm focusing on ensuring fingerprints are added to this table when a file is kept or discarded. I also have to update the_find_local function to use the new table. This should significantly enhance the system's ability to recognize songs locally.

Revising Fingerprint Index

I am now rethinking the structure of fingerprint_index in relation to the known_fingerprints table, as the mapping strategy has implications for the overall schema. It looks like I need a new table or an adjustment to an existing one to make fuzzy matching work correctly. I'm exploring different approaches, considering maintaining a mapping from a fingerprint ID to AcoustID.

Planning Implementation

I'm now focusing on implementing the proposed fingerprint history feature. Considering the previous interactions, it looks like the user expects an implementation. First, I'll create an aux_fingerprints table to store discarded fingerprints. Then I'll create a index and reuse fingerprint_index, if it's not messy. I plan to extend the script to manage the new table, so that fuzzy matching is improved.

Designing Fingerprint History

I'm now designing how to handle "ghost fingerprints." The current fingerprint_index table relies on the files table, and I need a mechanism to store fingerprints even when the associated file is deleted. I've decided on a fingerprint_history table to store fingerprints and their AcoustIDs, and fingerprint_index_history to index the fingerprint history by block. I will modify the local fuzzy match process to check both the current files and the history tables.

Implementing Fingerprint History

I'm now implementing the fingerprint history. Specifically, I'm working to save a file's fingerprint and AcoustID to fingerprint_history and updating the fingerprint_index_history when a duplicate is deleted. This approach maintains the core logic while integrating the optimization. I'm focusing on ensuring this adds value, as the user did ask about performance improvements.

Refining Fuzzy Matching

I'm now refining the fuzzy matching strategy to handle deleted files. My revised plan includes a fingerprint_cache table to store fingerprints and their associated AcoustIDs, independent of file existence. I will update fingerprint_index to reference the fingerprint string, not the file path, so that I can look up fingerprints whether or not the file still exists. I'll write a response describing this change and its benefits before implementing it. The goal is to improve robustness.

Adapting Fuzzy Match Algorithm

I'm now adapting the fuzzy match algorithm. The key issue is that the current implementation of _find_local_fuzzy_match relies on the file path to return results, which causes issues when handling ghost fingerprints. My plan involves modifying the algorithm to retrieve the AcoustID of a match. I will be sure to return the AcoustID, to use in duplicate detection, even if the file is no longer available. This will involve querying fingerprint tables by block to provide candidate IDs, then fuzzy-matching by fingerprints. The logic will need a redesign to handle non-existent file paths.

Implementing Fuzzy Match Optimization

I'm now implementing the fuzzy matching optimization strategy. The core change involves decoupling file paths from the match process. I'm focusing on the refactored _find_local_id_match function, which now searches for candidate AcoustID's directly from fingerprints. This bypasses the old file path dependency. I will be updating my process_library function to utilize this function to allow for faster local identification, reducing unnecessary API calls and reducing reliance on file existence. This means the deduplication logic will now operate directly on AcoustID's, not file paths.

Refactoring Matching Logic

I'm now refactoring the matching logic to work directly with AcoustID's. I'm focusing on the interaction between known_fingerprints and known_fingerprint_blocks. I'm updating_find_local_id_match to return AcoustID's. I'm also considering the implications of matching a "ghost" AcoustID, and whether I should require metadata lookup for such matches, or if I should require user confirmation. I need to make sure I'm querying known_fingerprints and files. I need to figure out what happens if the ID is known locally but the associated file is deleted, in terms of metadata.

Defining Deduplication Workflow

I'm now defining the new deduplication workflow based on fuzzy matches. The core idea is to maintain a persistent cache of fingerprint to AcoustID mappings. When I scan, I first check this cache for a match. If I find a match and the AcoustID exists in the files table, I handle it as a duplicate or upgrade. If the AcoustID is not found in the files table, or there's no cache match, I run an API lookup. Successful matches, either cache or API based, update the cache with the new fingerprint.


## Yes, storing multiple fingerprints for a single AcoustID significantly improves local matching, especially for libraries with mixed formats (FLAC, MP3, AAC).

### Why?

1. **Waveform Differences:** Lossy compression (MP3, AAC) alters the audio waveform. Therefore, the raw fingerprint of an MP3 is mathematically different from the FLAC source of the same song.
2. **The "Ghost" Problem:** In your current script, if you upgrade a song from MP3 to FLAC, the MP3 file (and its fingerprint) is deleted. If you later encounter another MP3 of that same song (e.g., from a backup), the script compares it to the FLAC's fingerprint. The similarity score will likely be low/uncertain, forcing an API call.
3. **The Solution:** By maintaining a history of **all** fingerprints you've ever seen for a specific `AcoustID` (even for files you've deleted), you create a robust local lookup. A new MP3 will fuzzy-match the "ghost" MP3 fingerprint in your database, allowing the script to instantly identify the song and its AcoustID locally, skipping the API call.

I have updated `library_manager.py` to implement this **Fingerprint History** strategy.

### Changes Implemented

1. **New Tables**:

* `known_fingerprints`: Stores pairs of `(acoustid_id, fingerprint)`. This persists even if the physical file is deleted.
* `known_blocks`: Maps fingerprint blocks to `acoustid_id` for fast searching.

1. **`_update_fingerprint_cache`**: A new method that saves every confirmed Fingerprint-to-ID association to these tables.
2. **`_identify_locally`**: A new matching engine that searches `known_blocks` first.

* It finds candidate AcoustIDs based on block matches.
* It performs fuzzy matching against *all* historical fingerprints for those IDs.
* If a high-confidence match is found (`> 0.98`), it returns the **AcoustID** directly, bypassing the API lookup.

1. **Refined Process Flow**: The script now attempts local identification first. If successful, it fetches metadata from the existing database entries (if available) or falls back to the API only if metadata is missing.
