Added SIMILARITY_STICKY: A new tuning constant set to 0.95.

Added last_selected_album_id: Added to __init__ to track the user's manual selection context.

Updated process_library:

Sticky Logic: Before prompting, it now checks if any candidate matches the last_selected_album_id with a similarity >= 95%. If so, it auto-selects that album.

Strict Auto-Select: Changed the default auto-select logic. It now only auto-selects if there is exactly one candidate with >= 98% similarity. If there are multiple candidates (even with high scores), it falls through to the user prompt.

Context Updating: When the user manually selects a single album from the menu, last_selected_album_id is updated, enabling the sticky behavior for subsequent songs.
