# migrate_db.py

 This script will:

1. **Create a fresh database** with the new schema (including the new `known_fingerprints` and `known_blocks` tables).
2. **Copy existing Album and File data** from your old database.
3. **Backfill the Fingerprint History**: It will iterate through your existing files. If a file has both a `fingerprint` and an `acoustid_id` (from the previous version's logic), it will automatically generate the 16-byte blocks and populate the `known_fingerprints` and `known_blocks` tables.

This effectively "pre-trains" the local matching engine using the work you've already done.

### How to use this

1. Ensure your old database is named `library_manager.db` (or edit the `OLD_DB_PATH` in the script).
2. Run the script: `python migrate_db.py`.
3. Once finished, check your `library_manager.py` config and ensure `db_path` points to the new `library_manager_v2.db`.

```python
    CONFIG = {
        # ... other config ...
        "db_path": "library_manager_v2.db", # Update this line
        # ...
    }

```
