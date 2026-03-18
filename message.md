Key Changes Made

1. Removed ProcessPoolExecutor ✅

    Before: ProcessPoolExecutor(max_workers=multiprocessing.cpu_count())
    After: ThreadPoolExecutor(max_workers=2)
    Reason: SQLite + multiprocessing = segfaults

2. Added Thread Safety ✅

    Added self.db_lock = threading.Lock() for all direct DB access
    Wrapped all self.cur.execute() calls with with self.db_lock:
    Created _query_audio_hash_safely() helper method

3. Safe Database Queries in Threads ✅

    _api_worker() now calls _query_audio_hash_safely() instead of direct self.cur.execute()
    All thread-local DB access is protected by locks

4. Better Error Handling ✅

    Added traceback.print_exc() throughout
    Catches futures exceptions in executors
    Better logging of actual errors

5. Reduced Worker Count ✅

    CPU phase: 2 workers (was CPU count)
    API phase: 2 workers (was 3)
    Reduces database lock contention

6. Null Safety ✅

    Check for empty fingerprints before API calls
    Check for None values before operations
