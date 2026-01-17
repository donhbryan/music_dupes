def generate_library_report():
    conn = sqlite3.connect("library_manager.db")
    cur = conn.cursor()

    # 1. General Stats
    cur.execute("SELECT COUNT(*), SUM(file_size) FROM files WHERE is_duplicate = 0")
    total_files, total_size = cur.fetchone()

    # 2. Duplicate Stats
    cur.execute("SELECT COUNT(*), SUM(file_size) FROM files WHERE is_duplicate = 1")
    dup_count, saved_bytes = cur.fetchone()

    # 3. Format Distribution
    cur.execute(
        "SELECT format, COUNT(*) FROM files WHERE is_duplicate = 0 GROUP BY format"
    )
    formats = cur.fetchall()

    conn.close()

    # Conversion to GB for readability
    total_gb = (total_size or 0) / (1024**3)
    saved_gb = (saved_bytes or 0) / (1024**3)

    print("\n" + "=" * 30)
    print("MUSIC LIBRARY REPORT")
    print("=" * 30)
    print(f"Total Unique Tracks:  {total_files}")
    print(f"Library Size:         {total_gb:.2f} GB")
    print(f"Duplicates Removed:   {dup_count or 0}")
    print(f"Storage Space Saved:  {saved_gb:.2f} GB")
    print("-" * 30)
    print("Format Breakdown:")
    for fmt, count in formats:
        print(f"  {fmt.upper()}: {count} files")
    print("=" * 30 + "\n")


if __name__ == "__main__":
    generate_library_report()
