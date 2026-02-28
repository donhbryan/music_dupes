import os

def cleanup_empty_folders(folder):
        """Recursively removes empty folders in the music dir."""
        if not os.path.exists(folder):
            return
        print("Cleaning up empty source folders...")
        for root, dirs, _ in os.walk(folder, topdown=False):
            for name in dirs:
                try:
                    p = os.path.join(root, name)
                    os.rmdir(p)  # Only removes if empty
                except OSError:
                    pass

if __name__ == "__main__":
    folder = "/mnt/ssk/music"
    cleanup_empty_folders(folder)
