import os
from mutagen.mp3 import MP3
# from mutagen.freeform import FreeformHeaderError
from mutagen.id3 import ID3NoHeaderError

# Set this to your mount point
LIBRARY_PATH = "/mnt/ssk/music"

def check_music_health(path):
    report = {"healthy": 0, "empty": 0, "corrupted": 0}
    corrupted_files = []

    print(f"--- Scanning: {path} ---")

    for root, dirs, files in os.walk(path):
        for name in files:
            if name.lower().endswith((".mp3", ".flac", ".m4a", ".mp4", ".wma", ".wav")):
                file_path = os.path.join(root, name)

                # Check 1: Is the file empty?
                if os.path.getsize(file_path) == 0:
                    report["empty"] += 1
                    corrupted_files.append(f"[EMPTY] {file_path}")
                    continue

                # Check 2: Is the MP3 header valid?
                try:
                    audio = MP3(file_path)
                    report["healthy"] += 1
                except (ID3NoHeaderError, Exception):
                    report["corrupted"] += 1
                    corrupted_files.append(f"[CORRUPT] {file_path}")

    return report, corrupted_files

if __name__ == "__main__":
    if not os.path.exists(LIBRARY_PATH):
        print(f"Error: Path {LIBRARY_PATH} not found. Is the drive mounted?")
    else:
        results, details = check_music_health(LIBRARY_PATH)
        
        print("\n--- Scan Results ---")
        print(f"Healthy Files:   {results['healthy']}")
        print(f"Empty Files:     {results['empty']}")
        print(f"Corrupted Files: {results['corrupted']}")
        
        if details:
            print("\n--- Problematic Files ---")
            for item in details:
                print(item)
