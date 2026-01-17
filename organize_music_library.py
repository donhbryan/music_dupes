import os
import acoustid
from mutagen.id3 import ID3, TRCK
from mutagen.easyid3 import EasyID3

# Configuration

API_KEY = "7dlZplmc3N"
MUSIC_FOLDER = "./data/music"  # Update this to your music path


def safe_rename(current_path: str, new_filename: str, rename: bool) -> str:
    """
    Renames a file. If the target exists, appends (1), (2), etc.
    Works with any file type.
    """
    directory = os.path.dirname(current_path)
    name, extension = os.path.splitext(new_filename)

    target_path = os.path.join(directory, new_filename)
    counter = 1

    # Loop until we find a filename that doesn't exist
    while os.path.exists(target_path):
        unique_name = f"{name} ({counter:02d}){extension}"
        target_path = os.path.join(directory, unique_name)
        counter += 1
        
    # Perform the rename if specified
    if rename:
        os.rename(current_path, target_path)
    return target_path


def organize_music_library(root_folder):
    for root, dirs, files in os.walk(root_folder):
        for filename in files:
            if filename.endswith((".mp3", ".flac", ".m4a")):
                file_path = os.path.join(root, filename)

                try:
                    # 1. Identify via AcoustID
                    duration, fingerprint = acoustid.fingerprint_file(file_path)
                    lookup = acoustid.lookup(
                        API_KEY, fingerprint, duration, meta="recordings releases"
                    )

                    if lookup["status"] == "ok" and lookup["results"]:
                        recording = lookup["results"][0]["recordings"][0]
                        title = recording.get("title", "Unknown")

                        # Get 2-digit track number
                        release = recording.get("releases", [{}])[0]
                        track_raw = (
                            release.get("mediums", [{}])[0]
                            .get("tracks", [{}])[0]
                            .get("position", 1)
                        )
                        track_no = str(track_raw).zfill(2)

                        # 2. Update Metadata Tags
                        audio = EasyID3(file_path)
                        audio["title"] = title
                        audio.save()

                        tags = ID3(file_path)
                        tags.add(TRCK(encoding=3, text=track_no))
                        tags.save()

                        # 3. Rename with Collision Handling
                        clean_title = "".join(
                            x for x in title if x.isalnum() or x in " -_"
                        )
                        ext = os.path.splitext(filename)[1]
                        new_name = f"{track_no} {clean_title}{ext}"

                        final_path = safe_rename(file_path, new_name)
                        print(f"Processed: {os.path.basename(final_path)}")

                except Exception as e:
                    print(f"Error on {filename}: {e}")


if __name__ == "__main__":
    organize_music_library(MUSIC_FOLDER)
