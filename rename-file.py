import os
import acoustid
from mutagen.id3 import ID3, TRCK, TPOS
from mutagen.easyid3 import EasyID3

# Configuration
API_KEY = "7dlZplmc3N"
MUSIC_FOLDER = "./data/music"  # Update this to your music path


def process_library(root_folder):
    for root, dirs, files in os.walk(root_folder):
        for filename in files:
            if filename.endswith((".mp3", ".wav", ".flac")):
                file_path = os.path.join(root, filename)
                print(f"Processing: {filename}...")

                try:
                    # 1. Get Metadata from AcoustID
                    duration, fingerprint = acoustid.fingerprint_file(file_path)
                    lookup = acoustid.lookup(
                        API_KEY, fingerprint, duration, meta="recordings releases"
                    )

                    if lookup["status"] == "ok" and lookup["results"]:
                        recording = lookup["results"][0]["recordings"][0]
                        title = recording.get("title", "Unknown Title")

                        # Get Track Number
                        release = recording.get("releases", [{}])[0]
                        medium = release.get("mediums", [{}])[0]
                        track_raw = medium.get("tracks", [{}])[0].get("position", 1)

                        # Format track number to 2 digits (e.g., 01, 02)
                        track_no = str(track_raw).zfill(2)

                        # 2. Update Internal Tags
                        audio = EasyID3(file_path)
                        audio["title"] = title
                        audio.save()

                        tags = ID3(file_path)
                        tags.add(TRCK(encoding=3, text=track_no))
                        tags.save()

                        # 3. Rename the File
                        extension = os.path.splitext(filename)[1]
                        # Clean title of illegal characters for Linux/Windows filesystems
                        clean_title = "".join(
                            x for x in title if x.isalnum() or x in " -_"
                        )
                        new_filename = f"{track_no} {clean_title}{extension}"
                        new_path = os.path.join(root, new_filename)

                        os.rename(file_path, new_path)
                        print(f"Success: -> {new_filename}")

                    else:
                        print(f"Skipping: No match found for {filename}")

                except Exception as e:
                    print(f"Error on {filename}: {e}")


if __name__ == "__main__":
    process_library(MUSIC_FOLDER)
