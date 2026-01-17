import acoustid
import os

# Configuration
API_KEY = "your_api_key_here"
TARGET_DIRECTORY = "./music_files"  # Folder where your songs are


def rename_music_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith((".mp3", ".wav", ".flac", ".m4a")):
            file_path = os.path.join(directory, filename)

            try:
                # Get identification results
                results = list(acoustid.match(API_KEY, file_path))

                if results:
                    # Pick the best match (highest confidence score)
                    score, recording_id, title, artist = results[0]

                    # Clean the metadata for filenames (remove invalid characters)
                    clean_artist = "".join(
                        x for x in artist if x.isalnum() or x in " -_"
                    )
                    clean_title = "".join(x for x in title if x.isalnum() or x in " -_")

                    extension = os.path.splitext(filename)[1]
                    new_name = f"{clean_artist} - {clean_title}{extension}"
                    new_path = os.path.join(directory, new_name)

                    # Rename the file
                    os.rename(file_path, new_path)
                    print(f"Renamed: {filename} -> {new_name}")
                else:
                    print(f"Could not find a match for: {filename}")

            except Exception as e:
                print(f"Error processing {filename}: {e}")


if __name__ == "__main__":
    rename_music_files(TARGET_DIRECTORY)
