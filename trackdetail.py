"""Utilities to fetch and update MP3 metadata using AcoustID and Mutagen."""
import os
import json
import acoustid
from mutagen.id3 import ID3, TRCK, TPOS, TCON
from mutagen.easyid3 import EasyID3


API_KEY = "7dlZplmc3N"
file_path = "./data/music/02 Welding (2017_12_10 03_45_02 UTC).mp3"


def get_detailed_metadata(api_key, path):
    try:
        # We request 'recordings' and 'releasegroups' to get track/disc/genre info
        # 'meta' can include: recordings, releases, releasegroups, medium, usermeta
        results = acoustid.match(
            api_key, path, meta="recordings releasegroups releases"
        )

        for score, recording_id, title, artist in results:
            print(f"--- Match Found ({score*100:.0f}%) ---")

            # Use lookup for even deeper data if match doesn't provide enough
            # Duration and Fingerprint are needed for lookup
            duration, fingerprint = acoustid.fingerprint_file(path)
            lookup_data = acoustid.lookup(
                api_key, fingerprint, duration, meta="recordings releases releasegroups"
            )

            if lookup_data["status"] == "ok" and lookup_data["results"]:
                # Navigate the nested JSON structure
                for result in lookup_data["results"]:
                    for recording in result.get("recordings", []):

                        # 1. Track and Disc Number (Found within 'releases')
                        for release in recording.get("releases", []):
                            for medium in release.get("mediums", []):
                                disc_num = medium.get("position")
                                for track in medium.get("tracks", []):
                                    print(f"Track Number: {track.get('position')}")
                                    print(f"Disc Number: {disc_num}")

                        # 2. Genre (Found within 'releasegroups' as tags)
                        for release_group in recording.get("releasegroups", []):
                            tags = release_group.get("tags", [])
                            if tags:
                                # Usually the first few tags are the genres
                                genres = [tag["name"] for tag in tags[:3]]
                                print(f"Genres/Tags: {', '.join(genres)}")
            break  # Stop after the best match

    except Exception as e:
        print(f"Error: {e}")

def update_file_metadata(file_path):
    try:
        # 1. Get IDs and Metadata from AcoustID
        duration, fingerprint = acoustid.fingerprint_file(file_path)
        lookup = acoustid.lookup(
            API_KEY, fingerprint, duration, meta="recordings releases releasegroups"
        )

        if lookup["status"] == "ok" and lookup["results"]:
            # Grab the first (best) match
            recording = lookup["results"][0]["recordings"][0]

            # Extract Genre from Release Group tags
            tags = recording.get("releasegroups", [{}])[0].get("tags", [])
            genre = tags[0]["name"] if tags else "Unknown"

            # Extract Track and Disc Number from the first release
            release = recording.get("releases", [{}])[0]
            medium = release.get("mediums", [{}])[0]
            track_num = medium.get("tracks", [{}])[0].get("position", "0")
            disc_num = medium.get("position", "1")

            # 2. Write to the File using Mutagen
            # We use EasyID3 for simple tags like Genre
            audio = EasyID3(file_path)
            audio["genre"] = genre
            audio.save()

            # We use standard ID3 for specific frame types like Track/Disc
            tags = ID3(file_path)
            tags.add(TRCK(encoding=3, text=str(track_num)))  # Track Number
            tags.add(TPOS(encoding=3, text=str(disc_num)))  # Disc Number/Part of Set
            tags.save()

            print(
                f"Updated {os.path.basename(file_path)}: Track {track_num}, Disc {disc_num}, Genre {genre}"
            )

    except Exception as e:
        print(f"Error processing {file_path}: {e}")


# Example usage
# update_file_metadata('test_song.mp3')

get_detailed_metadata(API_KEY, file_path)
update_file_metadata(file_path)