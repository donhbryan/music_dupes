import os
from pathlib import Path


def generate_integration_data():
    base = Path("./integration_test_data/music")
    base.mkdir(parents=True, exist_ok=True)

    files = [
        "Artist A/Album 1/Song 1.mp3",
        "Artist A/Album 1/Song 2.flac",
        "Artist B/Hit Single.wav",
        "Downloads/Unknown.m4a",
    ]

    print(f"Generating dummy files in {base}...")
    for f in files:
        full_path = base / f
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w") as f_obj:
            f_obj.write("Dummy audio content")

    print(
        "Done. You can point your Config.MUSIC_FOLDER to './integration_test_data/music' to test scanning."
    )


if __name__ == "__main__":
    generate_integration_data()
