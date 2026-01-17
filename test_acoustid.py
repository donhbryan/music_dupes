import acoustid

# Your API Key from acoustid.org
API_KEY = "your_api_key_here"
# Path to an audio file (mp3, wav, etc.)
path = "test_song.mp3"

try:
    results = acoustid.match(API_KEY, path)
    for score, recording_id, title, artist in results:
        print(f"Match Confidence: {score * 100:.0f}%")
        print(f"Artist: {artist}")
        print(f"Title: {title}")
        print(f"ID: {recording_id}\n")
except acoustid.NoBackendError:
    print("Error: fpcalc not found. Ensure it is in your PATH or project folder.")
