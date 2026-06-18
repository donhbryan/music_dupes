import os
from yt_dlp import YoutubeDL

def download_batch_to_flac(url_list, output_path="."):
    """
    Downloads the highest quality audio from a list of YouTube URLs 
    and converts them to FLAC format.
    """
    
    # Configuration options for yt-dlp
    ydl_opts = {
        # Selects the highest quality audio stream available
        'format': 'bestaudio/best',
        
        # Output template
        'outtmpl': os.path.join(output_path, '%(title)s.%(ext)s'),
        
        # Post-processing instructions
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            # Force conversion to FLAC
            'preferredcodec': 'flac', 
        }],
        
        # Ignores errors (like a deleted video) so the batch doesn't stop halfway
        'ignoreerrors': True,
        'quiet': False,
    }

    print(f"Starting batch download for {len(url_list)} video(s)...")
    
    try:
        with YoutubeDL(ydl_opts) as ydl:
            # yt-dlp natively accepts a list of URLs and processes them in order
            ydl.download(url_list)
        print("\nBatch processing complete!")
            
    except Exception as e:
        print(f"\nAn error occurred during the batch process: {e}")

if __name__ == "__main__":
    # Example Usage: You can hardcode your URLs here or input them dynamically
    print("Enter YouTube URLs separated by a comma:")
    user_input = input("> ").strip()
    
    if user_input:
        # Split the input string into a clean list of URLs
        urls = [url.strip() for url in user_input.split(",") if url.strip()]
        
        if urls:
            download_batch_to_flac(urls)
        else:
            print("No valid URLs found.")
    else:
        print("No input provided.")