import os
import subprocess
import configparser
import concurrent.futures
from pathlib import Path
from mutagen.flac import FLAC, FLACNoHeaderError


def load_config(config_file="config.ini"):
    """Loads the source and target directories from the config file."""
    config = configparser.ConfigParser()

    if not os.path.exists(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        return None, None

    config.read(config_file)
    try:
        source_dir = config.get("Settings", "source_dir")
        target_dir = config.get("Settings", "target_dir")
        return source_dir, target_dir
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"Configuration error: {e}")
        return None, None


def validate_flac(file_path):
    """Uses Mutagen to validate the FLAC file. Returns (is_valid, message)."""
    try:
        audio = FLAC(file_path)
        if audio.info.length > 0:
            return (
                True,
                f"Healthy audio stream ({round(audio.info.length, 2)} seconds).",
            )
        else:
            return False, "Audio duration is 0 seconds."

    except FLACNoHeaderError as e:
        return False, f"Could not find a valid FLAC header. Details: {e}"
    except Exception as e:
        return False, f"Unexpected Mutagen error: {e}"


def process_single_file(file_path, target_path):
    """Handles extraction and validation. Returns a tuple: (Status, Message)."""
    output_filename = file_path.stem + ".flac"
    output_path = target_path / output_filename

    command = [
        "ffmpeg",
        "-err_detect",
        "ignore_err",
        "-i",
        str(file_path),
        "-vn",
        "-map_metadata",
        "-1",
        "-c:a",
        "flac",
        "-y",
        "-v",
        "error",
        str(output_path),
    ]

    # 1. Run FFmpeg Conversion
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0 or not output_path.exists():
        return (
            "ERROR",
            f"[ERROR] {file_path.name} -> FFmpeg Failed: {result.stderr.strip()}",
        )

    # 2. Run Mutagen Validation
    is_valid, validation_msg = validate_flac(output_path)

    if is_valid:
        return "SUCCESS", f"[SUCCESS] {file_path.name} -> {validation_msg}"
    else:
        # 3. Delete the file if validation fails
        try:
            output_path.unlink(missing_ok=True)
            return (
                "DELETED",
                f"[DELETED] {file_path.name} -> Validation failed: {validation_msg}. File removed.",
            )
        except Exception as e:
            return (
                "ERROR",
                f"[ERROR] {file_path.name} -> Validation failed: {validation_msg}. Could NOT delete file: {e}",
            )


def process_audio_files(source_dir, target_dir, max_workers=4):
    """Manages concurrent processing and logs failures to a text file."""
    source_path = Path(source_dir)
    target_path = Path(target_dir)
    log_file_path = target_path / "failed_files.txt"

    target_path.mkdir(parents=True, exist_ok=True)

    if not source_path.is_dir():
        print(f"Error: Source directory '{source_dir}' does not exist.")
        return

    # Gather all valid audio files
    allowed_extensions = {".m4a", ".mp4", ".aac"}
    files_to_process = [
        f
        for f in source_path.iterdir()
        if f.is_file() and f.suffix.lower() in allowed_extensions
    ]

    if not files_to_process:
        print("No valid audio files found in the source directory.")
        return

    print(
        f"Found {len(files_to_process)} files. Starting concurrent processing with {max_workers} workers...\n"
    )

    error_logs = []

    # Process files concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single_file, file_path, target_path): file_path
            for file_path in files_to_process
        }

        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                status, result_message = future.result()
                print(result_message)

                # If the file failed or was deleted, add it to our log list
                if status in ["ERROR", "DELETED"]:
                    error_logs.append(result_message)
            except Exception as exc:
                fatal_msg = f"[FATAL ERROR] {file_path.name} generated an unhandled exception: {exc}"
                print(fatal_msg)
                error_logs.append(fatal_msg)

    # Write errors to the log file, if any exist
    if error_logs:
        with open(log_file_path, "w", encoding="utf-8") as log_file:
            log_file.write("--- Failed Audio Files Log ---\n\n")
            for log in error_logs:
                log_file.write(f"{log}\n")
        print(
            f"\n[INFO] {len(error_logs)} files failed. A report was saved to: {log_file_path}"
        )
    else:
        print(
            "\n[INFO] Flawless run! All files processed successfully. No error log needed."
        )


if __name__ == "__main__":
    src_dir, tgt_dir = load_config()

    if src_dir and tgt_dir:
        print(f"Source Directory: {src_dir}")
        print(f"Target Directory: {tgt_dir}\n")

        # Increase max_workers if your computer has a high-end CPU
        process_audio_files(src_dir, tgt_dir, max_workers=4)
        print("Batch processing complete.")
