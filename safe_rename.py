import os


def safe_rename(current_path: str, new_filename: str) -> str:
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
        unique_name = f"{name} ({counter}){extension}"
        target_path = os.path.join(directory, unique_name)
        counter += 1

    os.rename(current_path, target_path)
    return target_path
