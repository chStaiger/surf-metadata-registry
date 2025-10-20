import platform
import socket
from pathlib import Path
import hashlib
import warnings

def get_system_info():
    """Retrieve info from system where client is run."""
    platform_info = platform.node()
    return platform_info

def snellius_meta():
    """Create standard snellius metadata."""
    meta = {}
    meta["system_name"] = "snellius"
    meta["server"] = "snellius.surf.nl"
    meta["protocols"] = ["ssh", "rsync"]

def meta_checksum(meta: dict, file_path: Path):
    """Add filepath and checksum to metadata."""
    if file_path.is_file():
        #add checksum and filepath to meta
        ctype = "md5"
        meta["checksum"] = (ctype, calculate_local_checksum(file_path, ctype))
        meta["location"] = str(file_path)

        return meta
    warnings.warn(f"{str(file_path)} not a file. Cannot create checksum.")

def calculate_local_checksum(file_path: Path, algorithm: str = "sha256") -> str:
    """
    Calculate the checksum of a file using the given hashing algorithm.

    Args:
        file_path (Path): Path to the file.
        algorithm (str): Hashing algorithm (e.g., 'md5', 'sha1', 'sha256').

    Returns:
        str: Hex digest of the checksum.
    """
    try:
        hasher = hashlib.new(algorithm)
    except ValueError:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    # Read file in chunks to handle large files efficiently
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return hasher.hexdigest()

