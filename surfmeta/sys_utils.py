"""Module to create system metadata."""

import hashlib
import platform
import subprocess
import warnings
from pathlib import Path

SYSTEMS = ["snellius"]


def get_system_info():
    """Retrieve info from system where client is run."""
    platform_info = platform.node()
    return platform_info


def local_meta():
    """Create standard metadata for local data."""
    meta = {}
    meta["server"] = "local"
    return meta


def snellius_meta():
    """Create standard snellius metadata."""
    meta = {}
    meta["system_name"] = "snellius"
    meta["server"] = "snellius.surf.nl"
    meta["protocols"] = ["ssh", "rsync"]
    return meta


def meta_checksum(
    meta: dict,
    file_path: Path,
    *,
    remote: bool = False,
    host: str | None = None,
    username: str | None = None,
    algorithm: str = "md5",
) -> dict:
    """Add file path and checksum to metadata.

    Can calculate checksum locally or remotely over SSH.

    Args:
        meta (dict): Metadata dictionary to update.
        file_path (Path): Path to the file (local or remote).
        remote (bool): If True, calculate checksum remotely.
        host (str): Remote host (required if remote=True).
        username (str): SSH username (required if remote=True).
        algorithm (str): Hash algorithm (default: 'md5').

    Returns:
        dict: Updated metadata dictionary.

    """
    if not remote:
        # ✅ Local calculation
        if file_path.is_file():
            meta["checksum"] = (algorithm, calculate_local_checksum(file_path, algorithm))
            meta["location"] = str(file_path)
            return meta
        warnings.warn(f"{str(file_path)} not a file. Cannot create checksum.")
        return meta
    # 🌐 Remote calculation
    if not host or not username:
        raise ValueError("host and username must be provided for remote checksum")

    checksum = calculate_remote_checksum(host, username, file_path, algorithm)
    meta["checksum"] = (algorithm, checksum)
    # Store remote file location in a clean format
    meta["location"] = f"{host}:{file_path.absolute()}"
    return meta


def calculate_local_checksum(file_path: Path, algorithm: str = "sha256") -> str:
    """Calculate the checksum of a file using the given hashing algorithm.

    Args:
        file_path (Path): Path to the file.
        algorithm (str): Hashing algorithm (e.g., 'md5', 'sha1', 'sha256').

    Returns:
        str: Hex digest of the checksum.

    """
    try:
        hasher = hashlib.new(algorithm)
    except ValueError as e:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}") from e

    # Read file in chunks to handle large files efficiently
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def calculate_remote_checksum(host: str, username: str, file_path: Path, algorithm: str = "sha256") -> str:
    """Calculate a checksum for a file on a remote host using SSH.

    Args:
        host (str): The remote hostname or IP address.
        username (str): SSH username.
        file_path (Path): Remote file path.
        algorithm (str): Hash algorithm ('sha256', 'md5', 'sha1', etc.).

    Returns:
        str: The checksum (hex digest) of the remote file.

    Raises:
        RuntimeError: If SSH or checksum command fails.

    """
    # Map algorithm to remote commands
    cmd_map = {"sha256": "sha256sum", "md5": "md5sum", "sha1": "sha1sum", "sha512": "sha512sum"}

    if algorithm not in cmd_map:
        raise ValueError(f"Unsupported algorithm: {algorithm}")

    # Build the SSH command
    remote_file = str(file_path)
    remote_cmd = f"{cmd_map[algorithm]} {remote_file}"
    ssh_cmd = ["ssh", f"{username}@{host}", remote_cmd]

    # Run SSH command
    result = subprocess.run(ssh_cmd, capture_output=True, text=True, check=True)

    if result.returncode != 0:
        raise RuntimeError(f"SSH or checksum failed: {result.stderr.strip()}")

    # Parse checksum (first token in output)
    checksum = result.stdout.strip().split()[0]
    return checksum
