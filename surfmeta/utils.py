"""System and other utils."""

import hashlib
import platform
import subprocess
import sys
from pathlib import Path

from surfmeta.ckan import Ckan
from surfmeta.ckan_conf import CKANConf


def get_ckan_connection():
    """Instantiate the ckan connection from the current ckan config."""
    conf = CKANConf()
    url = conf.cur_ckan
    _, entry = conf.get_entry(url)

    if "token" not in entry:
        print(f"AUTHENTICATION ERROR: no token provided for {url}.")
        sys.exit(1)

    return Ckan(url, entry["token"])


def get_system_info():
    """Retrieve info from system where client is run."""
    platform_info = platform.node()
    return platform_info


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
