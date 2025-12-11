"""Module to create system metadata."""

import warnings
from pathlib import Path

from surfmeta.utils import calculate_local_checksum, calculate_remote_checksum, get_system_info

SYSTEMS = ["snellius", "spider", "src-surf-hosted-nl", "src.surf-hosted.nl"]


def local_meta():
    """Create standard metadata for local data."""
    meta = {}
    meta["server"] = "local"
    meta["system_name"] = "local"
    return meta


def snellius_meta():
    """Create standard snellius metadata."""
    meta = {}
    meta["system_name"] = "snellius"
    meta["server"] = "snellius.surf.nl"
    meta["protocols"] = ["ssh", "rsync"]
    return meta


def spider_meta():
    """Create standard spider metadata."""
    meta = {}
    meta["system_name"] = "spider"
    meta["server"] = "spider.surfsara.nl"
    meta["protocols"] = ["ssh", "rsync"]
    return meta


def rsc_meta():
    """Create standard spider metadata."""
    meta = {}
    meta["system_name"] = "researchcloud"
    meta["server"] = get_system_info()
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
    ----
        meta (dict): Metadata dictionary to update.
        file_path (Path): Path to the file (local or remote).
        remote (bool): If True, calculate checksum remotely.
        host (str): Remote host (required if remote=True).
        username (str): SSH username (required if remote=True).
        algorithm (str): Hash algorithm (default: 'md5').

    Returns:
    -------
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
    meta["location"] = f"{host}:{file_path}"
    return meta
