"""
Utility functions for dcache-related operations.

These functions are used by the `surfmeta dcache` CLI commands and provide:
- Tool availability checks
- Authentication
- Event listening
- File/folder operations on dCache
"""

import shutil
import subprocess
import sys
import json
from pathlib import Path
from surfmeta.ckan_conf import CKANConf

# ----------------------------------------------------------------------
# Tool Requirements
# ----------------------------------------------------------------------
def require_dcache_tools():
    """Ensure required external tools are installed: ada, get-macaroon."""
    missing = [tool for tool in ("ada", "get-macaroon") if not shutil.which(tool)]
    if missing:
        print(f"❌ Missing required tools: {', '.join(missing)}")
        sys.exit(1)

# ----------------------------------------------------------------------
# Internal Helper
# ----------------------------------------------------------------------
def _run_dcache_cmd(ada_args: list[str], check_success: str | None = None) -> str:
    """
    Run an ADA command using authentication from CKAN config.

    Args:
        ada_args: List of ADA subcommand arguments, including file paths.
        check_success: Optional string to check for success (case-insensitive).

    Returns:
        stdout of ADA

    Raises:
        RuntimeError if command fails or success string not found
    """
    require_dcache_tools()
    conf = CKANConf()
    auth_type, auth_file = conf.get_dcache_auth()
    auth_file = Path(auth_file).expanduser().resolve()
    if not auth_file.exists():
        raise RuntimeError(f"Authentication file not found: {auth_file}")

    cmd = ["ada"]
    if auth_type == "macaroon":
        cmd += ["--tokenfile", str(auth_file)]
    elif auth_type == "netrc":
        cmd += ["--netrc", str(auth_file)]
    else:
        raise RuntimeError(f"Unknown authentication type: {auth_type}")

    cmd += ada_args

    print("Running ADA command:", " ".join(cmd))
    result = subprocess.run(cmd, check=False, text=True, capture_output=True)
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()

    if stderr:
        raise RuntimeError(f"ADA command failed:\nstdout: {stdout}\nstderr: {stderr}")

    return stdout

# ----------------------------------------------------------------------
# High-Level dCache Actions
# ----------------------------------------------------------------------
def dcache_auth(method: str, file_path: Path, ckan_conf: CKANConf = CKANConf()):
    """Setup dCache authentication and save it in CKAN config."""
    require_dcache_tools()
    if method not in ("macaroon", "netrc"):
        print(f"❌ Invalid auth method '{method}'. Must be 'macaroon' or 'netrc'.")
        sys.exit(1)

    file_path = Path(file_path).expanduser().resolve()
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)

    # Test authentication by listing current directory
    try:
        if method == "macaroon":
            _run_dcache_cmd(["--list", "."], check_success=None)
            print("✅ Macaroon authentication successful.")
            ckan_conf.set_dcache_auth(macaroon=str(file_path))
        else:
            _run_dcache_cmd(["--list", "."], check_success=None)
            print("✅ netrc authentication successful.")
            ckan_conf.set_dcache_auth(netrc=str(file_path))

        print(f"💾 Saved {method} authentication for dCache in '{ckan_conf.config_path}'")
    except RuntimeError as exc:
        print(f"❌ Authentication failed: {exc}")
        sys.exit(1)

def dcache_label(dcache_path: Path, label: str = "test-ckan"):
    """Add a label to a dCache file or folder."""
    # Build ADA command correctly: filename first, then label
    ada_args = ["--setlabel", dcache_path, label]
    _run_dcache_cmd(ada_args, check_success="success")
    print(f"✅ Label '{label}' set successfully on '{dcache_path}'")

def _dcache_get_stat(dcache_path: Path) -> dict:
    """Retrieve dCache stat information."""
    try:
        out = _run_dcache_cmd(["--stat", str(dcache_path)])
        return out
    except RuntimeError as exc:
        raise RuntimeError from exc

def dcache_checksum(dcache_path: Path) -> tuple[str, str]:
    """Compute checksum of a dCache file."""
    out = _run_dcache_cmd(["--checksum", str(dcache_path)], check_success="success")
    for line in out.splitlines():
        if "=" in line:
            parts = line.strip().split()
            algo, checksum = parts[1].split("=")
            print(f"{algo}, {checksum}")


import subprocess
from pathlib import Path
from surfmeta.ckan_conf import CKANConf
from .dcache_utils import require_dcache_tools

def dcache_listen(dcache_path: Path, channel: str = "tokenchannel"):
    """
    Start a dCache event listener on a given folder.

    Args:
        dcache_path (Path): Folder on dCache to listen to
        channel (str): Name of the event channel (default: "tokenchannel")
    """
    require_dcache_tools()

    conf = CKANConf()
    auth_type, auth_file = conf.get_dcache_auth()
    auth_file = Path(auth_file).expanduser().resolve()
    if not auth_file.exists():
        raise RuntimeError(f"Authentication file not found: {auth_file}")

    cmd = ["ada"]
    if auth_type == "macaroon":
        cmd += ["--tokenfile", str(auth_file)]
    elif auth_type == "netrc":
        cmd += ["--netrc", str(auth_file)]
    else:
        raise RuntimeError(f"Unknown authentication type: {auth_type}")

    cmd += ["--events", channel, str(dcache_path)]

    print(f"🎧 Listening to dCache events on '{dcache_path}' (channel: {channel}) …")

    try:
        # Run listener in the foreground; Ctrl+C stops it
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\n🛑 Listener stopped by user.")
    except subprocess.CalledProcessError as exc:
        print(f"❌ Listener failed: {exc}")

def has_label(stat_dict: dict, label: str) -> bool:
    """
    Check if a given label exists in the dCache stat dictionary.

    Args:
        stat_dict (dict): Output from _dcache_get_stat
        label (str): Label to check

    Returns:
        bool: True if label exists, False otherwise
    """
    labels = stat_dict.get("labels", [])
    return label in labels


def get_checksum(stat_dict: dict, algorithm: str = "ADLER32") -> str | None:
    """
    Extract checksum value for a specific algorithm from dCache stat dictionary.

    Args:
        stat_dict (dict): Output from _dcache_get_stat
        algorithm (str): Checksum algorithm, e.g., "ADLER32", "MD5"

    Returns:
        str | None: Checksum value if found, otherwise None
    """
    for chk in stat_dict.get("checksums", []):
        if chk.get("type") == algorithm:
            return chk.get("value")
    return None
