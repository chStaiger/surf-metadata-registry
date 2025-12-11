"""Utility functions for dcache-related operations.

These functions are used by the `surfmeta dcache` CLI commands and provide:
- Tool availability checks
- Authentication
- Event listening
- File/folder operations on dCache
"""

import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from surfmeta.ckan import Ckan
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
def _run_dcache_cmd(ada_args: list[str]) -> str:
    """Run an ADA command using authentication from CKAN config.

    Args:
        ada_args: List of ADA subcommand arguments, including file paths.

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
    """Authenticate with dCache and save the parametersin CKAN config."""
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
            _run_dcache_cmd(["--list", "."])
            print("✅ Macaroon authentication successful.")
            ckan_conf.set_dcache_auth(macaroon=str(file_path))
        else:
            _run_dcache_cmd(["--list", "."])
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
    _run_dcache_cmd(ada_args)
    print(f"✅ Label '{label}' set successfully on '{dcache_path}'")


def _dcache_get_stat(dcache_path: Path) -> dict:
    """Retrieve dCache stat information."""
    try:
        out = _run_dcache_cmd(["--stat", str(dcache_path)])
        return json.loads(out)
    except RuntimeError as exc:
        raise RuntimeError from exc


def dcache_checksum(dcache_path: Path) -> tuple[str, str]:
    """Compute checksum of a dCache file."""
    out = _run_dcache_cmd(["--checksum", str(dcache_path)])
    for line in out.splitlines():
        if "=" in line:
            parts = line.strip().split()
            algo, checksum = parts[1].split("=")
            print(f"{algo}, {checksum}")


def dcache_listen(dcache_path: Path, ckan_conn: Ckan, channel: str = "tokenchannel"):  # pylint: disable=too-many-branches,unused-argument
    """Start a dCache event listener on a given folder.

    Parameters
    ----------
    dcache_path : Path
        The PNFS folder path to monitor.
    ckan_conn : Ckan
        An authenticated CKAN connection instance, used to update datasets.
    channel : str, optional
        The name of the dCache notification channel to listen to (default is "tokenchannel").

    """
    require_dcache_tools()

    conf = CKANConf()
    auth_type, auth_file = conf.get_dcache_auth()
    auth_file = Path(auth_file).expanduser().resolve()
    print(auth_type, auth_file)
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

    previous_move = None  # Store the last IN_MOVED_FROM path

    try:
        # Run the listener and capture output line by line
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc:    
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue

                if "IN_MOVED_FROM" in line:
                    event_path = _parse_inotify_path(line)
                    previous_move = event_path
                    # print(f"🟡 Detected move from: {previous_move}")
                elif "IN_MOVED_TO" in line:
                    event_path = _parse_inotify_path(line)
                    labels = _dcache_get_stat(event_path)["labels"]
                    if previous_move:
                        if "test-ckan" in labels:
                            # print(f"🟢 Detected move to: {event_path} (from {previous_move})")
                            update_ckan_location(ckan_conn, previous_move, event_path)
                            previous_move = None
                    else:
                        print(f"⚠️ IN_MOVED_TO detected without a previous IN_MOVED_FROM: {event_path}")
                elif "IN_DELETE" in line:
                    event_path = _parse_inotify_path(line)
                    print(f"🔴 Detected delete: {event_path}")
                    dcache_warning_ckan(event_path, ckan_conn)
                else:
                    continue
        raise subprocess.CalledProcessError(returncode=1,
                                            cmd="dcache listen",
                                            output=f"Channel {channel} already exists."
                                            )
    except KeyboardInterrupt:
        print("\n🛑 Listener stopped by user.")
        # Delete Channel after stopping
        _delete_dcache_channel(auth_type, auth_file, channel)
    except subprocess.CalledProcessError as exc:
        print(f"❌ Listener failed: {exc}")
        msg = exc.stderr or exc.output
        print("  Message:", msg.decode() if hasattr(msg, "decode") else msg)

def _delete_dcache_channel(auth_type, auth_file: Path, channel: str):
    """Delete the dCache event channel on listener shutdown."""
    delete_cmd = ["ada"]
    if auth_type == "macaroon":
        delete_cmd += ["--tokenfile", str(auth_file)]
    elif auth_type == "netrc":
        delete_cmd += ["--netrc", str(auth_file)]
    delete_cmd += ["--delete-channel", channel]

    print(f"🗑️  Deleting dCache event channel '{channel}' …")
    try:
        subprocess.run(delete_cmd, check=True)
        print(f"✅ Channel '{channel}' deleted successfully.")
    except subprocess.CalledProcessError as exc:
        print(f"❌ Failed to delete channel '{channel}': {exc}")


def _parse_inotify_path(event_line: str) -> str:
    """Extract the dCache path from an inotify-style event line.

    Args:
        event_line (str): Line like
        "inotify  /pnfs/grid.sara.nl/data/surfadvisors/disk/cs-testdata/bla1.txt  IN_MOVED_TO  cookie:eVh"

    Returns:
        str: The path portion

    """
    # Split by whitespace and take all but the last part (the event)
    parts = event_line.rsplit()
    return parts[1].strip()


def update_ckan_location(ckan: Ckan, old_path: str, new_path: str, verbose: bool = False):
    """Update the 'location' field of a CKAN dataset from an old PNFS path to a new one.

    This function searches all datasets for an `extras` entry where the key 'location'
    contains `old_path`. If found, it updates that entry to use `new_path` instead.

    Parameters
    ----------
    ckan : Ckan
        An authenticated CKAN connection instance used to modify datasets.
    old_path : str
        The PNFS path currently stored in the dataset's 'location' extra.
    new_path : str
        The new PNFS path to replace the old one in the dataset.
    verbose : bool, optional
        If True, prints status messages, by default False.

    Notes
    -----
    This function is NOT part of the Ckan class.

    """
    # 1) Find dataset by old PNFS path

    matches = ckan.find_dataset_by_dcache_path(old_path)
    if not matches:
        print(f"⚠️ No CKAN dataset found for path: {old_path}")
        return

    for match in matches:
        dataset = match["dataset"]
        dataset_id = dataset["name"]
        extras = dataset.get("extras", [])

        # Extract the existing URL from extras
        old_location = None
        for ex in extras:
            if ex.get("key") == "location":
                old_location = ex.get("value")
                break

        # 2) Build new URL (replace only the PNFS part)
        new_location = old_location.replace(old_path, new_path)

        if verbose:
            print(f"🔄 Updating dataset '{dataset_id}':")
            print(f"   Old location: {old_location}")
            print(f"   New location: {new_location}")

        # 3) Update the dataset dict
        updated_extras = []
        for ex in extras:
            if ex.get("key") == "location":
                updated_extras.append({"key": "location", "value": new_location})
            else:
                updated_extras.append(ex)

        dataset["extras"] = updated_extras

        # Perform update
        try:
            ckan.update_dataset(dataset)
            print(f"✅ Successfully updated location for dataset '{dataset_id}'.")
        except Exception as e:
            print(f"❌ Failed to update dataset '{dataset_id}': {e}")


def dcache_warning_ckan(event_path: str, ckan_conn):
    """Add a CKAN metadata entry to indicate that a file was deleted from dCache.

    Parameters
    ----------
    event_path : str
        The full PNFS path of the deleted file.
    ckan_conn : Ckan
        An authenticated CKAN connection instance.

    """
    # Find the dataset corresponding to this dCache path
    result = ckan_conn.find_dataset_by_dcache_path(event_path)
    if not result:
        print(f"⚠️ No CKAN dataset found for path: {event_path}")
        return

    dataset = result["dataset"]
    dataset_id = dataset["id"]

    # Create a unique strong key using timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"!!!DELETED_WARNING_{timestamp}"

    # Value contains the warning icon and path
    value = f"❌ File deleted from dCache: {event_path} at {timestamp}"

    # Update CKAN dataset
    try:
        # Merge with existing extras to avoid duplicates
        existing_extras = dataset.get("extras", [])
        existing_extras.append({"key": key, "value": value})
        dataset["extras"] = existing_extras

        ckan_conn.update_dataset(dataset)
        print(f"✅ CKAN dataset '{dataset_id}' updated with deletion warning.")
    except Exception as e:
        print(f"❌ Failed to update CKAN dataset '{dataset_id}': {e}")
