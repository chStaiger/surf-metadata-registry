"""DCacheManager: A class encapsulating dCache operations."""

import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from surfmeta.ckan_conf import CKANConf
from surfmeta.utils import get_ckan_connection


class DCache:
    """Manager class for dCache operations and CKAN integration."""

    REQUIRED_TOOLS = ("ada", "get-macaroon")

    def __init__(self, ckan_conf: CKANConf = CKANConf()):
        """Initialise the dCache connector.

        Loads CKAN configuration, establishes a CKAN connection, checks that
        required dCache tools are installed, retrieves stored authentication
        settings, normalises the auth file path, and validates that the current
        dCache authentication works.

        Parameters
        ----------
        ckan_conf : CKANConf, optional
            CKAN configuration object. A default instance is used if none is provided.

        Raises
        ------
        RuntimeError
            If authentication validation fails or tools are missing.
        FileNotFoundError
            If the authentication file does not exist.

        """
        self.ckan_conf = ckan_conf
        self.ckan = get_ckan_connection()
        self._require_dcache_tools()
        self.auth_type, self.auth_file = self.ckan_conf.get_dcache_auth()
        self.auth_file = Path(self.auth_file).expanduser().resolve()
        self._validate_auth()

    # ------------------------------------------------------------------
    # Tool Requirements
    # ------------------------------------------------------------------
    def _require_dcache_tools(self):
        missing = [tool for tool in self.REQUIRED_TOOLS if not shutil.which(tool)]
        if missing:
            print(f"❌ Missing required tools: {', '.join(missing)}")
            sys.exit(1)

    # ------------------------------------------------------------------
    # Internal Helper
    # ------------------------------------------------------------------
    def _run_dcache_cmd(self, ada_args: list[str]) -> str:
        if not self.auth_file.exists():
            raise RuntimeError(f"Authentication file not found: {self.auth_file}")

        cmd = ["ada"]
        if self.auth_type == "macaroon":
            cmd += ["--tokenfile", str(self.auth_file)]
        elif self.auth_type == "netrc":
            cmd += ["--netrc", str(self.auth_file)]
        else:
            raise RuntimeError(f"Unknown authentication type: {self.auth_type}")

        cmd += ada_args
        print("Running ADA command:", " ".join(cmd))
        result = subprocess.run(cmd, check=False, text=True, capture_output=True)
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        if stderr:
            raise RuntimeError(f"ADA command failed:\nstdout: {stdout}\nstderr: {stderr}")

        return stdout

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------
    def _validate_auth(self):
        """Private: Check whether the current CKAN config authentication works."""
        if not self.auth_file.exists():
            raise FileNotFoundError(f"Authentication file not found: {self.auth_file}")

        try:
            # Test authentication by listing the root directory
            self._run_dcache_cmd(["--list", "."])
            print(f"✅ {self.auth_type} authentication is valid.")
        except RuntimeError as exc:
            raise RuntimeError(f"Authentication test failed: {exc}")

    @classmethod
    def set_auth(cls, ckan_conf: CKANConf, method: str, file_path: Path):
        """Class method: Set dCache authentication parameters in the CKAN config.

        Parameters
        ----------
        ckan_conf : CKANConf
            CKAN configuration instance to store the authentication.
        method : str
            Authentication method ('macaroon' or 'netrc').
        file_path : Path
            Path to the authentication file.

        """
        if method not in ("macaroon", "netrc"):
            raise ValueError(f"Invalid auth method '{method}'. Must be 'macaroon' or 'netrc'.")

        file_path = Path(file_path).expanduser().resolve()
        if not file_path.exists():
            raise FileNotFoundError(f"Authentication file not found: {file_path}")

        # Create a temporary instance for validation
        temp_instance = cls(ckan_conf)
        temp_instance.auth_type = method
        temp_instance.auth_file = file_path

        try:
            temp_instance._validate_auth()
            # Save to CKAN configuration
            if method == "macaroon":
                ckan_conf.set_dcache_auth(macaroon=str(file_path))
            else:
                ckan_conf.set_dcache_auth(netrc=str(file_path))

            print(f"💾 Saved {method} authentication in '{ckan_conf.config_path}'")
        except RuntimeError as exc:
            print(f"❌ Failed to set authentication: {exc}")
            raise

    # ------------------------------------------------------------------
    # High-Level dCache Actions
    # ------------------------------------------------------------------
    def set_label(self, dcache_path: Path, label: str = "test-ckan"):
        """Set a label on a dCache file or directory.

        Parameters
        ----------
        dcache_path : Path
            Path to the dCache file or directory.
        label : str, optional
            Label to apply, by default "test-ckan".

        """
        self._run_dcache_cmd(["--setlabel", str(dcache_path), label])
        print(f"✅ Label '{label}' set successfully on '{dcache_path}'")

    def get_stat(self, dcache_path: Path) -> dict:
        """Retrieve dCache metadata for a given path.

        Parameters
        ----------
        dcache_path : Path
            Path to the dCache file or directory.

        Returns
        -------
        dict
            Parsed JSON metadata returned by ADA.

        """
        out = self._run_dcache_cmd(["--stat", str(dcache_path)])
        return json.loads(out)

    def get_checksum(self, dcache_path: Path) -> tuple[str, str]:
        """Retrieve the checksum of a dCache file.

        Parameters
        ----------
        dcache_path : Path
            Path to the file in dCache.

        Returns
        -------
        tuple[str, str]
            The checksum algorithm and checksum value.

        Raises
        ------
        KeyError
            If no checksum is found in the ADA output.

        """
        out = self._run_dcache_cmd(["--checksum", str(dcache_path)])
        for line in out.splitlines():
            if "=" in line:
                algo, checksum = line.strip().split()[1].split("=")
                return [algo, checksum]
        raise KeyError("Checksum not found in output.")

    # ------------------------------------------------------------------
    # Event Listening
    # ------------------------------------------------------------------
    def listen(self, dcache_path: Path, channel: str = "tokenchannel"):
        """Listen for dCache filesystem events on a directory.

        Starts an ADA event listener for the given dCache path and processes
        MOVE and DELETE events. When labeled files are moved, the corresponding
        CKAN dataset entries are updated. Delete events trigger a CKAN warning
        entry.

        Parameters
        ----------
        dcache_path : Path
            The dCache directory to monitor for events.
        channel : str, optional
            The dCache notification channel to use, by default "tokenchannel".

        Raises
        ------
        NotADirectoryError
            If the supplied path is not a directory.
        KeyError
            If the notification channel is already in use.
        Exception
            For unexpected ADA output or other unhandled errors.

        """
        cmd = ["ada"]
        if self.auth_type == "macaroon":
            cmd += ["--tokenfile", str(self.auth_file)]
        else:
            cmd += ["--netrc", str(self.auth_file)]
        cmd += ["--events", channel, str(dcache_path)]

        print(f"🎧 Listening to dCache events on '{dcache_path}' (channel: {channel}) …")
        previous_move = None

        try:
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc:
                for line in proc.stdout:
                    line = line.strip()
                    if not line:
                        continue
                    if "IN_MOVED_FROM" in line:
                        previous_move = self._parse_inotify_path(line)
                    elif "IN_MOVED_TO" in line:
                        event_path = self._parse_inotify_path(line)
                        labels = self.get_stat(event_path)["labels"]
                        if previous_move and "test-ckan" in labels:
                            self._update_ckan_location(previous_move, event_path)
                            previous_move = None
                    elif "IN_DELETE" in line:
                        event_path = self._parse_inotify_path(line)
                        self._dcache_warning_ckan(event_path)
            # some fails
            if "is not a directory." in line:
                raise NotADirectoryError(line)
            elif "ERROR: channel name 'tokenchannel' is already used." in line:
                raise KeyError(
                    f"ERROR: channel name {channel} is already used. For delete see surfmeta dcache ada-help."
                )
            else:
                raise Exception(line)
        except KeyboardInterrupt:
            print("\n🛑 Listener stopped by user.")
            self._delete_channel(channel)
        except (NotADirectoryError, KeyError, Exception) as exc:
            print(f"❌ Error: {exc}")

    def _delete_channel(self, channel: str):
        print(f"🗑️ Deleting dCache event channel '{channel}' …")
        self._run_dcache_cmd(["--delete-channel", channel])

    # ------------------------------------------------------------------
    # CKAN Integration
    # ------------------------------------------------------------------
    def _update_ckan_location(self, old_path: str, new_path: str, verbose: bool = False):
        matches = self.ckan.find_dataset_by_dcache_path(old_path)
        if not matches:
            print(f"⚠️ No CKAN dataset found for path: {old_path}")
            return

        for match in matches:
            dataset = match["dataset"]
            dataset_id = dataset["name"]
            extras = dataset.get("extras", [])

            old_location = next((ex["value"] for ex in extras if ex.get("key") == "location"), None)
            new_location = old_location.replace(old_path, new_path)

            if verbose:
                print(f"🔄 Updating dataset '{dataset_id}': {old_location} -> {new_location}")

            updated_extras = [
                {"key": "location", "value": new_location} if ex.get("key") == "location" else ex
                for ex in extras
            ]
            dataset["extras"] = updated_extras

            try:
                self.ckan.update_dataset(dataset)
                print(f"✅ Successfully updated location for dataset '{dataset_id}'.")
            except Exception as e:
                print(f"❌ Failed to update dataset '{dataset_id}': {e}")

    def _dcache_warning_ckan(self, event_path: str):
        result = self.ckan.find_dataset_by_dcache_path(event_path)
        if not result:
            print(f"⚠️ No CKAN dataset found for path: {event_path}")
            return

        dataset = result["dataset"]
        dataset_id = dataset["id"]

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        key = f"!!!DELETED_WARNING_{timestamp}"
        value = f"❌ File deleted from dCache: {event_path} at {timestamp}"

        existing_extras = dataset.get("extras", [])
        existing_extras.append({"key": key, "value": value})
        dataset["extras"] = existing_extras

        try:
            self.ckan.update_dataset(dataset)
            print(f"✅ CKAN dataset '{dataset_id}' updated with deletion warning.")
        except Exception as e:
            print(f"❌ Failed to update CKAN dataset '{dataset_id}': {e}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _parse_inotify_path(self, event_line: str) -> str:
        """Extract path from an inotify event line (regular method)."""
        parts = event_line.rsplit()
        return parts[1].strip()
