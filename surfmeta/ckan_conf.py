"""CKAN Config functions."""

import argparse
import json
import os
import warnings
from pathlib import Path
from typing import Union
from urllib.parse import urlparse

CKAN_CONFIG_FP = Path.home() / ".ckan" / "ckan.json"
DEMO_CKAN = "https://demo.ckan.org"


class CKANConf:
    """Interface to the CKAN config file."""

    def __init__(
        self, parser: argparse.ArgumentParser = None, config_path: Union[str, Path] = CKAN_CONFIG_FP
    ):
        """Read CKAN configuration file and validate it."""
        self.config_path = Path(config_path)
        self.parser = parser

        if not self.config_path.is_file():
            os.makedirs(self.config_path.parent, exist_ok=True)
            self.reset()

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                ckan_conf = json.load(f)
                self.ckans = ckan_conf["ckans"]
                self.cur_ckan = ckan_conf.get("cur_ckan", DEMO_CKAN)
                self.dcache = ckan_conf.get("dcache", ("netrc", "~/.netrc"))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            warnings.warn(f"{self.config_path} not found or invalid. Resetting. Reason: {exc}")
            self.reset()

        self.validate()

    def validate(self):
        """Validate the CKAN configuration."""
        changed = False
        try:
            if not isinstance(self.ckans, dict):
                raise ValueError("CKANs should be a dictionary.")

            if DEMO_CKAN not in self.ckans:
                raise ValueError("Default CKAN URL not found in config.")

            if not isinstance(self.cur_ckan, str):
                raise ValueError("Current CKAN URL must be a string.")

            aliases = set()
            valid_ckans = {}
            for url, entry in self.ckans.items():
                if url != DEMO_CKAN and not self.is_valid_url(url):
                    warnings.warn(f"Invalid CKAN URL: {url}, removing.")
                    changed = True
                elif entry.get("alias") in aliases:
                    warnings.warn(f"Duplicate alias '{entry.get('alias')}', removing entry.")
                    changed = True
                else:
                    valid_ckans[url] = entry
                    if "alias" in entry:
                        aliases.add(entry["alias"])

            self.ckans = valid_ckans
            if self.cur_ckan not in self.ckans:
                warnings.warn("Current CKAN not found in config. Resetting to first available.")
                self.cur_ckan = list(self.ckans)[0]
                changed = True

        except ValueError as exc:
            print(exc)
            self.reset()
            changed = True

        if changed:
            self.save()

    def reset(self):
        """Reset configuration to defaults."""
        self.ckans = {DEMO_CKAN: {"alias": "demo"}}
        self.cur_ckan = DEMO_CKAN
        self.dcache = ("netrc", "~/.netrc")
        self.save()

    def set_dcache_auth(self, macaroon: str = None, netrc: str = None):
        """Set dCache authentication for a CKAN entry.

        Only one method can be set at a time. Passing None will remove the previous setting.
        """
        if macaroon and netrc:
            raise ValueError("Cannot set both macaroon and netrc at the same time.")
        if macaroon:
            self.dcache = ("macaroon", str(macaroon))
        elif netrc:
            self.dcache = ("netrc", str(netrc))
        self.save()

    def get_dcache_auth(self) -> tuple:
        """Get dCache authentication for a CKAN entry.

        Returns a dict with keys:
          - 'macaroon' if set
          - 'netrc' if set
          - empty dict if none set
        """
        method, fname = self.dcache
        return (method, fname)

    # -----------------------------
    # Override save to ensure dcache dict exists
    # -----------------------------
    def save(self):
        """Write configuration to file."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump({"cur_ckan": self.cur_ckan, "ckans": self.ckans, "dcache": self.dcache}, f, indent=4)

    def get_entry(self, url_or_alias: Union[str, None] = None) -> tuple[str, dict]:
        """Get a CKAN config by URL or alias."""
        url_or_alias = self.cur_ckan if url_or_alias is None else str(url_or_alias)
        for url, entry in self.ckans.items():
            if url == url_or_alias:
                return url, entry
        for url, entry in self.ckans.items():
            if entry.get("alias") == url_or_alias:
                return url, entry
        raise KeyError(f"No CKAN entry found for '{url_or_alias}'.")

    def set_ckan(self, url_or_alias: Union[str, Path, None] = None):
        """Set the current CKAN instance."""
        if url_or_alias == "":
            return
        url_or_alias = DEMO_CKAN if url_or_alias is None else str(url_or_alias)
        try:
            url, _ = self.get_entry(url_or_alias)
        except KeyError as exc:
            url = url_or_alias
            if not self.is_valid_url(url):
                if self.parser:
                    raise self.parser.error(f"Invalid CKAN URL: {url}") from exc
                raise ValueError(f"Invalid CKAN URL: {url}") from exc
            self.ckans[url] = {}
        if self.cur_ckan != url:
            self.cur_ckan = url
            self.save()

    def set_alias(self, alias: str, url: str):
        """Assign an alias to a CKAN instance."""
        alias = str(alias)
        url = str(url)
        try:
            self.get_entry(alias)
            raise ValueError(f"Alias '{alias}' already exists.")
        except KeyError:
            pass

        try:
            url, entry = self.get_entry(url)
            entry["alias"] = alias
        except KeyError:
            self.ckans[url] = {"alias": alias}

        self.save()

    def delete_alias(self, alias: str):
        """Remove alias and/or CKAN entry."""
        try:
            url, entry = self.get_entry(alias)
        except KeyError:
            if self.parser:
                self.parser.error(f"Alias '{alias}' not found.")
            raise

        if url == DEMO_CKAN:
            entry.pop("alias", None)
        else:
            self.ckans.pop(url, None)

        self.save()

    def is_valid_url(self, url: str) -> bool:
        """Validate URL format."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False


def show_available(ckan_conf: CKANConf):
    """Display CKAN configurations."""
    for url, entry in ckan_conf.ckans.items():
        prefix = "*" if ckan_conf.cur_ckan in (url, entry.get("alias")) else " "
        alias = entry.get("alias", "[no alias]")
        print(f"{prefix} {alias} -> {url}")
