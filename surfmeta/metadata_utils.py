"""Metadata functions."""

import json
from pathlib import Path
from typing import Any, Dict, List

from surfmeta.system_metadata import SYSTEMS, local_meta, rsc_meta, snellius_meta, spider_meta
from surfmeta.utils import get_system_info

PROVO = [
    "prov:wasGeneratedBy",
    "prov:wasDerivedFrom",
    "prov:startedAtTime",
    "prov:endedAtTime",
    "prov:actedOnBehalfOf",
    "prov:SoftwareAgent",
]


def get_sys_meta() -> dict:
    """Lookup the system and create the metadata."""
    system = [name for name in SYSTEMS if name in get_system_info()]
    if len(system) == 0:
        sys_meta = local_meta()
    elif system[0] == "snellius":
        sys_meta = snellius_meta()
    elif system[0] == "spider":
        sys_meta = spider_meta()
    elif system[0] in ["src-surf-hosted-nl", "src.surf-hosted.nl"]:
        sys_meta = rsc_meta()
    else:
        sys_meta = {}

    return sys_meta


def load_and_validate_flat_json(json_path: Path) -> List[Dict[str, str]]:
    """Load a metadata JSON file, ensure it contains only flat key-value pairs.

    This function only allows primitive values (str, int, float, bool, None)
    or lists of primitives. It converts the JSON into a list of CKAN-style
    extras dictionaries: [{'key': ..., 'value': ...}, ...].

    Args:
        json_path (Path): Path to the JSON metafile.

    Returns:
        List[Dict[str, str]]: List of CKAN key/value entries.

    Raises:
        ValueError: If the JSON is invalid or contains nested structures.
        json.JSONDecodeError: If the file is not valid JSON.
        OSError: If the file cannot be read.

    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Ensure root is a dict
    if not isinstance(data, dict):
        raise ValueError(f"Metafile '{json_path}' must contain a JSON object (key-value pairs).")

    def is_simple_value(v: Any) -> bool:
        """Check if value is a simple type or list of simple types."""
        if isinstance(v, (str, int, float, bool)) or v is None:
            return True
        if isinstance(v, list):
            return all(isinstance(i, (str, int, float, bool)) or i is None for i in v)
        return False

    # Validate and build CKAN-style pairs
    extras = []
    for key, value in data.items():
        if not is_simple_value(value):
            raise ValueError(
                f"Metafile '{json_path}' contains unsupported nested structures for key: '{key}'. "
                "Only primitive types or lists of primitives are allowed."
            )

        # Convert lists to JSON string; primitives to str
        if isinstance(value, list):
            value_str = json.dumps(value)  # e.g. ["md5", "15f8..."]
        else:
            value_str = str(value) if value is not None else ""

        extras.append({"key": key, "value": value_str})

    return extras


def merge_ckan_metadata(meta: dict, sys_meta: dict, extras: list[dict]) -> dict:
    """Merge main metadata, system metadata, and user extras into a CKAN-ready metadata dict.

    Parameters
    ----------
    meta : dict
        Main dataset metadata, may include 'extras'.
    sys_meta : dict
        System metadata; values can be str, numbers, tuples, or lists.
    extras : list of dict
        User-provided extras already in CKAN style [{'key': ..., 'value': ...}].

    Returns
    -------
    dict
        CKAN-ready metadata dictionary with merged 'extras'.

    """
    # Start with a copy of meta to avoid mutating the original
    metadata = dict(meta)

    # Initialize extras list
    merged_extras = list(metadata.get("extras", []))  # existing extras in meta

    # Convert sys_meta items into CKAN extras format
    for key, value in sys_meta.items():
        if isinstance(value, (tuple, list)):
            value_str = json.dumps(value)
        else:
            value_str = str(value)
        merged_extras.append({"key": key, "value": value_str})

    # Add user-provided extras
    merged_extras.extend(extras)

    # Set merged extras back
    metadata["extras"] = merged_extras

    return metadata


def input_metadata_extras() -> tuple[dict, dict]:
    """Fetch CKAN metadata extras interactively."""
    print("Add Prov-O metadata (leave blank to skip any field):")
    prov_metadata = {}
    for field in PROVO:
        value = input(f"{field}: ").strip()
        if value:
            prov_metadata[field] = value
    print("\nAdd your own metadata (key-value pairs). Type 'done' as key to finish.")
    user_metadata = {}
    while True:
        key = input("Key: ").strip()
        if key.lower() == "done":
            break
        if not key:
            print("Key cannot be empty.")
            continue
        value = input("Value: ").strip()
        user_metadata[key] = value

    return prov_metadata, user_metadata


def _flatten_value_for_search(value):
    """Recursively flatten a value (str, list, dict) into lowercase strings."""
    if isinstance(value, str):
        return [value.lower()]
    if isinstance(value, list):
        result = []
        for v in value:
            result.extend(_flatten_value_for_search(v))
        return result
    if isinstance(value, dict):
        result = []
        for k, v in value.items():
            result.append(str(k).lower())
            result.extend(_flatten_value_for_search(v))
        return result
    return [str(value).lower()]


def normalize_extras_for_search(extras):
    """Flatten CKAN extras into lowercase strings for keyword search."""
    meta_key_and_values = []

    for e in extras:
        key = e.get("key", "")
        value = e.get("value", "")

        # Include the key itself
        if key:
            meta_key_and_values.append(str(key).lower())

        # Try to parse JSON values, fallback to string
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            parsed = value

        # Flatten values (list, dict, etc.) into strings
        meta_key_and_values.extend(_flatten_value_for_search(parsed))

    return meta_key_and_values
