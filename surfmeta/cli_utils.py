"""Useful functions for cli."""

import json
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

from ckanapi import ValidationError

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


def user_input_meta(ckan_conn: Ckan) -> dict:
    """Retrieve metadata input through CLI with organisation and optional group selection."""
    # Required metadata fields
    dataset_name = input("Dataset name: ").strip()
    author = input("Author name: ").strip()

    # --- Organisation selection ---
    orgs = ckan_conn.list_organisations()
    if not orgs:
        raise RuntimeError(
            "❌ No organisations found for your account. Cannot create dataset without an organisation."
        )

    print("\n📂 Available Organisations:")
    for idx, org in enumerate(orgs, 1):
        print(f"  {idx}) {org}")

    while True:
        try:
            choice = int(input("Select an organisation by number: "))
            if 1 <= choice <= len(orgs):
                chosen_org = orgs[choice - 1]
                break
            print(f"❌ Invalid choice. Please choose 1–{len(orgs)}.")
        except ValueError:
            print("❌ Please enter a valid number.")

    # --- Optional group selection ---
    groups = ckan_conn.list_groups()
    chosen_groups = []

    if groups:
        use_group = input("Do you want to add the dataset to a group? [y/N]: ").strip().lower()
        if use_group == "y":
            print("\n📁 Available Groups:")
            for idx, grp in enumerate(groups, 1):
                print(f"  {idx}) {grp}")
            while True:
                try:
                    choice = int(input("Select a group by number: "))
                    if 1 <= choice <= len(groups):
                        chosen_groups.append(groups[choice - 1])
                        break
                    print(f"❌ Invalid choice. Please choose 1–{len(groups)}.")
                except ValueError:
                    print("❌ Please enter a valid number.")

    # --- UUID generation ---
    dataset_uuid = str(uuid.uuid4())

    # --- Build metadata dictionary ---
    metadata = {
        "name": dataset_uuid,
        "title": dataset_name,
        "author": author,
        "owner_org": chosen_org,
        "extras": [{"key": "uuid", "value": dataset_uuid}],
    }

    # Add group if selected
    if chosen_groups:
        metadata["groups"] = [{"name": g} for g in chosen_groups]

    return metadata


def create_dataset(ckan_conn: Ckan, meta: dict, sys_meta: dict | None = None):
    """Create the dataset."""
    if sys_meta:
        extras = meta.get("extras", [])
        for key, value in sys_meta.items():
            # CKAN extras must be string key/value pairs
            if not isinstance(value, str):
                value = json.dumps(value)
            extras.append({"key": key, "value": value})
        meta["extras"] = extras

    # Try creating the dataset
    try:
        response = ckan_conn.create_dataset(meta)
        uuid_value = next((item["value"] for item in meta["extras"] if item["key"] == "uuid"), None)
        print("✅ Dataset created successfully!")
        print(f"🆔 UUID: {uuid_value}")
        print(f"🌐 Name: {response['title']}")
    except ValidationError as e:
        print("❌ Failed to create dataset. Validation error:", e)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print("❌ Failed to create dataset:", e)


def load_and_validate_flat_json(json_path: Path) -> List[Dict[str, str]]:
    """Load a JSON file, ensure it contains only flat key-value pairs.

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

def handle_md_list(ckan_conn, args):
    """Core logic for listing metadata entries from CKAN."""
    if not args.uuid:
        _list_all_datasets(ckan_conn)
    else:
        _show_dataset_metadata(ckan_conn, args)


def _list_all_datasets(ckan_conn):
    """List all datasets including private ones."""
    datasets = ckan_conn.list_all_datasets(include_private=True)
    if not datasets:
        print("⚠️ No datasets found on this CKAN instance.")
        return

    print(f"Found {len(datasets)} datasets (including private):\n")
    for ds in datasets:
        title = ds.get("title", "<no title>")
        name = ds.get("name", "<no uuid>")
        org = ds.get("organization", {}).get("name", "<no organization>")
        groups = [g.get("name", "") for g in ds.get("groups", [])]
        group_str = ", ".join(groups) if groups else "<no groups>"
        print(f"- {title} ({name}) | Org: {org} | Groups: {group_str}")


def _show_dataset_metadata(ckan_conn, args):
    """Show metadata for a specific dataset."""
    sys_keys = {"system_name", "server", "protocols", "uuid"}
    dataset = ckan_conn.get_dataset_info(args.uuid)

    extras = dataset.get("extras", [])
    meta_dict = {e["key"]: e["value"] for e in extras if "key" in e and "value" in e}

    # Separate system vs user metadata
    system_meta = {k: v for k, v in meta_dict.items() if k in sys_keys}
    user_meta = {k: v for k, v in meta_dict.items() if k not in sys_keys}

    # Apply filtering if flags are set
    filtered_meta = _apply_flags(args, system_meta, user_meta, meta_dict)

    if not filtered_meta:
        print(f"⚠️ No matching metadata found for dataset {args.uuid}.")
        return

    _print_dataset_info(dataset, system_meta, user_meta, args)


def _apply_flags(args, system_meta, user_meta, meta_dict):
    if args.sys:
        return system_meta
    if args.user:
        return user_meta
    return meta_dict


def _print_dataset_info(dataset, system_meta, user_meta, args):
    title = dataset.get("title", "<no title>")
    name = dataset.get("name", "<no uuid>")
    org = dataset.get("organization", {}).get("name", "<no organization>")
    groups = [g.get("name", "") for g in dataset.get("groups", [])]
    group_str = ", ".join(groups) if groups else "<no groups>"

    print(f"\nMetadata for dataset: {title} (UUID: {name})\n")

    if not args.sys and not args.user:
        print(f"Organization: {org}")
        print(f"Groups      : {group_str}\n")

    if system_meta and not args.user:
        print("System Metadata:")
        for k, v in system_meta.items():
            if isinstance(v, list):
                v = ", ".join(v)
            print(f"  {k:<14}: {v}")
        print()

    if user_meta and not args.sys:
        print("User Metadata:")
        for k, v in user_meta.items():
            if isinstance(v, list):
                v = ", ".join(v)
            print(f"  {k:<14}: {v}")
        print()

