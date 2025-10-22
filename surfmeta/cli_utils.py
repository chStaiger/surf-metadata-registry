"""Useful functions for cli."""

import json
import sys
import uuid

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


def user_input_meta(ckan_conn: "Ckan") -> dict:
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
