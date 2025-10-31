"""Useful functions for cli."""

import uuid

from ckanapi import ValidationError

from surfmeta.ckan import Ckan
from surfmeta.metadata_utils import normalize_extras_for_search, load_and_validate_flat_json

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


def create_dataset(ckan_conn: Ckan, meta: dict):
    """Create the dataset."""
    try:
        response = ckan_conn.create_dataset(meta)
        uuid_value = next((item["value"] for item in meta["extras"] if item["key"] == "uuid"), None)
        print(f"🆔 UUID: {uuid_value}")
        print(f"🌐 Name: {response['title']}")
    except ValidationError as e:
        print("❌ Failed to create dataset. Validation error:", e)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print("❌ Failed to create dataset:", e)


# List utils


def handle_md_list(ckan_conn, args):
    """Core logic for listing metadata entries from CKAN."""
    if not args.uuid:
        _list_all_datasets(ckan_conn)
    else:
        _show_dataset_metadata(ckan_conn, args)


def _list_all_datasets(ckan_conn):
    """List all datasets in a table-like format (without org/groups)."""
    datasets = ckan_conn.list_all_datasets(include_private=True)
    if not datasets:
        print("⚠️ No datasets found on this CKAN instance.")
        return

    print(f"Found {len(datasets)} datasets (including private):\n")

    # Determine max lengths for formatting
    max_title_len = max(len(ds.get("title", "<no title>")) for ds in datasets)

    # Print each dataset nicely
    for ds in datasets:
        title = ds.get("title", "<no title>")
        name = ds.get("name", "<no uuid>")
        print(f"- {title:<{max_title_len}} ({name})")


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


# Search utils


def handle_md_search(ckan_conn, args):
    """Search for datasets in CKAN and print results."""
    keyword = args.keyword or ""
    org = args.org or ""
    group = args.group or ""

    if not keyword and not org and not group:
        print("⚠️ Please provide at least one search criterion (keyword, org, or group).")
        return

    datasets = ckan_conn.list_all_datasets(include_private=True)
    if not datasets:
        print("⚠️ No datasets found on this CKAN instance.")
        return

    results = _search_datasets(datasets, keyword, org, group)
    if not results:
        print("⚠️ No datasets found matching the given criteria.")
        return

    # Print results in table-like format
    print(f"Found {len(results)} datasets:\n")
    _print_dataset_results(results)


def _print_dataset_results(datasets):
    """Nicely format and print CKAN dataset search results."""
    if not datasets:
        print("⚠️ No datasets found.")
        return

    # Compute max lengths for alignment
    max_title_len = max(len(ds.get("title", "<no title>")) for ds in datasets)
    max_name_len = max(len(ds.get("name", "<no uuid>")) for ds in datasets)
    max_org_len = max(len(ds.get("organization", {}).get("name", "<no org>")) for ds in datasets)

    header = f"{'Title':<{max_title_len}}  {'UUID':<{max_name_len}}  {'Organization':<{max_org_len}}  Groups"
    print(header)
    print("-" * len(header))

    for ds in datasets:
        title = ds.get("title", "<no title>")
        name = ds.get("name", "<no uuid>")
        org = ds.get("organization", {}).get("name", "<no org>")
        groups = [g.get("name", "") for g in ds.get("groups", [])]
        group_str = ", ".join(groups) if groups else "<no groups>"

        print(f"{title:<{max_title_len}}  {name:<{max_name_len}}  {org:<{max_org_len}}  {group_str}")


def _dataset_matches(dataset, keyword="", org_filter="", group_filter=""):
    """Check if a dataset matches keyword, org, and group filters."""
    keyword = (keyword or "").lower()
    org_filter = (org_filter or "").lower()
    group_filter = (group_filter or "").lower()

    title = dataset.get("title", "")
    name = dataset.get("name", "")
    org = dataset.get("organization", {}).get("name", "")
    groups = [g.get("name", "") for g in dataset.get("groups", [])]
    extras = dataset.get("extras", [])

    # Combine title, name, and flattened extras for keyword search
    combined_text = " ".join([title, name] + normalize_extras_for_search(extras))

    if keyword and keyword not in combined_text:
        return False
    if org_filter and org_filter != org.lower():
        return False
    if group_filter and group_filter not in [g.lower() for g in groups]:
        return False

    return True


def _search_datasets(datasets, keyword=None, org=None, group=None):
    """Return a list of datasets matching given filters."""
    return [ds for ds in datasets if _dataset_matches(ds, keyword, org, group)]


# Metadata update
def handle_md_update(ckan_conn, args):
    """Update metadata for an existing dataset in CKAN using a JSON metafile."""
    dataset_id = args.uuid
    metafile = args.metafile

    if not metafile:
        print("❌ You must provide a --metafile argument containing metadata JSON.")
        return
    if not metafile.exists():
        print(f"❌ File not found: {metafile}")
        return

    try:
        dataset = ckan_conn.get_dataset_info(dataset_id)
    except Exception as e:
        print(f"❌ Could not retrieve dataset '{dataset_id}': {e}")
        return

    print(
        f"\n🛠 Updating dataset '{dataset.get('title', '<no title>')}' "
        f"({dataset_id}) with metadata from {metafile}\n"
    )

    # Load and validate metafile (flat JSON)
    try:
        new_extras = load_and_validate_flat_json(metafile)
    except Exception as e:
        print(f"❌ Error reading metafile '{metafile}': {e}")
        return

    # Convert new extras into dict form for merging
    new_meta_dict = {e["key"]: e["value"] for e in new_extras}

    # Extract existing extras as dict
    existing_extras = {e["key"]: e["value"] for e in dataset.get("extras", []) if "key" in e and "value" in e}

    # Merge — replace existing keys with new ones
    merged_extras = {**existing_extras, **new_meta_dict}

    # Convert back to CKAN-style list
    dataset["extras"] = [{"key": k, "value": str(v)} for k, v in merged_extras.items()]

    # Push update
    try:
        updated = ckan_conn.update_dataset(dataset)
        print(f"✅ Dataset '{updated['title']}' successfully updated with metadata from '{metafile}'.")
    except ValidationError as e:
        print("❌ Validation error while updating dataset:", e)
    except Exception as e:
        print("❌ Failed to update dataset:", e)
