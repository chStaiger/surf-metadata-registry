"""Search helpers."""

from surfmeta.metadata_utils import normalize_extras_for_search


def _dataset_matches(dataset, keywords=None, org_filter="", group_filter="", system_filter=""):
    """Check if a dataset matches keyword, org, and group filters."""
    keywords = [k.lower() for k in keywords] or []
    org_filter = (org_filter or "").lower()
    group_filter = (group_filter or "").lower()
    system_filter = (system_filter or "").lower()

    title = dataset.get("title", "")
    name = dataset.get("name", "")
    org = dataset.get("organization", {}).get("name", "")
    groups = [g.get("name", "") for g in dataset.get("groups", [])]
    extras = dataset.get("extras", [])
    if extras:
        system = [item["value"] for item in extras if item["key"] == "system_name"]
    else:
        system = None
    # Combine title, name, and flattened extras for keyword search
    combined_text = " ".join([title, name] + normalize_extras_for_search(extras))

    if keywords and not any(keyword in combined_text for keyword in keywords):
        return False
    if org_filter and org_filter != org.lower():
        return False
    if group_filter and group_filter not in [g.lower() for g in groups]:
        return False
    # local data does not have a system name, system can be None and []
    if system_filter and not system and system_filter not in ["local", "localhost"]:
        return False
    if system_filter and system and system_filter != system[0].lower():
        return False

    return True


def print_dataset_results(datasets):
    """Nicely format and print CKAN dataset search results."""
    if not datasets:
        print("⚠️ No datasets found.")
        return

    # Compute max lengths for alignment
    max_title_len = max(len(ds.get("title", "<no title>")) for ds in datasets)
    max_name_len = max(len(ds.get("name", "<no uuid>")) for ds in datasets)
    max_org_len = max(len(ds.get("organization", {}).get("name", "<no org>")) for ds in datasets)

    header = f"{'Title':<{max_title_len}}  {'UUID':<{max_name_len}}  {'Organization':<{max_org_len}}  System"
    print(header)
    print("-" * len(header))

    for ds in datasets:
        system_name = next(
            (item["value"] for item in ds["extras"] if item["key"] == "system_name"), "local or not defined"
        )

        title = ds.get("title", "<no title>")
        name = ds.get("name", "<no uuid>")
        org = ds.get("organization", {}).get("name", "<no org>")

        print(f"{title:<{max_title_len}}  {name:<{max_name_len}}  {org:<{max_org_len}}  {system_name}")


def search_datasets(datasets, keyword=None, org=None, group=None, system=None):
    """Return a list of datasets matching given filters."""
    return [ds for ds in datasets if _dataset_matches(ds, keyword, org, group, system)]
