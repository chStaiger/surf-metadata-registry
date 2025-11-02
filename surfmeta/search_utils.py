"""Search helpers."""
from surfmeta.metadata_utils import normalize_extras_for_search


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


def print_dataset_results(datasets):
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


def search_datasets(datasets, keyword=None, org=None, group=None):
    """Return a list of datasets matching given filters."""
    return [ds for ds in datasets if _dataset_matches(ds, keyword, org, group)]
