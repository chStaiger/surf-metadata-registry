"""Command-line tool functions for SURF metadata management."""

import argparse
import json
import os
import sys
from getpass import getpass
from pathlib import Path

from ckanapi import NotAuthorized, NotFound, ValidationError

from surfmeta.ckan import Ckan
from surfmeta.ckan_conf import CKANConf, show_available
from surfmeta.cli_handlers import (
    create_dataset,
    handle_md_list,
    handle_md_search,
    handle_md_update,
    handle_mdentry_delete_dataset,
    handle_mdentry_delete_key,
    user_input_meta,
)
from surfmeta.dcache_utils import dcache_auth, dcache_checksum, dcache_label, dcache_listen
from surfmeta.metadata_utils import (
    get_sys_meta,
    input_metadata_extras,
    load_and_validate_flat_json,
    merge_ckan_metadata,
)
from surfmeta.system_metadata import meta_checksum
from surfmeta.utils import build_transfer_commands, get_ckan_connection

CKANCONFIG = CKANConf()


# -----------------------------
# CLI Argument Parser
# -----------------------------
def build_parser():
    """Create the main parser and subparsers for the CLI."""
    parser = argparse.ArgumentParser(
        prog="surfmeta",
        description="Create and manage metadata for datasets on SURF infrastructure.",
        usage="surfmeta [-h] <command> [<args>]",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  surfmeta ckan list
  surfmeta ckan init
  surfmeta ckan alias demo https://demo.ckan.org
  surfmeta create ./path/to/data
  surfmeta update <uuid> --metafile metadata.json
Use "surfmeta <command> --help" for more information on a command.""",
    )

    subparsers = parser.add_subparsers(
        title="Available commands",
        dest="command",
        metavar="<command>",
        required=True,
        help="Run 'surfmeta <command> --help' for more details",
    )

    _add_ckan_subcommands(subparsers)
    _add_dataset_subcommands(subparsers)
    _add_dcache_subcommands(subparsers)

    return parser


# -----------------------------
# CKAN Subcommand Registration
# -----------------------------
def _add_ckan_subcommands(subparsers):
    ckan_parser = subparsers.add_parser("ckan", help="Manage CKAN configurations and connections")
    ckan_subparsers = ckan_parser.add_subparsers(
        dest="ckan_command", metavar="<ckan-command>", title="CKAN commands", required=True
    )

    # Core CKAN commands
    cmds = [
        ("list", {}, ckan_list),
        ("switch", {"url_or_alias": {}}, ckan_switch),
        ("init", {"url_or_alias": {}}, ckan_init),
        ("remove", {"url_or_alias": {}}, ckan_remove),
        ("alias", {"alias": {}, "url": {}}, ckan_alias),
        ("orgs", {"--full": {"action": "store_true"}}, ckan_list_orgs),
        ("groups", {"--full": {"action": "store_true"}}, ckan_list_groups),
    ]

    for cmd, args, func in cmds:
        p = ckan_subparsers.add_parser(cmd, help=f"{cmd} CKAN command")
        for name, opts in args.items():
            p.add_argument(name, **opts)
        p.set_defaults(func=func)


# ---------------------------------
# dCache commands to listen to data
# ---------------------------------
def _add_dcache_subcommands(subparsers):
    """Register dcache command and its subcommands."""
    dcache_parser = subparsers.add_parser(
        "dcache",
        help="Commands to monitor a dCache folder and registered files.",
    )

    dcache_subparsers = dcache_parser.add_subparsers(
        dest="dcache_command",
        metavar="<dcache-command>",
        title="dCache commands",
        required=True,
    )

    # dcache auth
    p = dcache_subparsers.add_parser(
        "auth",
        help="Authenticate to dcache",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
    Authenticate to dcache using one of the supported methods.

    Methods:

      --macaroon <file>   Authenticate using a macaroon file.

          You can create a macaroon with the following command:

          get-macaroon \\
            --url https://<server>/<path_to_folder> \\
            --duration P7D \\
            --user <username> \\
            --permissions DOWNLOAD,UPLOAD,DELETE,MANAGE,LIST,READ_METADATA,UPDATE_METADATA,STAGE \\
            --output rclone \\
            mytoken

      --netrc <file>      Authenticate using a netrc file.

          Create a .netrc file with the following format:

          machine <dcache server>
          login <username>
          password <password>

    Notes:
      - Only one method can be used at a time (mutually exclusive)
      - Both methods require 'ada' and 'get-macaroon' to be installed and accessible from your PATH
    """,
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--macaroon", type=Path, help="Path to a macaroon file")
    group.add_argument("--netrc", type=Path, help="Path to a netrc file")
    p.set_defaults(func=cmd_dcache_auth)

    # dcache add label
    p = dcache_subparsers.add_parser("addlabel", help="Add a label to a file or folder on dCache")
    p.add_argument("dcache_path", help="Path on dCache to label (e.g., /pnfs/.../file)")
    p.add_argument(
        "label", nargs="?", default="test-ckan", help="Label to add to the file/folder (default: test-ckan)"
    )
    p.set_defaults(func=cmd_dcache_label)

    # dcache checksum
    p = dcache_subparsers.add_parser("checksum", help="Get checksum.")
    p.add_argument("dcache_path", help="Path on dCache to label (e.g., /pnfs/.../file)")
    p.set_defaults(func=cmd_dcache_checksum)

    # dcache listen
    p = dcache_subparsers.add_parser("listen", help="Start listening for dCache events on a folder")
    p.add_argument("dcache_path", help="Path on dCache to listen for events")
    p.add_argument(
        "--channel", default="tokenchannel", help="Name of the event channel (default: tokenchannel)"
    )
    p.set_defaults(func=cmd_dcache_listen)

    p = dcache_subparsers.add_parser("ada-help", help="Some useful ada examples.")
    p.set_defaults(func=cmd_ada_help)


# -----------------------------
# Dataset Subcommand Registration
# -----------------------------
def _add_dataset_subcommands(subparsers):
    # create
    p = subparsers.add_parser("create", help="Create a new metadata entry interactively in CKAN")
    p.add_argument("path", type=Path, help="Path for which to create metadata")
    p.add_argument("--metafile", type=Path, help="Path to a JSON file with additional metadata")
    p.add_argument(
        "--remote",
        action="store_true",
        help=(
            "Create a metadata for a file on a remote system."
            " Add system information and path manually."
            " ⚠️ No checksum check and no path-exists check will be done."
        ),
    )
    p.set_defaults(func=cmd_create)

    # create-meta-file
    p = subparsers.add_parser("create-md", help="Interactively create a JSON metadata file")
    p.add_argument("output", type=Path, help="Path to store the metadata JSON file")
    p.set_defaults(func=cmd_create_meta_file)

    # md-list
    p = subparsers.add_parser("list", help="List metadata entries from CKAN")
    p.add_argument("uuid", nargs="?", help="Optional UUID of the dataset to inspect")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--sys", action="store_true", help="Show only system metadata")
    g.add_argument("--user", action="store_true", help="Show only user metadata")
    p.set_defaults(func=cmd_md_list)

    # md-search
    p = subparsers.add_parser("search", help="Search CKAN datasets")
    p.add_argument(
        "--keyword",
        "-k",
        action="append",
        help=(
            "Keyword(s) to search in title, name, or metadata.\nExample: --keyword 'data' --keyword 'science'"
        ),
    )

    p.add_argument("--org", "-o", help="Filter by organization")
    p.add_argument("--group", "-g", help="Filter by group")
    p.add_argument("--system", "-s", help="Filter by system")
    p.set_defaults(func=cmd_md_search)

    # md-update
    p = subparsers.add_parser("update", help="Update metadata for an existing dataset")
    p.add_argument("uuid", help="UUID of the dataset to update")
    p.add_argument("--metafile", type=Path, required=True, help="Path to JSON metadata file")
    p.set_defaults(func=cmd_md_update)

    # md-delete
    p = subparsers.add_parser("delete", help="Delete a dataset or metadata key from CKAN")
    p.add_argument("uuid", help="UUID or dataset name to delete")
    p.add_argument("--yes", action="store_true", help="Confirm deletion without prompt")
    p.add_argument("-k", "--key", help="Delete a specific metadata key instead of the entire dataset")
    p.set_defaults(func=cmd_md_delete)

    # get
    p = subparsers.add_parser("get", help="Information how to retrieve data from the systems.")
    p.add_argument("uuid", help="UUID or dataset name")
    # Optional username
    p.add_argument(
        "-u", "--username", help="Username for remote systems (optional)", required=False, default=None
    )
    # Optional destination directory
    p.add_argument(
        "-d",
        "--dest",
        help="Destination directory for downloaded files (optional)",
        required=False,
        default=".",  # Current directory
    )
    p.set_defaults(func=cmd_get)


# -----------------------------
# CKAN Command Functions
# -----------------------------
def ckan_list(args):  # pylint: disable=unused-argument
    """List all available CKAN configurations."""
    show_available(CKANCONFIG)


def ckan_switch(args):
    """Switch between ckan instances."""
    CKANCONFIG.set_ckan(args.url_or_alias)


def ckan_alias(args):
    """Set an alias for ckan URL."""
    CKANCONFIG.set_alias(args.alias, args.url)


def ckan_init(args):
    """Init a ckan connection."""
    CKANCONFIG.set_ckan(args.url_or_alias)
    url, entry = CKANCONFIG.get_entry()
    token = _prompt_ckan_token(args.url_or_alias)
    try:
        _ = Ckan(url, token)
        entry["token"] = token
        CKANCONFIG.ckans[url] = entry
        CKANCONFIG.save()
    except NotAuthorized:
        print(f"❌ AUTH_ERR: Cannot authorize user for {url}. Token might be wrong.")


def _prompt_ckan_token(url_or_alias):
    """Prompt user for CKAN token safely."""
    if sys.stdin.isatty() or "ipykernel" in sys.modules:
        return getpass(f"Your CKAN token for {url_or_alias} : ")
    print(f"Your CKAN token for {url_or_alias} : ")
    return sys.stdin.readline().rstrip()


def ckan_remove(args):
    """Remove a ckan configuration."""
    CKANCONFIG.delete_alias(args.url_or_alias)


def ckan_list_orgs(args):
    """List orgs."""
    _list_entities(get_ckan_connection().list_organisations, args.full, "organizations")


def ckan_list_groups(args):
    """List groups."""
    _list_entities(get_ckan_connection().list_groups, args.full, "groups")


def _list_entities(list_func, include_full, entity_name):
    try:
        entities = list_func(include_extras=include_full)
        if not entities:
            print(f"⚠️ No {entity_name} found.")
            return
        if include_full:
            for e in entities:
                print(f"- {e['name']} ({e.get('title', 'No title')})")
        else:
            for name in entities:
                print(f"- {name}")
    except Exception as e:
        print(f"❌ Error listing {entity_name}: {e}")


# -----------------------------
# Dataset Command Functions
# -----------------------------
def cmd_create(args):
    """Create a CKAN dataset/entry."""
    ckan_conn = get_ckan_connection()
    if args.remote:
        print("⚠️ WARNING: --remote chosen: skipping checksum and system metadata.")
        print(f"Creating metadata for {str(args.path)}.")
        system = input("Enter the system name: ")
        sys_meta = {"system_name": system}
        sys_meta["location"] = str(args.path)
    else:
        sys_meta = get_sys_meta()
        if args.path.is_file():
            meta_checksum(sys_meta, args.path.resolve())

    extras = []
    if args.metafile:
        try:
            extras = load_and_validate_flat_json(args.metafile)
        except Exception as e:
            print(f"❌ Error reading metafile: {e}")
            return

    meta = user_input_meta(ckan_conn)
    ckan_metadata = merge_ckan_metadata(meta, sys_meta, extras)
    try:
        create_dataset(ckan_conn, ckan_metadata)
        print("✅ Dataset created successfully!")
    except ValidationError as e:
        print("❌ Failed to create dataset. Validation error:", e)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print("❌ Failed to create dataset:", e)


def cmd_create_meta_file(args):
    """Create a CKAN compatible metadata file."""
    prov_meta, user_meta = input_metadata_extras()
    all_meta = {**prov_meta, **user_meta}
    try:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(all_meta, f, indent=4)
        print(f"✅ Metadata saved to: {args.output}")
    except Exception as e:
        print(f"❌ Error saving metadata file: {e}")


def cmd_md_list(args):
    """List entries and their metadata."""
    _run_handler(handle_md_list, args)


def cmd_md_search(args):
    """Search CKAN."""
    _run_handler(handle_md_search, args)


def cmd_md_update(args):
    """Update entry."""
    _run_handler(handle_md_update, args)


def cmd_md_delete(args):
    """Delete entry or metadata item."""
    ckan_conn = get_ckan_connection()

    try:
        dataset = ckan_conn.get_dataset_info(args.uuid)
    except NotFound:
        print(f"❌ Dataset '{args.uuid}' not found.")
        return
    except NotAuthorized:
        print(f"❌ Not authorized to access dataset '{args.uuid}'.")
        return
    except Exception as e:
        print(f"❌ Error fetching dataset '{args.uuid}': {e}")
        return

    if getattr(args, "key", None):
        handle_mdentry_delete_key(ckan_conn, args)
    else:
        handle_mdentry_delete_dataset(ckan_conn, dataset, args)


def cmd_get(args):
    """Build the commands to get the data."""
    ckan_conn = get_ckan_connection()
    try:
        dataset = ckan_conn.get_dataset_info(args.uuid)
    except NotFound:
        print(f"❌ Dataset '{args.uuid}' not found.")
        return
    except NotAuthorized:
        print(f"❌ Not authorized to access dataset '{args.uuid}'.")
        return
    except Exception as e:
        print(f"❌ Error fetching dataset '{args.uuid}': {e}")
        return

    # Retrieve metadata of dataset by args.uuid
    commands = build_transfer_commands(dataset, args.username, args.dest)

    title = dataset.get("title", dataset.get("name", args.uuid))

    print(f"\n📦 Dataset: {title}")
    print(f"📁 Destination: {os.path.abspath(args.dest)}\n")

    print("Available transfer commands:\n")

    for method, cmd in commands.items():
        method_pretty = method.upper().replace("_", " ")

        print(f"  • {method_pretty}:")
        print(f"      {cmd}\n")


# -----------------------------
# dCache functions
# ------------------------------


def cmd_dcache_auth(args):
    """Authenticate dcache user."""
    if args.macaroon:
        dcache_auth("macaroon", args.macaroon)
    elif args.netrc:
        dcache_auth("netrc", args.netrc)


def cmd_dcache_label(args):
    """Add a default label for ckan to dCache data."""
    if not getattr(args, "dcache_path", None):
        print("❌ dCache path is required.")
        sys.exit(1)
    label = getattr(args, "label", "test-ckan") or "test-ckan"
    dcache_label(args.dcache_path, label)


def cmd_dcache_checksum(args):
    """Retrieve checksum and its algorithm from dCache."""
    if not getattr(args, "dcache_path", None):
        print("❌ dCache path is required.")
        sys.exit(1)
    dcache_checksum(args.dcache_path)


def cmd_dcache_listen(args):
    """Create a channel to listen to changes and push then through to ckan."""
    dcache_path = getattr(args, "dcache_path", None)
    if not dcache_path:
        print("❌ dCache path is required to listen for events.")
        sys.exit(1)
    ckan_conn = get_ckan_connection()

    channel = getattr(args, "channel", "tokenchannel")  # default channel
    dcache_listen(Path(dcache_path), ckan_conn, channel=channel)


def cmd_ada_help(args=None): # pylint: disable=unused-argument
    """Print some useful ADA examples."""
    examples = [
        {
            "description": "Remove a channel / listener",
            "command": "ada --tokenfile test-macaroon-token.conf --delete-channel mychannel",
        },
        {
            "description": "List all channels",
            "command": "ada --tokenfile test-macaroon-token.conf --channels",
        },
        {
            "description": "Remove file",
            "command": "ada --tokenfile  test-macaroon-token.conf --delete <filename>",
        },
        {
            "description": "List current working directory",
            "command": "ada --tokenfile  test-macaroon-token.conf --list .",
        },
    ]

    print("\n💡 Useful ADA examples:\n")
    for ex in examples:
        print(f"🔹 {ex['description']}:")
        print(f"    {ex['command']}\n")


# -----------------------------
# Helper Functions
# -----------------------------
def _run_handler(handler, args):
    """Wrap a CLI handler with exception handling."""
    try:
        handler(get_ckan_connection(), args)
    except Exception as e:
        # raise Exception from e
        print(f"❌ Error: {e}")


# -----------------------------
# Main Entry Point
# -----------------------------
def main():
    """Start main program."""
    parser = build_parser()
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        print(f"DEBUG: no function found for {args}")
        sys.exit(1)
