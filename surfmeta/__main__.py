"""Commandline tool functions."""

import argparse
import json
import sys
from getpass import getpass
from pathlib import Path

from ckanapi import NotAuthorized

from surfmeta.ckan import Ckan
from surfmeta.ckan_conf import CKANConf, show_available
from surfmeta.metadata_utils import (
    get_sys_meta,
    input_metadata_extras,
    load_and_validate_flat_json,
    merge_ckan_metadata,
)
from surfmeta.utils import get_ckan_connection
from surfmeta.system_metadata import meta_checksum

CKANCONFIG = CKANConf()


def build_parser():
    """Create parser and subparsers and arguments."""
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
  surfmeta md-update <uuid> --metafile metadata.json

Use "surfmeta <command> --help" for more information on a command.""",
    )

    # --- Main subcommands ---
    subparsers = parser.add_subparsers(
        title="Available commands",
        metavar="<command>",
        dest="command",
        required=True,
        help="Run 'surfmeta <command> --help' for more details",
    )

    # ───────────────────────────────
    # CKAN subparser group
    # ───────────────────────────────
    ckan_parser = subparsers.add_parser("ckan", help="Manage CKAN configurations and connections")
    ckan_subparsers = ckan_parser.add_subparsers(
        dest="ckan_command", required=True, metavar="<ckan-command>", title="CKAN commands"
    )

    parser_list = ckan_subparsers.add_parser("list", help="List all CKAN configurations")
    parser_list.set_defaults(func=ckan_list)

    parser_switch = ckan_subparsers.add_parser("switch", help="Switch active CKAN configuration")
    parser_switch.add_argument("url_or_alias", help="URL or alias to switch to")
    parser_switch.set_defaults(func=ckan_switch)

    parser_init = ckan_subparsers.add_parser("init", help="Initialize CKAN config with API token")
    parser_init.add_argument("url_or_alias", help="CKAN URL or alias")
    parser_init.set_defaults(func=ckan_init)

    parser_remove = ckan_subparsers.add_parser("remove", help="Remove CKAN configuration")
    parser_remove.add_argument("url_or_alias", help="URL or alias to remove")
    parser_remove.set_defaults(func=ckan_remove)

    parser_alias = ckan_subparsers.add_parser("alias", help="Set an alias for a CKAN URL")
    parser_alias.add_argument("alias", help="Alias name")
    parser_alias.add_argument("url", help="URL to associate with the alias")
    parser_alias.set_defaults(func=ckan_alias)

    parser_ckan_orgs = ckan_subparsers.add_parser("orgs", help="List organizations from CKAN")
    parser_ckan_orgs.add_argument("--full", action="store_true", help="Include full organization metadata")
    parser_ckan_orgs.set_defaults(func=ckan_list_orgs)

    parser_ckan_groups = ckan_subparsers.add_parser("groups", help="List groups from CKAN")
    parser_ckan_groups.add_argument("--full", action="store_true", help="Include full group metadata")
    parser_ckan_groups.set_defaults(func=ckan_list_groups)

    # ───────────────────────────────
    # create
    # ───────────────────────────────
    parser_create = subparsers.add_parser("create", help="Create a new metadata entry interactively in CKAN")
    parser_create.add_argument("path", type=Path, help="Path for which to create metadata")
    parser_create.add_argument("--metafile", type=Path, help="Path to a JSON file with additional metadata")
    parser_create.set_defaults(func=cmd_create)

    # ───────────────────────────────
    # create-meta-file
    # ───────────────────────────────
    parser_create_meta_file = subparsers.add_parser(
        "md-create", help="Interactively create a JSON metadata file"
    )
    parser_create_meta_file.add_argument("output", type=Path, help="Path to store the metadata JSON file")
    parser_create_meta_file.set_defaults(func=cmd_create_meta_file)

    # ───────────────────────────────
    # md-list
    # ───────────────────────────────
    parser_md_list = subparsers.add_parser("md-list", help="List metadata entries from CKAN")
    parser_md_list.add_argument("uuid", nargs="?", help="Optional UUID of the dataset to inspect")
    group = parser_md_list.add_mutually_exclusive_group()
    group.add_argument("--sys", action="store_true", help="Show only system metadata")
    group.add_argument("--user", action="store_true", help="Show only user metadata")
    parser_md_list.set_defaults(func=cmd_md_list)

    # ───────────────────────────────
    # md-search
    # ───────────────────────────────
    parser_md_search = subparsers.add_parser("md-search", help="Search CKAN datasets")
    parser_md_search.add_argument("--keyword", "-k", help="Keyword to search for in title, name, or metadata")
    parser_md_search.add_argument("--org", "-o", help="Filter by organization")
    parser_md_search.add_argument("--group", "-g", help="Filter by group")
    parser_md_search.set_defaults(func=cmd_md_search)

    # ───────────────────────────────
    # md-update
    # ───────────────────────────────
    parser_md_update = subparsers.add_parser("md-update", help="Update metadata for an existing dataset")
    parser_md_update.add_argument("uuid", help="UUID of the dataset to update")
    parser_md_update.add_argument("--metafile", type=Path, required=True, help="Path to JSON metadata file")
    parser_md_update.set_defaults(func=cmd_md_update)

    return parser


# CKAN COMMAND FUNCTIONS
def ckan_list(args):  # pylint: disable=unused-argument
    """List all available ckan configurations."""
    show_available(CKANCONFIG)


def ckan_switch(args):
    """Switch between ckan configurations."""
    CKANCONFIG.set_ckan(args.url_or_alias)


def ckan_alias(args):
    """Create alias for ckan configuration."""
    CKANCONFIG.set_alias(args.alias, args.url)


def ckan_init(args):
    """Initialise the ckan configuration with a valid token."""
    CKANCONFIG.set_ckan(args.url_or_alias)
    url, entry = CKANCONFIG.get_entry()
    if sys.stdin.isatty() or "ipykernel" in sys.modules:
        token = getpass(f"Your CKAN token for {args.url_or_alias} : ")
    else:
        print(f"Your CKAN token for {args.url_or_alias} : ")
        token = sys.stdin.readline().rstrip()
    try:
        _ = Ckan(url, token)
        entry["token"] = token
        CKANCONFIG.ckans[url] = entry  # Update the entry with the token
        CKANCONFIG.save()
    except NotAuthorized:
        print(f"AUTH_ERR: Cannot authorize user for {url}. Token might be wrong.")


def ckan_remove(args):
    """Remove a ckan configuration."""
    CKANCONFIG.delete_alias(args.url_or_alias)


def ckan_list_orgs(args):
    """List ckan organisations."""
    ckan_conn = get_ckan_connection()
    try:
        orgs = ckan_conn.list_organisations(include_extras=args.full)
        if not orgs:
            print("⚠️ No organizations found.")
            return

        if args.full:
            for org in orgs:
                print(f"- {org['name']} ({org.get('title', 'No title')})")
        else:
            for name in orgs:
                print(f"- {name}")
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Error listing organizations: {e}")


def ckan_list_groups(args):
    """List ckan groups."""
    ckan_conn = get_ckan_connection()
    try:
        groups = ckan_conn.list_groups(include_extras=args.full)
        if not groups:
            print("⚠️ No groups found.")
            return

        if args.full:
            for group in groups:
                print(f"- {group['name']} ({group.get('title', 'No title')})")
        else:
            for name in groups:
                print(f"- {name}")
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Error listing groups: {e}")


def cmd_create(args):
    """Create a new dataset in CKAN."""
    ckan_conn = get_ckan_connection()

    # Determine system metadata
    sys_meta = get_sys_meta()

    # Optional: verify checksum if file exists
    if args.path.is_file():
        meta_checksum(sys_meta, args.path.resolve())

    # Load CKAN-style extras from metafile
    extras = []
    if args.metafile:
        try:
            extras = load_and_validate_flat_json(args.metafile)
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"❌ Error reading metafile: {e}")
            return

    meta = user_input_meta(ckan_conn)

    ckan_metadata = merge_ckan_metadata(meta, sys_meta, extras)
    try:
        create_dataset(ckan_conn, ckan_metadata)
        print("✅ Dataset created successfully!")
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Failed to create dataset: {e}")


def cmd_create_meta_file(args):
    """Create a JSON metadata file interactively."""
    json_path = Path(args.output).absolute()

    prov_metadata, user_metadata = input_metadata_extras()
    all_metadata = {**prov_metadata, **user_metadata}

    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_metadata, f, indent=4)
        print(f"\nMetadata saved to: {json_path}")
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error saving metadata file: {e}")


def cmd_md_list(args):
    """List metadata entries or metadata details for a dataset."""
    ckan_conn = get_ckan_connection()
    try:
        handle_md_list(ckan_conn, args)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Error: {e}")


def cmd_md_search(args):
    """Search for entries."""
    ckan_conn = get_ckan_connection()
    try:
        handle_md_search(ckan_conn, args)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Error: {e}")


def cmd_md_update(args):
    """Update metadata for a dataset."""
    ckan_conn = get_ckan_connection()
    try:
        handle_md_update(ckan_conn, args)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Error: {e}")


def main():
    """CLI with different subcommands."""
    parser = build_parser()
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        print(f"DEBUG: no function for {args}")
    sys.exit(1)
