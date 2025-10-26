"""Commandline tool to register metadata for data on SURF infrastructure."""

import argparse
import json
import sys
from getpass import getpass
from pathlib import Path

from ckanapi import NotAuthorized

from surfmeta.ckan import Ckan
from surfmeta.ckan_conf import CKANConf, show_available
from surfmeta.cli_utils import (
    get_ckan_connection,
    load_and_validate_flat_json,
    merge_ckan_metadata,
    user_input_meta,
)
from surfmeta.sys_utils import SYSTEMS, get_system_info, local_meta, meta_checksum, snellius_meta

MAIN_HELP_MESSAGE = """
Create metadata for data on SURF infrastructure.

Usage: surfmeta [subcommand] [options]

Available subcommands:
    ckan        Manage CKAN configurations
    create      Create a new dataset only containing metadata in CKAN

Example usage:
    surfmeta ckan list
    surfmeta ckan switch myalias
    surfmeta ckan init
    surfmeta ckan remove ckanurl
    surfmeta ckan alias myalias https://demo.ckan.org
    surfmeta ckan orgs
    surfmeta ckan groups

    surfmeta create path
"""

CKANCONFIG = CKANConf()


def build_parser():
    """Create parser and subparsers and arguments."""
    parser = argparse.ArgumentParser(
        prog="surfmeta", description=MAIN_HELP_MESSAGE, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Top-level subparser: `ckan`
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ───────────────────────────────
    # CKAN subparser group
    # surfmeta ckan <subcommand>
    # ───────────────────────────────

    ckan_parser = subparsers.add_parser("ckan", help="CKAN configuration commands")
    ckan_subparsers = ckan_parser.add_subparsers(dest="ckan_command", required=True)

    # `surfmeta ckan list`
    parser_list = ckan_subparsers.add_parser("list", help="List all CKAN configurations")
    parser_list.set_defaults(func=ckan_list)

    # `surfmeta ckan switch url_or_alias`
    parser_switch = ckan_subparsers.add_parser("switch", help="Switch active CKAN config")
    parser_switch.add_argument("url_or_alias", help="URL or alias to switch to")
    parser_switch.set_defaults(func=ckan_switch)

    # `surfmeta ckan init password`
    parser_init = ckan_subparsers.add_parser("init", help="Initialize CKAN config with token")
    parser_init.add_argument("url_or_alias", help="CKAN URL or lias for the URL.")
    parser_init.set_defaults(func=ckan_init)

    # `surfmeta ckan remove url_or_alias`
    parser_remove = ckan_subparsers.add_parser("remove", help="Remove CKAN config")
    parser_remove.add_argument("url_or_alias", help="URL or alias to remove")
    parser_remove.set_defaults(func=ckan_remove)

    # `surfmeta ckan alias alias url`
    parser_alias = ckan_subparsers.add_parser("alias", help="Set an alias for a CKAN URL")
    parser_alias.add_argument("alias", help="Alias name")
    parser_alias.add_argument("url", help="URL to associate with alias")
    parser_alias.set_defaults(func=ckan_alias)

    # `surfmeta ckan orgs`
    parser_ckan_orgs = ckan_subparsers.add_parser("orgs", help="List organizations from CKAN")
    parser_ckan_orgs.add_argument("--full", action="store_true", help="Include full organization metadata")
    parser_ckan_orgs.set_defaults(func=ckan_list_orgs)

    # `surfmeta ckan groups`
    parser_ckan_groups = ckan_subparsers.add_parser("groups", help="List groups from CKAN")
    parser_ckan_groups.add_argument("--full", action="store_true", help="Include full group metadata")
    parser_ckan_groups.set_defaults(func=ckan_list_groups)

    # surfmeta create
    parser_create = subparsers.add_parser("create", help="Create a new metadata entry interactively in CKAN")
    parser_create.add_argument("path", type=Path, help="Path for which to create metadata.")
    parser_create.add_argument(
        "--metafile", type=Path, help="Path to a JSON file containing additional metadata."
    )
    parser_create.set_defaults(func=cmd_create)

    # surfmeta create-meta-file

    parser_create_meta_file = subparsers.add_parser(
        "create-meta-file",
        help="Interactively create a JSON metadata file"
    )
    parser_create_meta_file.add_argument(
        "output",
        type=Path,
        help="Path to store the metadata JSON file"
    )
    parser_create_meta_file.set_defaults(func=cmd_create_meta_file)

    return parser


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


# CKAN CONFIG FUNCTIONS
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
    meta = user_input_meta(ckan_conn)

    # Determine system metadata
    system = [name for name in SYSTEMS if name in get_system_info()]
    if len(system) == 0:
        sys_meta = local_meta()
    elif system[0] == "snellius":
        sys_meta = snellius_meta()
    else:
        sys_meta = {}

    # Optional: verify checksum if file exists
    if args.path.is_file():
        meta_checksum(sys_meta, args.path.absolute())

    # Load CKAN-style extras from metafile
    extras = []
    if args.metafile:
        try:
            extras = load_and_validate_flat_json(args.metafile)
        except Exception as e: # pylint: disable=broad-exception-caught
            print(f"❌ Error reading metafile: {e}")
            return

    print(meta)
    print(sys_meta)
    print(extras)

    ckan_metadata = merge_ckan_metadata(meta, sys_meta, extras)
    try:
        ckan_conn.create_dataset(ckan_metadata, verbose=True)
        print("✅ Dataset created successfully!")
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"❌ Failed to create dataset: {e}")


def cmd_create_meta_file(args):
    """Create a JSON metadata file interactively.

    Steps:
    1. Ask for Prov-O metadata.
    2. Ask for user-defined metadata (key-value pairs).
    3. Save to the specified file.
    """
    json_path = Path(args.output).absolute()
    print(json_path)

    # Step 1: Collect Prov-O metadata
    print("Add Prov-O metadata (leave blank to skip any field):")
    prov_fields = [
        "prov:wasGeneratedBy",
        "prov:wasDerivedFrom",
        "prov:startedAtTime",
        "prov:endedAtTime",
        "prov:actedOnBehalfOf",
        "prov:SoftwareAgent",
    ]
    prov_metadata = {}
    for field in prov_fields:
        value = input(f"{field}: ").strip()
        if value:
            prov_metadata[field] = value

    # Step 2: Collect user-defined metadata
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

    # Combine metadata
    all_metadata = {**prov_metadata, **user_metadata}

    # Step 3: Save to JSON file
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_metadata, f, indent=4)
        print(f"\nMetadata saved to: {json_path}")
    except Exception as e: # pylint: disable=broad-exception-caught
        print(f"Error saving metadata file: {e}")
