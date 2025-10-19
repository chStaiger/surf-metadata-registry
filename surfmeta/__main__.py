"""Commandline tool to register metadata for data on SURF infrastructure."""

import argparse
import sys
from getpass import getpass

from ckanapi import NotAuthorized

from surfmeta.ckan import Ckan
from surfmeta.ckan_conf import CKANConf, show_available

MAIN_HELP_MESSAGE = """
Create metadata for data on SURF infrastructure.

Usage: surfmeta [subcommand] [options]

Available subcommands:
    ckan        Manage CKAN configurations

Example usage:
    surfmeta ckan list
    surfmeta ckan switch myalias
    surfmeta ckan init mytoken
    surfmeta ckan remove demo
    surfmeta ckan alias myalias https://demo.ckan.org
"""

CKANCONFIG = CKANConf()

def build_parser():
    """Create parser and subparsers and arguments."""
    parser = argparse.ArgumentParser(
        prog="surfmeta",
        description=MAIN_HELP_MESSAGE,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Top-level subparser: `ckan`
    subparsers = parser.add_subparsers(dest="command", required=True)

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
        parser.print_help(sys.stderr)
        sys.exit(1)

# CKAN CONFIG FUNCTIONS
def ckan_list(args):
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
