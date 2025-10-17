"""Commandline tool to register metadata for data on SURF infrastructure."""

import argparse
import sys
from importlib.metadata import version

MAIN_HELP_MESSAGE = f"""
Create metadata for data on SURF infrastructure.

Usage: surfmeta [subcommand] [options]

Available subcommands:

Example usage:

"""

def main():
    """CLI with different subcommands."""
    subcommand = "--help" if len(sys.argv) < 2 else sys.argv.pop(1)

    if subcommand in ["-h", "--help"]:
        print(MAIN_HELP_MESSAGE)
    elif subcommand in ["-v", "--version"]:
        print(f"surfiamviz version {version('surfmeta')}")
