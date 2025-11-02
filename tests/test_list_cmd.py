import pytest
from unittest.mock import MagicMock
from io import StringIO
import sys

from surfmeta.cli_handlers import handle_md_list

class Args:
    """Simple container for command-line args."""
    def __init__(self, uuid=None, sys=False, user=False):
        self.uuid = uuid
        self.sys = sys
        self.user = user


@pytest.fixture
def ckan_conn_mock():
    """Mock CKAN connection object."""
    mock = MagicMock()
    # Mock list_all_datasets
    mock.list_all_datasets.return_value = [
        {
            "title": "Dataset 1",
            "name": "uuid-1",
        },
        {
            "title": "Dataset 2",
            "name": "uuid-2",
        },
    ]
    # Mock get_dataset_info
    mock.get_dataset_info.return_value = {
        "title": "Dataset 1",
        "name": "uuid-1",
        "organization": {"name": "Org1"},
        "groups": [{"name": "groupA"}],
        "extras": [
            {"key": "system_name", "value": "sys1"},
            {"key": "server", "value": "local"},
            {"key": "algorithm", "value": "RandomForest"},
            {"key": "uuid", "value": "uuid-1"},
        ],
    }
    return mock


def capture_output(func, *args, **kwargs):
    """Helper to capture print output."""
    old_out = sys.stdout
    sys.stdout = mystdout = StringIO()
    try:
        func(*args, **kwargs)
    finally:
        sys.stdout = old_out
    return mystdout.getvalue()


def test_list_all_datasets(ckan_conn_mock):
    args = Args()
    output = capture_output(handle_md_list, ckan_conn_mock, args)
    assert "Dataset 1 (uuid-1)" in output

def test_show_dataset_metadata_full(ckan_conn_mock):
    args = Args(uuid="uuid-1")
    output = capture_output(handle_md_list, ckan_conn_mock, args)
    # General info
    assert "Metadata for dataset: Dataset 1" in output
    assert "Organization: Org1" in output
    assert "Groups      : groupA" in output
    # System metadata
    assert "System Metadata:" in output
    assert "system_name" in output
    # User metadata
    assert "User Metadata:" in output
    assert "algorithm" in output


def test_show_dataset_metadata_sys_flag(ckan_conn_mock):
    args = Args(uuid="uuid-1", sys=True)
    output = capture_output(handle_md_list, ckan_conn_mock, args)
    assert "System Metadata:" in output
    assert "system_name" in output
    # Should not include user metadata
    assert "algorithm" not in output
    # Should not print Organization / Groups
    assert "Organization" not in output


def test_show_dataset_metadata_user_flag(ckan_conn_mock):
    args = Args(uuid="uuid-1", user=True)
    output = capture_output(handle_md_list, ckan_conn_mock, args)
    assert "User Metadata:" in output
    assert "algorithm" in output
    # Should not include system metadata
    assert "system_name" not in output
    # Should not print Organization / Groups
    assert "Organization" not in output

