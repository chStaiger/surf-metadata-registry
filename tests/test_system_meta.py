import platform
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import surfmeta.sys_utils as sm


def test_get_system_info(monkeypatch):
    """Ensure get_system_info returns the platform node name."""
    monkeypatch.setattr(platform, "node", lambda: "test-node")
    assert sm.get_system_info() == "test-node"


def test_local_meta_and_snellius_meta():
    """Test that local_meta and snellius_meta return dicts with expected content."""
    assert {'server': 'local'} == sm.local_meta()
    assert {'system_name': 'snellius', 'server': 'snellius.surf.nl', 'protocols': ['ssh', 'rsync']} == sm.snellius_meta()


def test_calculate_local_checksum(tmp_path: Path):
    """Verify correct checksum calculation for a known file content."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world")

    result = sm.calculate_local_checksum(test_file, "md5")
    # known md5 of "hello world"
    assert result == "5eb63bbbe01eeed093cb22bb8f5acdc3"


def test_calculate_local_checksum_invalid_algorithm(tmp_path: Path):
    """Invalid algorithm should raise ValueError."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("content")

    with pytest.raises(ValueError):
        sm.calculate_local_checksum(test_file, "unsupportedalgo")


@patch("subprocess.run")
def test_calculate_remote_checksum(mock_run):
    """Test remote checksum parsing from subprocess output."""
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "deadbeef1234567890  /path/to/file\n"
    mock_run.return_value = mock_result

    checksum = sm.calculate_remote_checksum("host", "user", Path("/remote/file"), "md5")

    assert checksum == "deadbeef1234567890"
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "ssh"
    assert "user@host" in cmd


def test_calculate_remote_checksum_invalid_algorithm():
    """Unsupported algorithm should raise ValueError."""
    with pytest.raises(ValueError):
        sm.calculate_remote_checksum("host", "user", Path("/file"), "invalidalgo")


@patch("surfmeta.sys_utils.calculate_remote_checksum", return_value="fakechecksum")
def test_meta_checksum_remote(mock_remote):
    """meta_checksum should call calculate_remote_checksum for remote files."""
    meta = {}
    file_path = Path("/remote/path/file.txt")
    updated = sm.meta_checksum(meta, file_path, remote=True, host="host", username="user", algorithm="md5")

    assert updated["checksum"] == ("md5", "fakechecksum")
    assert updated["location"] == f"host:{file_path}"
    mock_remote.assert_called_once_with("host", "user", file_path, "md5")


def test_meta_checksum_remote_missing_params():
    """Missing host or username should raise ValueError."""
    with pytest.raises(ValueError):
        sm.meta_checksum({}, Path("/file.txt"), remote=True, host=None, username=None)


def test_meta_checksum_local_file(tmp_path: Path):
    """meta_checksum should calculate local checksum when file exists."""
    f = tmp_path / "f.txt"
    f.write_text("hello world")
    meta = {}
    result = sm.meta_checksum(meta, f, algorithm="md5")

    assert result["checksum"][0] == "md5"
    assert result["checksum"][1] == "5eb63bbbe01eeed093cb22bb8f5acdc3"
    assert result["location"] == str(f)


def test_meta_checksum_local_missing_file(tmp_path: Path):
    """meta_checksum should warn when file doesn't exist."""
    missing = tmp_path / "nofile.txt"
    meta = {}
    with warnings.catch_warnings(record=True) as w:
        result = sm.meta_checksum(meta, missing)
        assert len(w) == 1
        assert "not a file" in str(w[0].message)
        assert result == {}
