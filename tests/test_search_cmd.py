import pytest
import json
from io import StringIO
from surfmeta.cli_handlers import handle_md_search
from surfmeta.search_utils import print_dataset_results, _dataset_matches, search_datasets
from surfmeta.metadata_utils import normalize_extras_for_search, _flatten_value_for_search


# Sample mock datasets
MOCK_DATASETS = [
    {
        "title": "Test Dataset 1",
        "name": "dataset1",
        "organization": {"name": "org1"},
        "groups": [{"name": "group1"}],
        "extras": [{"key": "system_name", "value": "systemA"},
                   {"key": "Algorithm", "value": "RandomForest"}],
    },
    {
        "title": "Another Dataset",
        "name": "dataset2",
        "organization": {"name": "org2"},
        "groups": [],
        "extras": [{"key": "prov:SoftwareAgent", "value": "AgentX"}],
    },
    {
        "title": "Third Dataset",
        "name": "dataset3",
        "organization": {"name": "org1"},
        "groups": [{"name": "group2"}],
        "extras": [{"key": "system_name", "value": "systemB"}],
    },
    {
        "title": "Local Dataset",
        "name": "dataset4",
        "organization": {"name": "org3"},
        "groups": [{"name": "group3"}],
        "extras": [],
    },
    {
        "title": "Test Dataset X",
        "name": "datasetX",
        "organization": {"name": "orgX"},
        "groups": [{"name": "group1"}],
        "extras": [{"key": "system_name", "value": "systemA"},
                   {"key": "Algorithm", "value": "RandomForest"}],
    },
]

# -----------------------------
# Tests for _flatten_value_for_search
# -----------------------------
def test_flatten_value_for_search_str():
    assert _flatten_value_for_search("hello") == ["hello"]

def test_flatten_value_for_search_list():
    assert _flatten_value_for_search(["a", "B"]) == ["a", "b"]

def test_flatten_value_for_search_dict():
    d = {"key1": "value1", "key2": ["x", "y"]}
    result = _flatten_value_for_search(d)
    assert "key1" in result
    assert "value1" in result
    assert "key2" in result
    assert "x" in result
    assert "y" in result

# -----------------------------
# Tests for normalize_extras_for_search
# -----------------------------
def test_normalize_extras_for_search():
    extras = [
        {"key": "Algorithm", "value": "RandomForest"},
        {"key": "Params", "value": json.dumps({"n_estimators": 100})},
    ]
    result = normalize_extras_for_search(extras)
    assert "algorithm" in result
    assert "randomforest" in result
    assert "params" in result
    assert "n_estimators" in result
    assert "100" in result

# -----------------------------
# Tests for _dataset_matches
# -----------------------------
def test_dataset_matches_keyword_only():
    ds = MOCK_DATASETS[0]
    assert _dataset_matches(ds, keywords=["randomforest"]) is True
    assert _dataset_matches(ds, keywords=["nonexistent"]) is False

def test_dataset_matches_org_only():
    ds = MOCK_DATASETS[1]
    assert _dataset_matches(ds, org_filter="org2") is True
    assert _dataset_matches(ds, org_filter="org1") is False

def test_dataset_matches_group_only():
    ds = MOCK_DATASETS[0]
    assert _dataset_matches(ds, group_filter="group1") is True
    assert _dataset_matches(ds, group_filter="group2") is False

# -----------------------------
# Tests for _dataset_matches with system filter
# -----------------------------
def test_dataset_matches_system_name():
    # Dataset with system_name in extras
    ds = MOCK_DATASETS[0]
    print(ds)
    assert _dataset_matches(ds, system_filter="systema") is True  # systemA should match
    assert _dataset_matches(ds, system_filter="systemB") is False  # systemB should not match

    # Dataset with a different system_name in extras
    ds = MOCK_DATASETS[2]
    assert _dataset_matches(ds, system_filter="systemB") is True  # systemB should match
    assert _dataset_matches(ds, system_filter="systemA") is False  # systemA should not match

    # Dataset without a system_name, should match local
    ds = MOCK_DATASETS[3]
    assert _dataset_matches(ds, system_filter="local") is True  # Should match "local"
    assert _dataset_matches(ds, system_filter="localhost") is True  # Should match "localhost"
    assert _dataset_matches(ds, system_filter="systemA") is False  # systemA should not match

    # Dataset without a system_name, should not match if not local or localhost
    ds = MOCK_DATASETS[3]
    assert _dataset_matches(ds, system_filter="systemA") is False  # systemA should not match


def test_dataset_matches_combined():
    ds = MOCK_DATASETS[0]
    assert _dataset_matches(ds, keywords=["randomforest"], org_filter="org1", group_filter="group1", system_filter="systemA") is True
    assert _dataset_matches(ds, keywords=["randomforest"], org_filter="org2", group_filter="group1", system_filter="systemA") is False

# -----------------------------
# Tests for search_datasets
# -----------------------------
def test_search_datasets_keyword():
    result = search_datasets(MOCK_DATASETS, keyword=["randomforest"])
    assert len(result) == 2
    assert result[0]["name"] == "dataset1"

def test_search_datasets_org():
    result = search_datasets(MOCK_DATASETS, org="org1")
    assert len(result) == 2

def test_search_datasets_group():
    result = search_datasets(MOCK_DATASETS, group="group2")
    assert len(result) == 1
    assert result[0]["name"] == "dataset3"

def test_search_datasets_system():
    result = search_datasets(MOCK_DATASETS, system="systemA")
    assert len(result) == 2  # dataset1 and dataset3 should match systemA
    assert "dataset1" in [ds["name"] for ds in result]
    assert "datasetX" in [ds["name"] for ds in result]

# -----------------------------
# Tests for _print_dataset_results
# -----------------------------
def test_print_dataset_results(capsys):
    print_dataset_results(MOCK_DATASETS[:2])
    captured = capsys.readouterr().out
    assert "Test Dataset 1" in captured
    assert "Another Dataset" in captured
    assert "Org: org1" not in captured  # We don’t use Org label inside print_dataset_results, it's direct

# -----------------------------
# Tests for handle_md_search
# -----------------------------
class MockCKANConn:
    def list_all_datasets(self, include_private=True):
        return MOCK_DATASETS

@pytest.mark.parametrize("args, expected_count", [
    ({"keyword": ["randomforest"], "org": None, "group": None, "system_name": None}, 2),
    ({"keyword": None, "org": "org1", "group": None, "system_name": None}, 2),
    ({"keyword": None, "org": None, "group": "group2", "system_name": None}, 1),
    ({"keyword": None, "org": None, "group": None, "system_name": "systemA"}, 2),
])
def test_handle_md_search(capsys, args, expected_count):
    mock_conn = MockCKANConn()
    class Args:
        keyword = args["keyword"]
        org = args["org"]
        group = args["group"]
        system = args["system_name"]  # Include system_name in the test arguments

    handle_md_search(mock_conn, Args)
    captured = capsys.readouterr().out
    assert f"Found {expected_count} datasets" in captured

