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
        "extras": [
            {"key": "algorithm", "value": "RandomForest"},
            {"key": "system_name", "value": "localhost"}
        ],
    },
    {
        "title": "Another Dataset",
        "name": "dataset2",
        "organization": {"name": "org2"},
        "groups": [],
        "extras": [
            {"key": "prov:SoftwareAgent", "value": "AgentX"},
            {"key": "system_name", "value": "remote"}
        ],
    },
    {
        "title": "Third Dataset",
        "name": "dataset3",
        "organization": {"name": "org1"},
        "groups": [{"name": "group2"}],
        "extras": [
            {"key": "nested", "value": json.dumps({"a": [1, 2]})},
            {"key": "system_name", "value": "local"}
        ],
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

def test_dataset_matches_combined():
    ds = MOCK_DATASETS[0]
    assert _dataset_matches(ds, keywords=["randomforest"], org_filter="org1", group_filter="group1") is True
    assert _dataset_matches(ds, keywords=["randomforest"], org_filter="org2", group_filter="group1") is False

# -----------------------------
# Tests for search_datasets
# -----------------------------
def test_search_datasets_keyword():
    result = search_datasets(MOCK_DATASETS, keyword=["randomforest"])
    assert len(result) == 1
    assert result[0]["name"] == "dataset1"

def test_search_datasets_org():
    result = search_datasets(MOCK_DATASETS, org="org1")
    assert len(result) == 2

def test_search_datasets_group():
    result = search_datasets(MOCK_DATASETS, group="group2")
    assert len(result) == 1
    assert result[0]["name"] == "dataset3"

# -----------------------------
# Tests for _print_dataset_results
# -----------------------------
def test_print_dataset_results(capsys):
    print_dataset_results(MOCK_DATASETS[:2])
    captured = capsys.readouterr().out
    assert "Test Dataset 1" in captured
    assert "Another Dataset" in captured
    assert "Org: org1" not in captured  # We don’t use Org label inside print_dataset_results, it's direct
