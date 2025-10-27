# surf-metadata-registry
[![Python package](https://github.com/chStaiger/surf-metadata-registry/actions/workflows/linter.yml/badge.svg)](https://github.com/chStaiger/surf-metadata-registry/actions/workflows/linter.yml)
# Dpendencies

- Requires python 3.13 or higher because of the module `ckanapi`.
- `ckanapi`

# Installation

## Install a git branch

### pip

```
pip install git+https://github.com/chStaiger/surf-metadata-registry.git@<branch>
surfmeta
````

### uv

```
uv install git+https://github.com/chStaiger/surf-metadata-registry.git@<branch>
uv run surfmeta
```

## Checkout code and install locally

```
git clone https://github.com/chStaiger/surf-metadata-registry.git
cd surf-metadata-registry
```

### pip

```
pip install -e .
surfmeta
```

### uv

```
uv build
uv run surfmeta
```

# Configuration
To configure a CKAN connection you first need to create an alias:

```
surfmeta alias your_alias https://your.ckan.url
```

To activate it with a token, please generate one under your CKAN user in the webinterface and then do:

```
surfmeta init alias_or_url
```
You will be asked to copy your token, Note, it will not be shown!

# Usage

```
Example usage:
    surfmeta ckan list
    surfmeta ckan switch myalias
    surfmeta ckan init mytoken
    surfmeta ckan remove demo
    surfmeta ckan alias myalias https://demo.ckan.org
    surfmeta ckan orgs
    surfmeta ckan groups

    surfmeta create
```

# Additional configuration

# Known Issues

## The spider platform
The spider platform does not provide version 3.13 or higher for python.
You can still use the software and run it in a virtual environment. We will use `uv` in the eample below:

1. Clone the code repository

```
git clone https://github.com/chStaiger/surf-metadata-registry.git
cd surf-metadata-registry/
```

2. Create a python3.13 environment

```
uv venv --python 3.13
source .venv/bin/activate
```

3. Build the software package and run it

```
uv build
uv run surfmeta
```

All `surfmeta` commands need to be run through `uv run`.
