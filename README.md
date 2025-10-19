# surf-metadata-registry

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
