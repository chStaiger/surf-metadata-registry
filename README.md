# surf-metadata-registry
[![Python package](https://github.com/chStaiger/surf-metadata-registry/actions/workflows/linter.yml/badge.svg)](https://github.com/chStaiger/surf-metadata-registry/actions/workflows/linter.yml)

This package installs a client with which you can create some descriptive metadata for data files and folders you stored on SURF data and compute infrastrcuture.
Currently implemented SURF systems:
- Snellius
- Spider

# Dpendencies

- Requires python 3.13 or higher because of the module `ckanapi`.
- `ckanapi`

# Installation

## Install a git branch

### pip

```
pip install git+https://github.com/chStaiger/surf-metadata-registry.git@<branch>

# or

git clone https://github.com/chStaiger/surf-metadata-registry.git
git checkout <branch>
cd surf-metadata-registry
pip install -e .

surfmeta
````

### uv

```
uv install git+https://github.com/chStaiger/surf-metadata-registry.git@<branch>

# or

git clone https://github.com/chStaiger/surf-metadata-registry.git
git checkout <branch>
cd surf-metadata-registry
uv build

uv run surfmeta
```
You should receive the help fpr the command:

```
usage: surfmeta [-h] {ckan,create,create-meta-file,md-list,md-search} ...

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

positional arguments:
  {ckan,create,create-meta-file,md-list,md-search}
    ckan                CKAN configuration commands
    create              Create a new metadata entry interactively in CKAN
    create-meta-file    Interactively create a JSON metadata file
    md-list             List metadata entries from CKAN. Without arguments:
                        show all entries (name + UUID). With <uuid>: show
                        metadata of that entry.
    md-search           Search CKAN datasets

options:
  -h, --help            show this help message and exit
```

# Usage

Before we start we need to configure the access toa metadata store (CKAN). Please go through the section *Configuration*.

## Configuration
To configure a CKAN connection you first need to create an alias:

```
surfmeta alias your_alias https://your.ckan.url
```

To activate it with a token, please generate one under your CKAN user in the webinterface and then do:

```
surfmeta init alias_or_url
```
You will be asked to copy your token, Note, it will not be shown!


## Create a minimal metadata entry for a file on your computer
When you have just started it might be that there no metadata entries for you available yet:
```
surfmeta md-list
⚠️ No datasets found on this CKAN instance.
```

So let us create a new entry with the minimal necessary metadata:

```
surfmeta create ../../my_books/AdventuresSherlockHolmes.txt

Dataset name: Sherlock Holmes
Author name: A Conan Doyle

📂 Available Organisations:
  1) book-club
Select an organisation by number: 1
Do you want to add the dataset to a group? [y/N]: y

📁 Available Groups:
  1) analysis-data
Select a group by number: 1
🆔 UUID: f05ef194-4e14-4e01-98eb-253fd0784456
🌐 Name: Sherlock Holmes
✅ Dataset created successfully!
```

You will always have to choose an organisation under which the metadata is created. Everyone in the organisation can see the entry.
Now let us see how to create a metadata entry wioth some more information.

## Create a metadata file

In the example below we prepare the metadata entry for the whole folder `my_books`:

```
surfmeta create-meta-file annotations_my_books.json

Add Prov-O metadata (leave blank to skip any field):
prov:wasGeneratedBy: Christine
prov:wasDerivedFrom: Project Gutenberg
prov:startedAtTime:
prov:endedAtTime:
prov:actedOnBehalfOf:
prov:SoftwareAgent: wget

Add your own metadata (key-value pairs). Type 'done' as key to finish.
Key: data_type
Value: training data
Key: done

Metadata saved to: /Users/christine/git-repos/surf-metadata-registry/annotations_my_books.json
```
The metadata is stored in a file called `annotations_my_books.json` and we can upload it via the `create` command:

```
surfmeta create ../../my_books --metafile annotations_my_books.json

Dataset name: Book collection
Author name:

📂 Available Organisations:
  1) book-club
Select an organisation by number: 1
Do you want to add the dataset to a group? [y/N]:
🆔 UUID: d24468e0-e708-41ba-ace4-cc11cecafc15
🌐 Name: Book collection
✅ Dataset created successfully!
```

## Spider and Snellius
If you want to create some metadata for spider or snellius, please install the client on the infrastructure and use as above.
Tje metadata will be automatically extended wirth system information that enables you to download the files and folders for which you created the metadata.

## Finding metadata entries

To find and explore metadata entries we have the commands `surfmeta search` and `surfmeta list`. I can find all data which are on my local computer:

```
surfmeta md-search -k local
Found 2 datasets:

Title            UUID                                  Organization  Groups
---------------------------------------------------------------------------
Sherlock Holmes  1fd0e173-8873-4619-ab38-bfda5a47a8cc  book-club  analysis-data
Book collection  d24468e0-e708-41ba-ace4-cc11cecafc15  book-club  <no groups>
```

To further onspect the metadata of the Sherlock Holmes files use the UUID in the command `md-list`:

```
surfmeta md-list 1fd0e173-8873-4619-ab38-bfda5a47a8cc

Metadata for dataset: Sherlock Holmes (UUID: 1fd0e173-8873-4619-ab38-bfda5a47a8cc)

Organization: book-club
Groups      : analysis-data

System Metadata:
  server        : local
  uuid          : 1fd0e173-8873-4619-ab38-bfda5a47a8cc

User Metadata:
  checksum      : ["md5", "18bc425382bced34c158ae7e2fa3dd88"]
  location      : /Users/christine/my_books/AdventuresSherlockHolmes.txt
```

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
