"""CKAN functionality for creating and managing datasets."""

from pathlib import Path
from typing import Optional

from ckanapi import NotAuthorized, NotFound, RemoteCKAN, ValidationError
from httpx import HTTPError


class Ckan:
    """A utility class to interact with a CKAN instance using its API.

    This class supports authentication, dataset management, and file operations.
    """

    def __init__(self, url: str, token: str):
        """Initialise the CKAN instance.

        Parameters
        ----------
        url : str
            The base URL of the CKAN instance.
        token : str
            The CKAN API token for authentication.

        Raises
        ------
        ValueError
            If the token is not provided.
        NotAuthorized
            If the token is invalid or does not authenticate successfully.

        """
        if not token:
            raise ValueError("CKAN API token cannot be empty.")

        self.ckan_url = url.rstrip("/")
        self.ckan_token = token
        self.api = RemoteCKAN(self.ckan_url, apikey=self.ckan_token)

        # Quick authentication check
        if not self.user_authenticated():
            raise NotAuthorized("CKAN and API token do not match.")

    def user_authenticated(self) -> bool:
        """Check whether the API token is valid.

        Returns
        -------
        bool
            True if authenticated successfully, False otherwise.

        """
        try:
            self.api.action.user_show()
            return True
        except NotAuthorized:
            return False

    def dataset_exists(self, dataset_id: str) -> bool:
        """Check whether a dataset exists.

        Parameters
        ----------
        dataset_id : str
            The dataset name or ID.

        Returns
        -------
        bool
            True if the dataset exists, False otherwise.

        """
        try:
            self.api.action.package_show(id=dataset_id)
            return True
        except NotFound:
            return False

    def get_dataset_info(self, dataset_id: str) -> dict:
        """Retrieve dataset metadata and information.

        Parameters
        ----------
        dataset_id : str
            The dataset name or ID.

        Returns
        -------
        dict
            The dataset information.

        Raises
        ------
        NotFound
            If the dataset is not found.
        HTTPError
            For other HTTP errors.

        """
        try:
            return self.api.action.package_show(id=dataset_id)
        except NotFound as e:
            raise e
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise HTTPError(f"Error retrieving dataset info: {e}") from e

    def create_dataset(self, metadata: dict, verbose: bool = False) -> dict:
        """Create a new dataset in CKAN.

        Parameters
        ----------
        metadata : dict
            A dictionary containing dataset metadata (e.g., name, title, author, etc.)
        verbose : bool, optional
            If True, prints the created dataset info.

        Returns
        -------
        dict
            The API response containing the created dataset info.

        Raises
        ------
        ValidationError
            If the metadata is invalid.
        HTTPError
            For other HTTP errors.

        """
        if not metadata or "name" not in metadata:
            raise ValueError("Metadata must include at least a dataset 'name'.")

        try:
            response = self.api.action.package_create(**metadata)
            if verbose:
                print("Dataset created:", response)
            return response
        except ValidationError as e:
            raise e
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise HTTPError(f"Error creating dataset: {e}") from e

    def list_all_datasets(self, include_private: bool = False) -> list:
        """List all datasets available in the CKAN instance.

        Parameters
        ----------
        include_private : bool, optional
            Whether to include private datasets. Defaults to False.

        Returns
        -------
        list
            A list of max 1000 dataset metadata dictionaries.

        Raises
        ------
        HTTPError
            If the API call fails.

        """
        try:
            response = self.api.action.package_search(rows=1000, include_private=include_private)
            datasets = response.get("results", [])
            search_params = {}
            # If limit is higher than 1000, paginate
            limit = 1000
            if limit and limit > 1000:
                start = len(datasets)
                while start < limit:
                    search_params["start"] = start
                    search_params["rows"] = min(limit - start, 1000)
                    resp = self.api.action.package_search(**search_params)
                    results = resp.get("results", [])
                    if not results:
                        break
                    datasets.extend(results)
                    start += len(results)

            return datasets

        except Exception as e:  # pylint: disable=broad-exception-caught
            raise HTTPError(f"Error listing datasets: {e}") from e

    def add_meta_to_dataset(self, dataset_id: str, metadata: dict, verbose: bool = False) -> dict:
        """Add or update metadata fields for an existing dataset.

        Parameters
        ----------
        dataset_id : str
            The dataset name or ID to update.
        metadata : dict
            A dictionary containing the metadata fields to add or update.
            Example: {"author": "John Doe", "notes": "Updated description"}
        verbose : bool, optional
            If True, prints the updated dataset info.

        Returns
        -------
        dict
            The updated dataset metadata returned by the CKAN API.

        Raises
        ------
        NotFound
            If the dataset does not exist.
        ValidationError
            If the metadata is invalid.
        HTTPError
            For other HTTP errors.

        """
        if not metadata or not isinstance(metadata, dict):
            raise ValueError("Metadata must be a non-empty dictionary.")

        # Ensure dataset exists before attempting to patch
        if not self.dataset_exists(dataset_id):
            raise NotFound(f"Dataset '{dataset_id}' not found.")

        try:
            # `package_patch` requires the dataset ID to be specified
            payload = {"id": dataset_id}
            payload.update(metadata)

            response = self.api.action.package_patch(**payload)

            if verbose:
                print(f"Metadata updated for dataset '{dataset_id}':", response)

            return response

        except ValidationError as e:
            raise e
        except NotFound as e:
            raise e
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise HTTPError(f"Error updating metadata: {e}") from e

    def add_datafile_to_dataset(self, dataset_id: str, file_path: Path, verbose: bool = True) -> dict:
        """Upload a file to a specific dataset as a resource.

        Parameters
        ----------
        dataset_id : str
            The dataset name or ID to which the file will be uploaded.
        file_path : Path
            The local file path to upload.
        verbose : bool, optional
            If True, prints additional information.

        Returns
        -------
        dict
            The API response containing the uploaded resource info.

        Raises
        ------
        HTTPError
            If the upload fails.

        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            with open(file_path, "rb") as f:
                response = self.api.action.resource_create(
                    package_id=dataset_id, name=file_path.name, upload=f
                )
            if verbose:
                print(f"File '{file_path.name}' uploaded to dataset '{dataset_id}'.")
            return response
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise HTTPError(f"Error uploading file: {e}") from e

    def get_checksum_by_filename(self, dataset_id: str, target_label: str) -> Optional[str]:
        """Retrieve the checksum of a specific file by its name.

        Parameters
        ----------
        dataset_id : str
            The dataset name or ID.
        target_label : str
            The file name (resource name).

        Returns
        -------
        str or None
            The checksum if found, None otherwise.

        """
        info = self.get_dataset_info(dataset_id)
        resources = info.get("resources", [])
        for resource in resources:
            if resource.get("name") == target_label:
                return resource.get("hash")
        return None

    def list_dataset_resources(self, dataset_id: str) -> list:
        """List all resources (files) attached to a dataset.

        Parameters
        ----------
        dataset_id : str
            The dataset name or ID.

        Returns
        -------
        list
            A list of resource metadata dictionaries.

        """
        info = self.get_dataset_info(dataset_id)
        return info.get("resources", [])

    def list_organisations(self, include_extras: bool = False) -> list:
        """List all organizations visible to the authenticated user.

        Parameters
        ----------
        include_extras : bool, optional
            If True, returns full metadata for each organization using organization_show.
            If False, returns only the list of organization names.

        Returns
        -------
        list
            A list of organization names or full metadata dictionaries.

        Raises
        ------
        HTTPError
            If the API call fails.

        """
        try:
            orgs = self.api.action.organization_list()
            if include_extras:
                orgs_full = []
                for org_name in orgs:
                    try:
                        org_info = self.api.action.organization_show(id=org_name)
                        orgs_full.append(org_info)
                    except Exception as e:  # pylint: disable=broad-exception-caught
                        print(f"⚠️ Could not retrieve info for org '{org_name}': {e}")
                return orgs_full
            return orgs
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise HTTPError(f"Error listing organizations: {e}") from e

    def list_groups(self, include_extras: bool = False) -> list:
        """List all groups available in the CKAN instance.

        Parameters
        ----------
        include_extras : bool, optional
            If True, returns full metadata for each group using group_show.
            If False, returns only the list of group names.

        Returns
        -------
        list
            A list of group names or full metadata dictionaries.

        Raises
        ------
        HTTPError
            If the API call fails.

        """
        try:
            groups = self.api.action.group_list()
            if include_extras:
                groups_full = []
                for group_name in groups:
                    try:
                        group_info = self.api.action.group_show(id=group_name)
                        groups_full.append(group_info)
                    except Exception as e:  # pylint: disable=broad-exception-caught
                        print(f"⚠️ Could not retrieve info for group '{group_name}': {e}")
                return groups_full
            return groups
        except Exception as e:
            raise HTTPError(f"Error listing groups: {e}") from e

    def update_dataset(self, dataset_dict: dict):
        """Update an existing CKAN dataset.

        Parameters
        ----------
        dataset_dict : dict
            A full dataset dictionary, including 'name' or 'id' and updated fields.
            Typically this includes an updated 'extras' list.

        Returns
        -------
        dict
            The updated dataset as returned by CKAN.

        Raises
        ------
        ValidationError
            If CKAN rejects the update due to schema or validation issues.
        NotFound
            If the dataset does not exist.
        Exception
            For other unexpected errors.

        """
        try:
            updated = self.api.action.package_update(**dataset_dict)
            return updated
        except ValidationError:
            raise
        except NotFound:
            raise
        except Exception as e:
            raise RuntimeError(f"Unexpected error while updating dataset: {e}") from e
