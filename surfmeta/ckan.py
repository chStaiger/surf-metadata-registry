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
        except Exception as e:
            raise HTTPError(f"Error retrieving dataset info: {e}")

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
        except Exception as e:
            raise HTTPError(f"Error creating dataset: {e}")

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
            response = self.api.action.package_search(rows=1000,
                                                      include_private=include_private)
            datasets = response.get("results", [])

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

        except Exception as e:
            raise HTTPError(f"Error listing datasets: {e}")

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
        except Exception as e:
            raise HTTPError(f"Error updating metadata: {e}")

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
        except Exception as e:
            raise HTTPError(f"Error uploading file: {e}")

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
