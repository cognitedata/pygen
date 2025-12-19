import difflib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling.ids import NodeId

from cognite_core.data_classes._core.constants import DEFAULT_QUERY_LIMIT


class FileContentAPI:
    def __init__(self, client: CogniteClient, get_node_ids: Callable[[int], list[NodeId]]) -> None:
        self._client = client
        self._get_node_ids = get_node_ids

    def download(
        self,
        directory: str | Path,
        keep_directory_structure: bool = False,
        resolve_duplicate_file_names: bool = False,
        files_limit: int = DEFAULT_QUERY_LIMIT,
    ) -> None:
        """`Download files. <https://developer.cognite.com/api#tag/Files/operation/downloadLinks>`_

        This method will stream all files to disk, never keeping more than 2MB in memory per worker.
        The files will be stored in the provided directory using the file name retrieved from the
        file metadata in CDF. You can also choose to keep the directory structure from CDF so that the files
        will be stored in subdirectories matching the directory attribute on the files. When missing,
        the (root) directory is used. By default, duplicate file names to the same local folder will be
        resolved by only keeping one of the files. You can choose to resolve this by appending a number to the
        file name using the resolve_duplicate_file_names argument.

        Warning:
            If you are downloading several files at once, be aware that file name collisions lead to all-but-one of
            the files missing. A warning is issued when this happens, listing the affected files.

        Args:
            directory (str | Path): Directory to download the file(s) to.
            keep_directory_structure (bool): Whether to keep the directory hierarchy in CDF,
                creating subdirectories as needed below the given directory.
            resolve_duplicate_file_names (bool): Whether to resolve duplicate file names by appending
                a number on duplicate file names
            files_limit (int): Maximum number of files to download. Defaults to 5.
        """
        node_ids = self._get_node_ids(files_limit)
        if not node_ids:
            return None
        self._client.files.download(
            directory=directory,
            instance_id=node_ids,
            keep_directory_structure=keep_directory_structure,
            resolve_duplicate_file_names=resolve_duplicate_file_names,
        )

    def __getattr__(self, item: str) -> Any:
        error_message = f"'{self.__class__.__name__}' object has no attribute '{item}'"
        attributes = [name for name in vars(self).keys() if not name.startswith("_")]
        if matches := difflib.get_close_matches(item, attributes):
            error_message += f". Did you mean one of: {matches}?"
        raise AttributeError(error_message)
