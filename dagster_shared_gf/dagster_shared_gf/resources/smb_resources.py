import os
from collections.abc import Iterator
from pathlib import PureWindowsPath
from typing import ClassVar, Dict, Literal

import smbclient
import smbclient.path as smb_path
from smbclient import _os as smb_os
from smbclient import _pool as smb_poll
from dagster import ConfigurableResource, EnvVar, InitResourceContext
from pydantic import dataclasses

from dagster_shared_gf.load_env_run import load_env_vars
from dagster_shared_gf.shared_functions import get_for_current_env

if (
    not os.environ.get("DAGSTER_DEV_DWH_FARINTER_IP")
    or not EnvVar("DAGSTER_ANALITICA_FARINTERNET_USERNAME").get_value()
):
    load_env_vars()

p_server_ip_dwh = get_for_current_env(
    {
        "dev": os.environ.get("DAGSTER_DEV_DWH_FARINTER_IP"),
        "prd": os.environ.get("DAGSTER_PRD_DWH_FARINTER_IP"),
    }
)


@dataclasses.dataclass(config={"arbitrary_types_allowed": True})
class SMBClientConfigCredentials:
    username: str
    password: EnvVar


all_credentials: Dict[str, SMBClientConfigCredentials] = {
    "analitica": SMBClientConfigCredentials(
        username=get_for_current_env(
            {"dev": os.environ.get("DAGSTER_ANALITICA_FARINTERNET_USERNAME")}
        ),
        password=get_for_current_env(
            {"dev": EnvVar("DAGSTER_SECRET_ANALITICA_FARINTERNET_PASSWORD")}
        ),
    ),
}

all_servers: dict[str, str] = {"NASGFTGU02": "10.0.4.157", "DWH": p_server_ip_dwh}


class SMBResource(ConfigurableResource):
    credentials: str
    server: str
    username: str
    password: str
    server_ip: str
    _is_initialized: bool | None = None
    _context: InitResourceContext | None = None
    SMBStatResult: ClassVar = smbclient.SMBStatResult
    SMBStatVolumeResult: ClassVar = smbclient.SMBStatVolumeResult
    path: ClassVar = smb_path
    """
    This module provides functions to check the existence and type of paths in an SMB (Server Message Block) file system. 
    It includes the following functions:

    - `exists(path, **kwargs)`: Returns True if the specified path refers to an existing path. Returns False for broken symbolic links or links pointing to unreachable locations.
    - `lexists(path, **kwargs)`: Returns True if the specified path refers to an existing path, including broken symbolic links or links pointing to unreachable locations.
    - `getatime(path, **kwargs)`: Returns the last access time of the specified path.
    - `getmtime(path, **kwargs)`: Returns the last modification time of the specified path.
    - `getctime(path, **kwargs)`: Returns the creation time of the specified path.
    - `getsize(path, **kwargs)`: Returns the size of the specified path in bytes.
    - `isfile(path, **kwargs)`: Returns True if the specified path is an existing regular file.
    - `isdir(path, **kwargs)`: Returns True if the specified path is an existing directory. This follows symbolic links, so both `islink()` and `isdir()` can be true for the same path.
    - `islink(path, **kwargs)`: Returns True if the specified path is a symbolic link.
    - `samefile(path1, path2, **kwargs)`: Returns True if both pathname arguments refer to the same file or directory. This is determined by the device number and i-node number and raises an exception if an `os.stat()` call on either pathname fails.

    All functions accept a `path` parameter, which is the path to check, and `kwargs`, which are common arguments used to build the SMB Session.
    """
    # Direct pass-through for smbclient functions using staticmethod
    client: ClassVar = smbclient
    """
    This module provides a variety of functions to interact with an SMB (Server Message Block) file system. 
    It includes the following commonly used functions:

    - `copyfile(src, dst, **kwargs)`: Copies a file from `src` to `dst`.
    - `listdir(path, **kwargs)`: Returns a list of entries in the directory given by `path`.
    - `open_file(path, mode='r', **kwargs)`: Opens a file at the given `path` with the specified `mode`.
    - `remove(path, **kwargs)`: Removes the file at the given `path`.
    - `rename(src, dst, **kwargs)`: Renames a file or directory from `src` to `dst`.
    - `rmdir(path, **kwargs)`: Removes the directory at the given `path`.
    - `stat(path, **kwargs)`: Performs a stat system call on the given `path`.
    - `symlink(src, dst, **kwargs)`: Creates a symbolic link pointing to `src` named `dst`.
    - `unlink(path, **kwargs)`: Removes (deletes) the file at the given `path`.
    - `walk(top, **kwargs)`: Generates the file names in a directory tree, starting at `top`.

    All functions accept a `path` parameter, which is the path to operate on, and `kwargs`, which are common arguments used to build the SMB Session.
    """
    def setup_for_execution(self, context: InitResourceContext):
        self._context = context
        self._is_initialized = True
        self.register_session()

    def log_event(self, type: Literal["info", "warning", "error"], message: str):
        if not hasattr(self, "_context") or self._context is None:
            raise ValueError(
                "Context has not been set. Call setup_for_execution first."
            )
        cl = self._context.log
        if cl:
            cl.info(message) if type == "info" else cl.warning(
                message
            ) if type == "warning" else cl.error(message)

    def register_session(self) -> smb_poll.Session:  # returns smbclient:
        """
        A function that creates and returns an smbclient using the already set username, password and server attributes.
        """

        if self.password is None or self.username is None:
            raise ValueError("Username and password must be set")
        return smbclient.register_session(
            server=self.server_ip, username=self.username, password=self.password
        )

    def get_full_server_path(self, directory: str | PureWindowsPath) -> PureWindowsPath:
        return PureWindowsPath(f"//{self.server_ip}").joinpath(directory)

    def get_server_dirs(
        self,
        directory: PureWindowsPath,
        recursive_depth: int | None = None,
        extension: str = ".xlsx",
        exclude: list[str] | None = None,
    ) -> Iterator[smbclient.SMBDirEntry]:
        """
        Retrieves a list of directories from the server share, filtered by the specified directory, recursive depth, file extension, and excluded files.

        Args:
            directory (PureWindowsPath): The directory to scan for files and subdirectories.
            recursive_depth (int | None): The maximum depth to recurse into subdirectories. Defaults to None.
            extension (str): The file extension to filter by. Defaults to ".xlsx".
            exclude (list[str] | None): A list of file names to exclude from the results. Defaults to None.

        Yields:
            Iterator[smbclient.SMBDirEntry]: A generator of SMB directory entries.
        """
        exclude = [x.lower() for x in exclude] if exclude else []

        def _scan_dir(
            current_dir: PureWindowsPath, current_depth: int
        ) -> Iterator[smb_os.SMBDirEntry]:
            """
            Recursively scans a directory and its subdirectories for files and directories on current server share, no server needed on path.

            Args:
                current_dir (PureWindowsPath): The current directory being scanned.
                current_depth (int): The current depth of the directory scan.

            Yields:
                Iterator[SMBDirEntry]: A generator of SMB directory entries.
            """
            # Generate the full path for the SMB directory
            directory_path = self.get_full_server_path(current_dir)

            # List all files and directories in the current directory
            for file_descriptor in self.client.scandir(directory_path):
                if file_descriptor.name.lower() in exclude:
                    continue

                # Ignore non-excel files or specific filenames
                if file_descriptor.name.lower().endswith(extension):
                    yield file_descriptor  # Yield the valid file

                # If the item is a directory and the depth limit hasn't been reached, recurse into it
                if file_descriptor.is_dir() and (
                    recursive_depth is None or current_depth < recursive_depth
                ):
                    # Recursively scan subdirectories
                    yield from _scan_dir(
                        current_dir.joinpath(file_descriptor.name), current_depth + 1
                    )

        # Start scanning the directory from depth 0
        return _scan_dir(directory, 0)

    def move_server_file(self, file_path: PureWindowsPath, new_path: PureWindowsPath):
        """
        Moves a file from one current server share location to another.

        Args:
            file_path (PureWindowsPath): The path of the file to be moved, no server needed.
            new_path (PureWindowsPath): The new path where the file will be moved, no server needed.

        Returns:
            None
        """

        def get_unique_dst_path(dst_path: PureWindowsPath):
            counter = 1
            new_dst_path = dst_path

            # Check if file already exists
            while self.path.exists(new_dst_path):
                new_dst_path = dst_path.with_name(
                    f"{dst_path.stem}_{counter}{dst_path.suffix}"
                )
                counter += 1

            return new_dst_path

        file_path = self.get_full_server_path(file_path)
        new_path = self.get_full_server_path(new_path)
        # if exists add a number
        new_path = get_unique_dst_path(new_path)
        self.log_event(
            type="info",
            message=f"Moving {str(file_path.as_posix())} to {str(new_path.as_posix())}",
        )
        self.client.renames(file_path.as_posix(), new_path.as_posix())

    class SMBDirEntry(smb_os.SMBDirEntry):
        pass

    class SMBDirEntryInformation(smb_os.SMBDirEntryInformation):
        pass




smb_resource_analitica_nasgftgu02 = SMBResource(
    credentials="analitica",
    server="NASGFTGU02",
    username=all_credentials["analitica"].username,
    password=all_credentials["analitica"].password,
    server_ip=all_servers["NASGFTGU02"],
)

smb_resource_staging_dagster_dwh = SMBResource(
    credentials="analitica",
    server="DWH",
    username=all_credentials["analitica"].username,
    password=all_credentials["analitica"].password,
    server_ip=all_servers["DWH"],
)

smb_resource_independent_dagster_dwh = SMBResource(
    credentials="analitica",
    server="DWH",
    username=all_credentials["analitica"].username,
    password=all_credentials["analitica"].password.get_value(),
    server_ip=all_servers["DWH"],
)

if __name__ == "__main__":
    load_env_vars()
    print(smb_resource_analitica_nasgftgu02.username)
    smb_resource_independent_dagster_dwh.register_session()

    print(
        smb_resource_independent_dagster_dwh.client.listdir(
            f"\\{all_servers["DWH"]}\staging_dagster"
        )
    )
