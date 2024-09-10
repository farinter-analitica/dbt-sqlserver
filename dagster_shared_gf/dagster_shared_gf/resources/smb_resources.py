from collections.abc import Iterator
from pathlib import PurePath
from dagster import ConfigurableResource, EnvVar, InitResourceContext, asset, Definitions
from pydantic import Field, dataclasses, PrivateAttr
from typing import ClassVar, List, Literal, Generator, Any, Dict

import os, base64, contextlib, pyodbc, smbclient
import smbclient.path as smb_path
from dagster_shared_gf import shared_variables as shared_vars
from dagster_shared_gf.shared_functions import get_for_current_env
from dagster_shared_gf.load_env_run import load_env_vars

if not EnvVar("DAGSTER_ANALITICA_FARINTERNET_USERNAME").get_value():
    load_env_vars()

@dataclasses.dataclass(config={"arbitrary_types_allowed": True})
class SMBClientConfigCredentials:
    username: str 
    password: EnvVar

all_credentials: Dict[str, SMBClientConfigCredentials] = \
    {
        "analitica": SMBClientConfigCredentials(
            username=get_for_current_env(
                {"dev": os.environ.get("DAGSTER_ANALITICA_FARINTERNET_USERNAME")}
            ),
            password=get_for_current_env(
                {"dev": EnvVar("DAGSTER_SECRET_ANALITICA_FARINTERNET_PASSWORD")}
            ),
        )
    }

all_servers: dict[str, str] = {"NASGFTGU02": "10.0.4.157"}



class SMBResource(ConfigurableResource):
    credentials: str
    server: str
    username: str 
    password: str
    server_ip: str 
    _is_initialized: bool = None
    _context: InitResourceContext = None
    SMBStatResult: ClassVar = smbclient.SMBStatResult
    SMBStatVolumeResult: ClassVar = smbclient.SMBStatVolumeResult
    def setup_for_execution(self, context: InitResourceContext):
        self._context = context
        self._is_initialized = True
        self.register_session()

    def log_event(self, type: Literal['info', 'warning', 'error'], message: str):
        if not hasattr(self, '_context') or self._context is None:
            raise ValueError("Context has not been set. Call setup_for_execution first.")
        with self._context.log as cl:
            cl.info(message) if type == "info" else cl.warning(message) if type == "warning" else cl.error(message)

    def register_session(self) -> None: # returns smbclient:
        """
        A function that creates and returns an smbclient using the already set username, password and server attributes.
        """

        if self.password is None or self.username is None:
            raise ValueError("Username and password must be set")
        smbclient.register_session(server=self.server_ip
                                                   , username=self.username
                                                   , password=self.password
                                                   )
    def get_server_dirs(self, directory: PurePath, recursive_depth: int | None = None, extension: str = ".xlsx", exclude: list[str] | None = None) -> Iterator[smbclient.SMBDirEntry]:
        def _scan_dir(current_dir: PurePath, current_depth: int) -> Iterator[SMBResource.SMBDirEntry]:
            """
            Recursively scans a directory and its subdirectories for files and directories on current server share, no server needed on path.

            Args:
                current_dir (PurePath): The current directory being scanned.
                current_depth (int): The current depth of the directory scan.

            Yields:
                Iterator[SMBResource.SMBDirEntry]: A generator of SMB directory entries.
            """
            # Generate the full path for the SMB directory
            directory_path = PurePath(f"//{self.server_ip}").joinpath(current_dir)
            
            # List all files and directories in the current directory
            for file_descriptor in self.scandir(directory_path):
                # Ignore non-excel files or specific filenames
                if file_descriptor.name.lower().endswith(extension) and file_descriptor.name.lower() not in [x.lower() for x in exclude]:
                    yield file_descriptor  # Yield the valid file
                
                # If the item is a directory and the depth limit hasn't been reached, recurse into it
                if file_descriptor.is_dir() and (recursive_depth is None or current_depth < recursive_depth):
                    # Recursively scan subdirectories
                    yield from _scan_dir(current_dir.joinpath(file_descriptor.name), current_depth + 1)

        # Start scanning the directory from depth 0
        return _scan_dir(directory, 0)
    
    def move_server_file(self, file_path: PurePath, new_path: PurePath):
        """
        Moves a file from one current server share location to another.

        Args:
            file_path (PurePath): The path of the file to be moved, no server needed.
            new_path (PurePath): The new path where the file will be moved, no server needed.

        Returns:
            None
        """
        def get_unique_dst_path(dst_path: PurePath):
            counter = 1
            new_dst_path = dst_path
            
            # Check if file already exists
            while self.path.exists(new_dst_path):
                new_dst_path = dst_path.with_name(f"{dst_path.stem}_{counter}{dst_path.suffix}")
                counter += 1
                
            return new_dst_path
        file_path = PurePath(f"//{self.server_ip}").joinpath(file_path)
        new_path = PurePath(f"//{self.server_ip}").joinpath(new_path)
        #if exists add a number
        new_path = get_unique_dst_path(new_path)
        self.log_event(type="info", message=f"Moving {str(file_path.as_posix())} to {str(new_path.as_posix())}")
        self.renames(file_path.as_posix(),new_path.as_posix())
    
    def open_server_file(self, file_path: PurePath
                , mode: str="rb"):
        """
        A function to open a file using the provided file path, SMB resource, and mode.
        
        Args:
            file_path (Path): The path to the file to be opened no server needed.
            mode (str, optional): The mode in which the file should be opened. Defaults to "rb".
            Open Modes:
                        'r': Open for reading (default).
                        'w': Open for writing, truncating the file first.
                        'x': Open for exclusive creation, failing if the file already exists.
                        'a': Open for writing, appending to the end of the file if it exists.
                        '+': Open for updating (reading and writing), can be used in conjunction with any of the above. Open Type - can be specified with the OpenMode
                        't': Text mode (default).
                        'b': Binary mode.
        Returns:
            The opened file using the specified mode.
        """
        file_path = PurePath(f"//{self.server_ip}").joinpath(file_path)
        return self.open_file(path=file_path, mode=mode)


    # Direct pass-through for smbclient functions using staticmethod
    copyfile = staticmethod(smbclient.copyfile)
    getxattr = staticmethod(smbclient.getxattr)
    link = staticmethod(smbclient.link)
    listdir = staticmethod(smbclient.listdir)
    listxattr = staticmethod(smbclient.listxattr)
    lstat = staticmethod(smbclient.lstat)
    makedirs = staticmethod(smbclient.makedirs)
    mkdir = staticmethod(smbclient.mkdir)
    open_file = staticmethod(smbclient.open_file)
    readlink = staticmethod(smbclient.readlink)
    remove = staticmethod(smbclient.remove)
    removedirs = staticmethod(smbclient.removedirs)
    removexattr = staticmethod(smbclient.removexattr)
    rename = staticmethod(smbclient.rename)
    renames = staticmethod(smbclient.renames)
    replace = staticmethod(smbclient.replace)
    rmdir = staticmethod(smbclient.rmdir)
    scandir = staticmethod(smbclient.scandir)
    setxattr = staticmethod(smbclient.setxattr)
    stat = staticmethod(smbclient.stat)
    stat_volume = staticmethod(smbclient.stat_volume)
    symlink = staticmethod(smbclient.symlink)
    truncate = staticmethod(smbclient.truncate)
    unlink = staticmethod(smbclient.unlink)
    utime = staticmethod(smbclient.utime)
    walk = staticmethod(smbclient.walk)
    class SMBDirEntry(smbclient.SMBDirEntry):
        pass
    class SMBDirEntryInformation(smbclient.SMBDirEntryInformation):
        pass
    class path:
        exists = staticmethod(smb_path.exists)
        lexists = staticmethod(smb_path.lexists)
        getatime = staticmethod(smb_path.getatime)
        getmtime = staticmethod(smb_path.getmtime)
        getctime = staticmethod(smb_path.getctime)
        getsize = staticmethod(smb_path.getsize)
        isfile = staticmethod(smb_path.isfile)
        isdir = staticmethod(smb_path.isdir)
        islink = staticmethod(smb_path.islink)
        samefile = staticmethod(smb_path.samefile)




smb_resource_analitica_nasgftgu02 = SMBResource(
    credentials= "analitica",
    server="NASGFTGU02",
    username= all_credentials["analitica"].username,
    password= all_credentials["analitica"].password,
    server_ip= all_servers["NASGFTGU02"],
)

if __name__ == "__main__":
    load_env_vars()
    print(smb_resource_analitica_nasgftgu02.username)	
    print(smb_resource_analitica_nasgftgu02.register_session().listdir(r"\\10.0.4.157\data_repo\grupo_farinter\presupuesto_ventas_finanzas"))