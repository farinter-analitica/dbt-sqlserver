from dagster import ConfigurableResource, EnvVar, InitResourceContext, asset, Definitions
from pydantic import Field, dataclasses, PrivateAttr
from typing import ClassVar, List, Literal, Generator, Any, Dict

import os, base64, contextlib, pyodbc, smbclient
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

# import smbclient.path
# from smbclient._io import SEEK_CUR, SEEK_END, SEEK_SET
# from smbclient._os import (
#     XATTR_CREATE,
#     XATTR_REPLACE,
#     SMBDirEntry,
#     SMBDirEntryInformation,
#     SMBStatResult,
#     SMBStatVolumeResult,
#     copyfile,
#     getxattr,
#     link,
#     listdir,
#     listxattr,
#     lstat,
#     makedirs,
#     mkdir,
#     open_file,
#     readlink,
#     remove,
#     removedirs,
#     removexattr,
#     rename,
#     renames,
#     replace,
#     rmdir,
#     scandir,
#     setxattr,
#     stat,
#     stat_volume,
#     symlink,
#     truncate,
#     unlink,
#     utime,
#     walk,
# )
# from smbclient._pool import (
#     ClientConfig,
#     delete_session,
#     register_session,
#     reset_connection_cache,

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
    # class SMBStatResult(smbclient.SMBStatResult):
    #     pass
    # class SMBStatVolumeResult(smbclient.SMBStatVolumeResult):
    #     pass




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