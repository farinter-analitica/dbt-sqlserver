from dagster import ConfigurableResource, EnvVar, InitResourceContext, asset, Definitions
from pydantic import Field, dataclasses, PrivateAttr
from typing import List, Literal, Generator, Any, Dict

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

class SMBResource(ConfigurableResource):
    credentials: str
    server: str
    username: str 
    password: str
    server_ip: str 
    _is_initialized: bool = None
    _context: InitResourceContext = None
    def setup_for_execution(self, context: InitResourceContext):
        self._context = context
        self._is_initialized = True

    def log_event(self, type: Literal['info', 'warning', 'error'], message: str):
        if not hasattr(self, '_context') or self._context is None:
            raise ValueError("Context has not been set. Call setup_for_execution first.")
        with self._context.log as cl:
            cl.info(message) if type == "info" else cl.warning(message) if type == "warning" else cl.error(message)

    def get_smbclient(self) -> smbclient: # returns smbclient:
        """
        A function that creates and returns an smbclient using the already set username, password and server attributes.
        """

        if self.password is None or self.username is None:
            raise ValueError("Username and password must be set")
        new_smbclient = smbclient
        new_smbclient.register_session(server=self.server_ip
                                                   , username=self.username
                                                   , password=self.password
                                                   )
        return new_smbclient



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
    print(smb_resource_analitica_nasgftgu02.get_smbclient().listdir(r"\\10.0.4.157\data_repo\grupo_farinter\presupuesto_ventas_finanzas"))