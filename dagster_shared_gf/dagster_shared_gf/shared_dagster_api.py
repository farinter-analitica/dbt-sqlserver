import requests
from typing import Optional
from enum import Enum
from dataclasses import dataclass

from dagster_graphql import DagsterGraphQLClient, ReloadRepositoryLocationStatus


def get_client(
    host: Optional[str] = None, port: Optional[int] = None
) -> DagsterGraphQLClient:
    """
    Instancia un cliente GraphQL de Dagster apuntando al webserver.
    """
    if host is None:
        host = os.environ.get("DAGSTER_WEBSERVER_HOST", "localhost")
    if port is None:
        port = int(os.environ.get("DAGSTER_WEBSERVER_PORT", 3000))
    return DagsterGraphQLClient(hostname=host, port_number=port)


def reload_code_location(host: str, port: int, location_name: str) -> bool:
    client = get_client(host, port)
    result = client.reload_repository_location(location_name)
    return result.status == ReloadRepositoryLocationStatus.SUCCESS


class ReloadWorkspaceStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass
class ReloadWorkspaceInfo:
    status: ReloadWorkspaceStatus
    failure_type: Optional[str] = None
    message: Optional[str] = None


RELOAD_WORKSPACE_MUTATION = """
mutation GraphQLClientReloadWorkspace {
   reloadWorkspace {
      __typename
      ... on Workspace {
        locationEntries {
          name
          loadStatus
          locationOrLoadError {
            __typename
            ... on RepositoryLocation {
              repositories {
                name
              }
            }
            ... on PythonError {
              message
            }
          }
        }
      }
      ... on UnauthorizedError {
        message
      }
      ... on PythonError {
        message
      }
   }
}
"""


def reload_workspace_standard(
    host: Optional[str] = None, port: Optional[int] = None
) -> ReloadWorkspaceInfo:
    """
    Reloads the entire Dagster workspace, which reloads all repository locations.

    This follows the same pattern as reload_repository_location from the DagsterGraphQLClient.

    Args:
        host: The hostname or IP of the Dagster webserver
        port: The port the webserver is running on

    Returns:
        ReloadWorkspaceInfo: Object with information about the result of the reload request
    """
    import os

    if host is None:
        host = os.environ.get("DAGSTER_WEBSERVER_HOST", "localhost")
    if port is None:
        port = int(os.environ.get("DAGSTER_WEBSERVER_PORT", 3000))

    client = get_client(host, port)

    try:
        res_data = client._execute(RELOAD_WORKSPACE_MUTATION)
        query_result = res_data["reloadWorkspace"]
        query_result_type = query_result["__typename"]

        if query_result_type == "Workspace":
            # Check if any location entries have errors
            location_entries = query_result.get("locationEntries", [])
            failed_locations = []

            for entry in location_entries:
                location_or_error = entry.get("locationOrLoadError", {})
                if location_or_error.get("__typename") == "PythonError":
                    failed_locations.append(
                        {
                            "name": entry.get("name"),
                            "error": location_or_error.get("message"),
                        }
                    )

            if failed_locations:
                return ReloadWorkspaceInfo(
                    status=ReloadWorkspaceStatus.FAILURE,
                    failure_type="PartialFailure",
                    message=f"Some locations failed to reload: {failed_locations}",
                )
            else:
                return ReloadWorkspaceInfo(status=ReloadWorkspaceStatus.SUCCESS)

        elif query_result_type == "UnauthorizedError":
            return ReloadWorkspaceInfo(
                status=ReloadWorkspaceStatus.FAILURE,
                failure_type="UnauthorizedError",
                message=query_result["message"],
            )
        elif query_result_type == "PythonError":
            return ReloadWorkspaceInfo(
                status=ReloadWorkspaceStatus.FAILURE,
                failure_type="PythonError",
                message=query_result["message"],
            )
        else:
            return ReloadWorkspaceInfo(
                status=ReloadWorkspaceStatus.FAILURE,
                failure_type=query_result_type,
                message=query_result.get("message", "Unknown error"),
            )

    except Exception as e:
        return ReloadWorkspaceInfo(
            status=ReloadWorkspaceStatus.FAILURE,
            failure_type="ClientError",
            message=str(e),
        )


def reload_workspace(host: str, port: int) -> bool:
    """
    Reloads the entire Dagster workspace, which reloads all repository locations.

    Args:
        host: The hostname or IP of the Dagster webserver
        port: The port the webserver is running on

    Returns:
        True if the reload was successful, False otherwise
    """
    info = reload_workspace_standard(host, port)
    return info.status == ReloadWorkspaceStatus.SUCCESS


if __name__ == "__main__":
    import os

    # Example usage:
    host = os.environ.get("DAGSTER_WEBSERVER_HOST", "localhost")
    port = int(os.environ.get("DAGSTER_WEBSERVER_PORT", 3000))
    location_to_reload = "dagster_kielsa_gf"

    try:
        # Reload a specific location
        result = reload_code_location(host, port, location_to_reload)
        print("Reload Location Response:", result)

        # Reload entire workspace
        workspace_result = reload_workspace(host, port)
        print("Reload Workspace Response:", workspace_result)
    except requests.RequestException as exc:
        print("Error reloading:", exc)
