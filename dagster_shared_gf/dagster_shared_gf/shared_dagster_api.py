import requests
import uuid
from typing import Optional

from dagster_graphql import DagsterGraphQLClient


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


def reload_workspace(host: str, port: int) -> dict:
    """
    Sends a GraphQL mutation to the Dagster webserver to reload the entire workspace (all locations).

    :param host: The hostname or IP of the Dagster webserver (e.g. 'localhost', '100.#.#.#').
    :param port: The port the webserver is running on (e.g. 9786).
    :return: The JSON response from the Dagster webserver.
    """

    # This is the GraphQL mutation string used to reload the entire workspace.
    graphql_mutation = """
    mutation ReloadWorkspaceMutation {
      reloadWorkspace {
        ... on Workspace {
          id
          locationEntries {
            name
            id
            loadStatus
            locationOrLoadError {
              ... on RepositoryLocation {
                id
                repositories {
                  id
                  name
                  pipelines {
                    id
                    name
                    __typename
                  }
                  __typename
                }
                __typename
              }
              ...PythonErrorFragment
              __typename
            }
            __typename
          }
          __typename
        }
        ...UnauthorizedErrorFragment
        ...PythonErrorFragment
        __typename
      }
    }

    fragment UnauthorizedErrorFragment on UnauthorizedError {
      message
      __typename
    }

    fragment PythonErrorFragment on PythonError {
      message
      stack
      errorChain {
        ...PythonErrorChain
        __typename
      }
      __typename
    }

    fragment PythonErrorChain on ErrorChainLink {
      isExplicitLink
      error {
        message
        stack
        __typename
      }
      __typename
    }
    """

    # GraphQL endpoint; note ?op=... is optional but matches what the UI does
    url = f"http://{host}:{port}/graphql?op=ReloadWorkspaceMutation"

    # For local/unsecured development, these headers are often enough. Adjust as needed for auth.
    headers = {
        "content-type": "application/json",
        "accept": "*/*",
    }

    payload = {
        "operationName": "ReloadWorkspaceMutation",
        "variables": {},
        "query": graphql_mutation,
        "Origin": f"http://{host}:{port}",
        "Referer": f"http://{host}:{port}/deployment/locations",
        "idempotency-key": str(uuid.uuid4()),
    }

    response = requests.post(url, headers=headers, json=payload, verify=False)
    response.raise_for_status()  # Raise exception if the request failed at the HTTP level

    return response.json()


def reload_code_location(host: str, port: int, location_name: str) -> dict:
    """
    Sends a GraphQL mutation to the Dagster webserver to reload the specified repository location.

    :param host: The hostname or IP of the Dagster webserver (e.g. 'localhost', '100.#.#.#').
    :param port: The port the webserver is running on (e.g. 9786).
    :param location_name: The code location name to reload.
    :return: The JSON response from the Dagster webserver.
    """
    graphql_mutation = """
    mutation ReloadRepositoryLocationMutation($location: String!) {
      reloadRepositoryLocation(repositoryLocationName: $location) {
        ... on WorkspaceLocationEntry {
          id
          loadStatus
          locationOrLoadError {
            ... on RepositoryLocation {
              id
              __typename
            }
            ...PythonErrorFragment
            __typename
          }
          __typename
        }
        ... on UnauthorizedError {
          message
          __typename
        }
        ... on ReloadNotSupported {
          message
          __typename
        }
        ... on RepositoryLocationNotFound {
          message
          __typename
        }
        ...PythonErrorFragment
        __typename
      }
    }

    fragment PythonErrorFragment on PythonError {
      message
      stack
      errorChain {
        ...PythonErrorChain
        __typename
      }
      __typename
    }

    fragment PythonErrorChain on ErrorChainLink {
      isExplicitLink
      error {
        message
        stack
        __typename
      }
      __typename
    }
    """

    variables = {"location": location_name}
    url = f"http://{host}:{port}/graphql?op=ReloadRepositoryLocationMutation"
    headers = {
        "content-type": "application/json",
        "accept": "*/*",
        "Origin": f"http://{host}:{port}",
        "Referer": f"http://{host}:{port}/deployment/locations",
        "idempotency-key": str(uuid.uuid4()),
    }
    payload = {
        "operationName": "ReloadRepositoryLocationMutation",
        "variables": variables,
        "query": graphql_mutation,
    }
    response = requests.post(url, headers=headers, json=payload, verify=False)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    import os

    # Example usage:
    host = os.environ.get("DAGSTER_WEBSERVER_HOST", "172.16.2.235")
    port = int(os.environ.get("DAGSTER_WEBSERVER_PORT", 9300))
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
