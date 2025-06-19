import requests

# minimal_graphql_mutation = """
# mutation ReloadRepositoryLocationMutation($location: String!) {
#     reloadRepositoryLocation(repositoryLocationName: $location) {
#         ... on WorkspaceLocationEntry {
#         id
#         loadStatus
#         __typename
#         }
#         __typename
#     }
#     }
# """


def reload_code_location(host: str, port: int, location_name: str) -> dict:
    """
    Sends a GraphQL mutation to the Dagster webserver to reload the specified repository location.

    :param host: The hostname or IP of the Dagster webserver (e.g. 'localhost', '100.#.#.#').
    :param port: The port the webserver is running on (e.g. 9786).
    :param location_name: The code location name to reload.
    :return: The JSON response from the Dagster webserver.
    """

    # This is the GraphQL mutation string used to reload a repository location.
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
            ... on PythonError {
              message
              stack
            }
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
        ... on PythonError {
          message
          stack
        }
        __typename
      }
    }
    """

    variables = {"location": location_name}

    # GraphQL endpoint; note ?op=... is optional but matches what the UI does
    url = f"http://{host}:{port}/graphql?op=ReloadRepositoryLocationMutation"

    # For local/unsecured development, these headers are often enough. Adjust as needed for auth.
    headers = {
        "content-type": "application/json",
        "accept": "*/*",
    }

    payload = {
        "operationName": "ReloadRepositoryLocationMutation",
        "variables": variables,
        "query": graphql_mutation,
    }

    response = requests.post(url, headers=headers, json=payload, verify=False)
    response.raise_for_status()  # Raise exception if the request failed at the HTTP level

    return response.json()


if __name__ == "__main__":
    import os

    # Example usage:
    host = os.environ.get("DAGSTER_WEBSERVER_HOST", "172.16.2.235")
    port = int(os.environ.get("DAGSTER_WEBSERVER_PORT", 9300))
    location_to_reload = "dagster_kielsa_gf"

    try:
        result = reload_code_location(host, port, location_to_reload)
        print("Reload Response:", result)
    except requests.RequestException as exc:
        print("Error reloading code location:", exc)
