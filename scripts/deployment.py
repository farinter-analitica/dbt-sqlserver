#!/usr/bin/env python3
import os
import sys
import subprocess
import platform
import argparse
import shutil

# Minimum Python version for running this script
REQUIRED_MAJOR = 3
REQUIRED_MINOR = 11
DEFAULT_PYTHON_VERSION = f"{REQUIRED_MAJOR}.{REQUIRED_MINOR}"
CARGO_BIN_DIR = os.path.expanduser("~/.local/bin")
CARGO_UV_BIN = os.path.join(CARGO_BIN_DIR, "uv")

# List of private repositories for SSH deploy key management
PRIVATE_REPOS_TO_SSH = [
    {
        "repo_name": "algoritmos-gf",
        "github_org": "farinter-analitica",
    }
]

###############################################################################
# Utility Functions
###############################################################################


def run_cmd(
    cmd: list[str],
    error_msg: str = "Command failed",
    capture: bool = True,
    cwd: str | None = None,
) -> str | None:
    """
    Run a subprocess command and capture its output.
    Raises RuntimeError with detailed error information if the command fails.
    """
    try:
        result = subprocess.run(
            cmd, check=True, capture_output=capture, text=True, cwd=cwd
        )
        return result.stdout.strip() if capture else None
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"{error_msg}: {' '.join(cmd)}\nReturn code: {e.returncode}\nStdout: {e.stdout}\nStderr: {e.stderr}"
        ) from e


def get_uv_command() -> str:
    """Get the uv command, preferring the one in PATH, else using the saved CARGO_UV_BIN."""
    return (
        "uv"
        if shutil.which("uv")
        else CARGO_UV_BIN + ".exe"
        if platform.system() == "Windows"
        else CARGO_UV_BIN
    )


###############################################################################
# uv Integration Functions
###############################################################################


def install_uv_standalone(reinstall: bool = False):
    """
    Install uv using its standalone installer only if it is not already installed.
    This installs uv to ~/.cargo/bin and updates PATH accordingly.
    """
    exit = False
    if shutil.which("uv"):
        print("uv is already installed, skipping installation.")
        exit = True

    if exit and not reinstall:
        return

    print("uv not installed or reinstall requested. Proceeding with installation...")

    # Create cargo bin directory if it doesn't exist
    try:
        os.makedirs(CARGO_BIN_DIR, exist_ok=True)
    except Exception as e:
        print(f"Ignored error creating cargo bin directory: {e}")

    if platform.system() == "Linux":
        # Add cargo_bin to PATH if not already there
        if CARGO_UV_BIN not in os.environ.get("PATH", ""):
            os.environ["PATH"] = (
                f"{CARGO_UV_BIN}{os.pathsep}{os.environ.get('PATH', '')}"
            )
        run_cmd(
            ["bash", "-c", "curl -LsSf https://astral.sh/uv/install.sh | sh"],
            error_msg="Failed to install uv",
            capture=False,
        )
        run_cmd(
            [
                "bash",
                "-c",
                "echo 'eval \"$(uv generate-shell-completion bash)\"' >> ~/.bashrc",
            ],
            error_msg="Failed to install uv",
            capture=False,
        )
        run_cmd(
            ["bash", "-c", "source ~/.bashrc"],
            error_msg="Failed to source .bashrc",
            capture=False,
        )

    elif platform.system() == "Windows":
        run_cmd(
            ["powershell", "-Command", "irm https://astral.sh/uv/install.ps1 | iex"],
            error_msg="Failed to install uv",
            capture=False,
        )


def get_python_version_from_config(deploy_dir: str) -> str:
    """
    Extract Python version from pyproject.toml.
    Returns the required version (defaulting to DEFAULT_PYTHON_VERSION if not found).
    """
    pyproject_file = os.path.join(deploy_dir, "pyproject.toml")
    try:
        with open(pyproject_file, "rb") as f:
            import tomllib  # Requires Python 3.11+

            pyproject_data = tomllib.load(f)
            required_version = (
                pyproject_data.get("tool", {})
                .get("main_dagster", {})
                .get("python_version", DEFAULT_PYTHON_VERSION)
            )
            return required_version
    except Exception as e:
        print(f"Error reading Python version from config: {e}")
        return DEFAULT_PYTHON_VERSION


def manage_python_and_venv(
    deploy_dir: str, venv_dir: str, required_version: str
) -> tuple[str, str]:
    """
    Unified Python version and virtual environment management using uv.
    Installs the required Python version, removes any existing venv,
    and creates a new virtual environment using uv.
    Returns the venv directory and the path to the new Python binary.
    """
    print(f"Installing Python {required_version} using uv...")

    # Check if uv is in PATH, otherwise use the saved CARGO_UV_BIN path
    uv_command = get_uv_command()

    # Install the required Python
    run_cmd(
        [uv_command, "python", "install", required_version],
        error_msg=f"Failed to install Python {required_version}",
        capture=False,
    )

    print("Creating virtual environment using uv...")
    run_cmd(
        [uv_command, "venv", "-p", required_version, venv_dir],
        error_msg="Failed to create virtual environment",
        capture=False,
    )

    # Manually re-mark the venv as active by setting VIRTUAL_ENV
    os.environ["VIRTUAL_ENV"] = venv_dir

    bin_dir = "Scripts" if platform.system() == "Windows" else "bin"
    python_path = os.path.join(venv_dir, bin_dir, "python")
    return venv_dir, python_path


def install_deps_uv(
    venv_dir: str, deploy_dir: str, dev: bool = False, only_external: bool = False
):
    """
    Install dependencies using uv.
    - Installs foundation packages and local packages (with optional dev flag).
    - When only_external is True, attempts to install external dependencies.
    """
    # Check if uv is in PATH, otherwise use the saved CARGO_UV_BIN path
    uv_command = get_uv_command()

    if not only_external:
        print("Installing core dependencies...")
        sync_cmd = [uv_command, "sync"]
        if not dev:
            sync_cmd.append("--locked")
            sync_cmd.append("--no-dev")
            sync_cmd.append("--inexact")

        run_cmd(
            sync_cmd,
            error_msg="Dagster deps installation failed",
            capture=False,
            cwd=deploy_dir,
        )

    if only_external:
        print("\nAttempting to install external dependencies...")
        try:
            sync_cmd = [uv_command, "sync", "--only-group", "external", "--inexact"]
            if not dev:
                sync_cmd.append("--locked")
            run_cmd(
                sync_cmd,
                error_msg="External dependency installation",
                capture=False,
                cwd=deploy_dir,
            )
            print("✅ External dependencies installed successfully")
        except RuntimeError as e:
            print(f"⚠️ External dependencies could not be installed: {e}")
            print("The system will continue to function without these dependencies.")


###############################################################################
# Legacy Environment & SSH Management (Backwards Compatible)
###############################################################################


def check_vars():
    """Ensure required environment variables are provided."""
    for var in ["ENV", "DEPLOY_DIR"]:
        if not os.environ.get(var):
            sys.exit(f"Error: {var} must be explicitly provided (e.g., {var}=value).")


def check_os():
    """Ensure the script is running on Linux (for deployment targets)."""
    if platform.system() != "Linux":
        sys.exit("This deployment procedure is intended for Linux systems only.")


def manage_services(env: str, action: str = "restart"):
    """Manage dagster services via systemctl (start, stop, or restart)."""
    if action not in ["start", "stop", "restart"]:
        raise ValueError("Invalid action. Use 'start', 'stop', or 'restart'.")
    print(f"Managing services: {action}")
    service1 = f"dagster_{env}_webserver"
    service2 = f"dagster_{env}_daemon"
    run_cmd(
        ["sudo", "systemctl", action, service1, service2],
        error_msg=f"Service {action} failed",
        capture=False,
    )


def generate_service(python_bin: str, deploy_dir: str):
    """Regenerate service templates (full deployment only)."""
    print("Generating service template...")
    cmd = [
        "sudo",
        python_bin,
        os.path.join(deploy_dir, "scripts", "generate_dagster_service.py"),
    ]
    try:
        output = run_cmd(cmd)
        if output:
            sys.stdout.write(output)
    except RuntimeError as e:
        raise Exception(f"Error executing command: {' '.join(cmd)}\n{e}") from e


# SSH deploy key management functions remain largely unchanged for backward compatibility.
def check_deploy_key(repo_name="algoritmos-gf"):
    """Check if a deploy key exists for the specified repository."""
    home_dir = os.path.expanduser("~")
    ssh_dir = os.path.join(home_dir, ".ssh")
    key_name = f"{repo_name.replace('-', '_')}_deploy_key"
    key_path = os.path.join(ssh_dir, key_name)
    pub_key_path = f"{key_path}.pub"
    key_exists = os.path.exists(key_path) and os.path.exists(pub_key_path)
    pub_key_content = None
    if key_exists:
        try:
            with open(pub_key_path, "r") as pub_file:
                pub_key_content = pub_file.read().strip()
        except Exception:
            pass
    return key_exists, key_path, pub_key_content


def generate_deploy_key(key_path: str, repo_name: str) -> str | None:
    """Generate an SSH deploy key."""
    try:
        os.makedirs(os.path.dirname(key_path), mode=0o700, exist_ok=True)
        run_cmd(
            [
                "ssh-keygen",
                "-t",
                "ed25519",
                "-C",
                f"deploy-key-{repo_name}",
                "-f",
                key_path,
                "-N",
                "",
            ],
            error_msg="Failed to generate SSH key",
            capture=False,
        )
        if platform.system() != "Windows":
            os.chmod(key_path, 0o600)
        with open(f"{key_path}.pub", "r") as pub_file:
            return pub_file.read().strip()
    except Exception as e:
        print(f"Error generating SSH key: {e}")
        return None


def configure_ssh_for_repo(key_path: str, repo_name: str) -> tuple[bool, str]:
    """Configure SSH to use the specified key for a GitHub repository."""
    host_alias = f"github.com-{repo_name}"
    config_path = os.path.join(os.path.dirname(key_path), "config")
    config_entry = (
        f"Host {host_alias}\n"
        f"    HostName github.com\n"
        f"    User git\n"
        f"    IdentityFile {key_path}\n"
        f"    IdentitiesOnly yes\n"
    )
    config_exists = False
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as config_file:
                if host_alias in config_file.read():
                    config_exists = True
        except Exception:
            pass
    if not config_exists:
        try:
            mode = "a+" if os.path.exists(config_path) else "w"
            with open(config_path, mode) as config_file:
                if mode == "a+":
                    config_file.seek(0, os.SEEK_END)
                    if config_file.tell() > 0:
                        config_file.seek(config_file.tell() - 1, os.SEEK_SET)
                        last_char = config_file.read(1)
                        if last_char != "\n":
                            config_file.write("\n")
                config_file.write(config_entry)
            if platform.system() != "Windows":
                os.chmod(config_path, 0o600)
            return True, host_alias
        except Exception as e:
            print(f"Error updating SSH config: {e}")
            return False, host_alias
    return True, host_alias


def verify_ssh_connection(host_alias: str) -> bool:
    """Test the SSH connection to GitHub using the specified host alias."""
    try:
        ssh_dir = os.path.join(os.path.expanduser("~"), ".ssh")
        os.makedirs(ssh_dir, exist_ok=True)
        config_path = os.path.join(ssh_dir, "config")
        if not os.path.exists(config_path):
            print(f"⚠️ SSH config file doesn't exist at {config_path}")
            print("SSH connection test will likely fail.")
        known_hosts_path = os.path.join(ssh_dir, "known_hosts")
        github_in_known_hosts = False
        alias_in_known_hosts = False
        if os.path.exists(known_hosts_path):
            try:
                with open(known_hosts_path, "r") as kh_file:
                    content = kh_file.read()
                    if "github.com" in content:
                        github_in_known_hosts = True
                    if host_alias in content:
                        alias_in_known_hosts = True
            except Exception:
                pass
        if not github_in_known_hosts:
            print("Adding github.com to known_hosts...")
            try:
                keyscan_output = run_cmd(
                    ["ssh-keyscan", "github.com"],
                    error_msg="Failed to scan GitHub host key",
                    capture=True,
                )
                if not keyscan_output:
                    raise RuntimeError("ssh-keyscan did not return any output")
                with open(known_hosts_path, "a+") as kh_file:
                    kh_file.write(keyscan_output + "\n")
                print("✅ Added github.com to known_hosts")
            except Exception as e:
                print(f"Warning: Could not add GitHub to known_hosts: {e}")
        if not alias_in_known_hosts:
            print(f"Adding {host_alias} to known_hosts...")
            try:
                keyscan_output = run_cmd(
                    ["ssh-keyscan", "github.com"],
                    error_msg=f"Failed to scan GitHub host key for {host_alias}",
                    capture=True,
                )
                if not keyscan_output:
                    raise RuntimeError("ssh-keyscan did not return any output")
                keyscan_output_modified = keyscan_output.replace(
                    "github.com", host_alias
                )
                with open(known_hosts_path, "a+") as kh_file:
                    kh_file.write(keyscan_output_modified + "\n")
                print(f"✅ Added {host_alias} to known_hosts")
            except Exception as e:
                print(f"Warning: Could not add {host_alias} to known_hosts: {e}")
        try:
            result = subprocess.run(
                [
                    "ssh",
                    "-T",
                    "-o",
                    "StrictHostKeyChecking=accept-new",
                    f"git@{host_alias}",
                ],
                capture_output=True,
                text=True,
            )
            output = result.stdout + result.stderr
            if "You've successfully authenticated" in output:
                print("✅ SSH connection successful!")
                return True
            else:
                print(f"⚠️ SSH connection response: {output}")
                print("SSH connection may require further verification.")
                return False
        except Exception as e:
            print(f"❌ SSH connection failed: {e}")
            return False
    except Exception as e:
        print(f"Error during SSH connection test: {e}")
        return False


def setup_deploy_keys(
    repo_name: str | None = None,
    github_org: str | None = None,
    setup: bool = True,
    test: bool = True,
    force_new: bool = False,
) -> bool:
    """
    Check, set up and/or test SSH deploy keys for private GitHub repositories.
    If repo_name or github_org is not provided, uses default configuration.
    """
    if not repo_name or not github_org:
        repo_configs = PRIVATE_REPOS_TO_SSH
    else:
        repo_configs = [{"repo_name": repo_name, "github_org": github_org}]
    for repo in repo_configs:
        repo_name = repo["repo_name"]
        github_org = repo["github_org"]
        print(f"Managing deploy key for {github_org}/{repo_name}...")
        key_exists, key_path, pub_key = check_deploy_key(repo_name)
        if key_exists and not force_new:
            print(f"SSH key already exists at: {key_path}")
            if pub_key:
                print("\nExisting public key:")
                print(f"\n{pub_key}\n")
        elif setup or force_new:
            if key_exists and force_new:
                print("Forcing creation of new SSH key...")
            else:
                print("Generating new SSH key...")
            pub_key = generate_deploy_key(key_path, repo_name)
            if not pub_key:
                print("❌ Failed to generate SSH key")
                return False
            print(f"✅ Generated SSH key: {key_path}")
            print(
                "\nIMPORTANT: Add this public key as a deploy key to your GitHub repository:"
            )
            print(f"https://github.com/{github_org}/{repo_name}/settings/keys")
            print("\nYour public key is:")
            print(f"\n{pub_key}\n")
        if setup:
            success, host_alias = configure_ssh_for_repo(key_path, repo_name)
            if success:
                print(f"✅ SSH config updated for {host_alias}")
            else:
                print(f"❌ Failed to update SSH config for {host_alias}")
                return False
        else:
            host_alias = f"github.com-{repo_name}"
        if test:
            print(f"\nTesting SSH connection to GitHub for {repo_name}...")
            if not verify_ssh_connection(host_alias):
                print("\n⚠️ Connection test did not confirm successful authentication.")
                print("This could be because:")
                print(
                    "1. The deploy key hasn't been added to the GitHub repository yet"
                )
                print("2. The deploy key doesn't have the correct permissions")
                print("3. There's a network or SSH configuration issue")
                print("\nAdd the key to GitHub at:")
                print(f"https://github.com/{github_org}/{repo_name}/settings/keys")
                if not setup:
                    return False
        print(
            "\nTo use this deploy key in your pyproject.toml, add the dependency like this:"
        )
        print(
            f'"algoritmos-gf @ git+ssh://git@{host_alias}/{github_org}/{repo_name}.git",'
        )
    return True


def setup_git(python_path: str) -> bool:
    """
    Automate Git configuration for commit templates and pre-commit hooks using uv.
    This version replaces direct pip commands with uv equivalents and handles cases
    where uv isn't in the PATH.
    """
    print("Configuring Git commit template and pre-commit hooks...")
    template_path = os.path.join("templates", "commit-template.git.txt")

    # Check if uv is in PATH, otherwise use the saved CARGO_UV_BIN path
    uv_command = get_uv_command()

    try:
        # Verify Git is available and set commit template
        run_cmd(["git", "--version"], error_msg="Git not found")
        run_cmd(
            ["git", "config", "commit.template", template_path],
            error_msg="Failed to set Git commit template",
        )
        print("Git commit template configured successfully.")

        # Use uv to check if pre-commit is installed; install if missing
        try:
            run_cmd(
                [uv_command, "pip", "show", "pre-commit"],
                error_msg="Pre-commit not found",
            )
        except RuntimeError:
            print("Installing pre-commit via uv...")
            run_cmd(
                [uv_command, "pip", "install", "pre-commit"],
                error_msg="Failed to install pre-commit",
            )

        # Determine the pre-commit binary from the uv-managed venv
        # Assuming the pre-commit binary is in the same bin directory as the current interpreter.
        precommit_bin = os.path.join(os.path.dirname(python_path), "pre-commit")

        # Add .exe extension on Windows
        if platform.system() == "Windows" and not precommit_bin.endswith(".exe"):
            precommit_bin += ".exe"

        print("Installing pre-commit hooks...")
        run_cmd(
            [precommit_bin, "install"], error_msg="Failed to install pre-commit hooks"
        )
        print("Pre-commit hooks installed successfully.")
    except RuntimeError as e:
        print(f"Error setting up Git environment: {e}")
        return False
    return True


def set_deployment_vars() -> tuple[str, str, str, str, str]:
    """
    Set deployment variables based on the environment and local context.
    Reads 'ENV' and 'DEPLOY_DIR' from environment variables, then computes:
      - deploy_dir: absolute path of DEPLOY_DIR
      - venv_dir: deployment directory + ".venv"
      - python_path: path to the Python executable in the virtual environment
      - pip_path: path to the pip executable in the virtual environment
    Also sets the VIRTUAL_ENV and PYTHONPATH environment variables.

    Returns:
        env, deploy_dir, venv_dir, python_path, pip_path
    """
    env = os.environ.get("ENV", "dev")
    deploy_dir = os.path.abspath(os.environ.get("DEPLOY_DIR", "."))
    venv_dir = os.path.join(deploy_dir, ".venv")
    bin_dir = "Scripts" if platform.system() == "Windows" else "bin"
    python_path = os.path.join(venv_dir, bin_dir, "python")
    pip_path = os.path.join(venv_dir, bin_dir, "pip")

    # Set environment variables for uv and module resolution
    os.environ["VIRTUAL_ENV"] = venv_dir
    os.environ["PYTHONPATH"] = python_path

    print(f"Deploy dir: {deploy_dir}")
    print(f"Venv dir: {venv_dir}")
    print(f"Python bin: {python_path}")
    print(f"Pip bin: {pip_path}")

    return env, deploy_dir, venv_dir, python_path, pip_path


def dev():
    """
    Set up a platform-agnostic development environment using uv.

    This function now:
      - Reads the required Python version from pyproject.toml.
      - Creates (or updates) the virtual environment using uv.
      - Installs both core and external dependencies via uv.
      - Sets up deploy keys and configures Git.
    """
    print("Setting up development environment...")
    # Use the current working directory as the deployment directory.
    env, deploy_dir, venv_dir, python_path, pip_path = set_deployment_vars()

    # Get the required Python version from configuration (or fallback to default)
    required_version = get_python_version_from_config(deploy_dir)

    # Install uv standalone
    install_uv_standalone(reinstall=True)

    # Use uv to create or update the virtual environment and get the Python binary path.
    venv_dir, python_bin = manage_python_and_venv(
        deploy_dir, venv_dir, required_version
    )

    # Install core development dependencies using uv
    install_deps_uv(venv_dir, deploy_dir, dev=True)

    # Set up SSH deploy keys for private repos
    setup_deploy_keys()

    # Install external dependencies via uv (non-critical, fallback installation)
    install_deps_uv(venv_dir, deploy_dir, only_external=True)

    # Configure Git commit templates and pre-commit hooks using uv commands
    setup_git(python_path)


def reload_code_locations():
    try:
        import requests
        import yaml

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
                        __typename
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

        # Read workspace.yaml to get code location names
        with open("workspace.yaml", "r") as f:
            workspace_config = yaml.safe_load(f)

        location_names = []
        load_from = workspace_config.get("load_from", [])
        for entry in load_from:
            # entry is a dict like {'python_module': {...}} or {'python_file': {...}}
            for value in entry.values():
                # value is the nested dict like {'module_name': ..., 'location_name': ...}
                if isinstance(value, dict) and "location_name" in value:
                    location_names.append(value["location_name"])
                    # Assume only one location_name per entry in load_from
                    break

        if not location_names:
            print("No code locations found in workspace.yaml.")
            return False

        for location_name in location_names:
            reload_code_location(
                host="localhost",
                port=int(os.environ.get("DAGSTER_GRAPHQL_PORT", 3000)),
                location_name=location_name,
            )
            print(f"Reloaded code location: {location_name}")

        print("Code locations reloaded successfully.")
        return True

    except ImportError as e:
        print(
            "Error: Failed to import reload_code_location from dagster_shared_gf."
            f"Error: {e}"
        )
        return False
    except Exception as e:
        print(f"Error reloading code locations: {e}")
        return False


###############################################################################
# Deployment Functions Using uv
###############################################################################


def deploy_continuous():
    """Perform continuous deployment (code changes only)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_path, pip_path = set_deployment_vars()
    print("Performing continuous deployment (code changes only)...")
    reload_code_locations()


def deploy_fast():
    """Perform fast deployment (dependencies only)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_path, pip_path = set_deployment_vars()
    print("Performing fast deployment (dependencies only)...")
    install_uv_standalone()
    python_version = get_python_version_from_config(deploy_dir)
    venv_dir, python_path = manage_python_and_venv(deploy_dir, venv_dir, python_version)
    install_deps_uv(venv_dir, deploy_dir)
    setup_deploy_keys()
    install_deps_uv(venv_dir, deploy_dir, only_external=True)
    reload_code_locations()


def deploy_partial():
    """Perform partial deployment (update Python and all dependencies)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_path, pip_path = set_deployment_vars()
    print("Performing partial deployment (Python + dependencies)...")
    manage_services(env, action="stop")
    python_version = get_python_version_from_config(deploy_dir)
    install_uv_standalone(reinstall=True)
    venv_dir, python_path = manage_python_and_venv(deploy_dir, venv_dir, python_version)
    install_deps_uv(venv_dir, deploy_dir)
    setup_deploy_keys()
    install_deps_uv(venv_dir, deploy_dir, only_external=True)
    manage_services(env)


def deploy_full():
    """Perform full deployment (service template, Python, and dependencies)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_path, pip_path = set_deployment_vars()
    print("Performing full deployment (service template, Python, and dependencies)...")
    manage_services(env, action="stop")
    install_uv_standalone(reinstall=True)
    python_version = get_python_version_from_config(deploy_dir)
    venv_dir, python_path = manage_python_and_venv(deploy_dir, venv_dir, python_version)
    install_deps_uv(venv_dir, deploy_dir)
    setup_deploy_keys()
    install_deps_uv(venv_dir, deploy_dir, only_external=True)
    generate_service(python_path, deploy_dir)
    manage_services(env)


###############################################################################
# Main Command Dispatcher
###############################################################################


def main():
    parser = argparse.ArgumentParser(
        description="Deployment script with uv integration"
    )
    parser.add_argument(
        "command",
        choices=[
            "deploy-full",
            "deploy-partial",
            "deploy-fast",
            "deploy-continuous",
            "dev",
            "install-deps",
            "setup-git",
            "check-deploy-key",
            "setup-deploy-key",
            "test-deploy-key",
            "reload-code-locations",
        ],
        help="Deployment command to run",
    )
    parser.add_argument("--repo", default=None, help="Repository name for deploy keys")
    parser.add_argument(
        "--org", default=None, help="GitHub organization for deploy keys"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force creation of new deploy key"
    )
    parser.add_argument(
        "--only-external",
        action="store_true",
        help="Only install external dependencies",
    )
    parser.add_argument(
        "--dev", action="store_true", help="Install development dependencies"
    )
    args = parser.parse_args()

    env, deploy_dir, venv_dir, python_path, pip_path = set_deployment_vars()
    if args.command == "check-deploy-key":
        key_exists, key_path, pub_key = check_deploy_key(args.repo)
        if key_exists:
            print(f"✅ Deploy key exists at: {key_path}")
            if pub_key:
                print(f"\nPublic key:\n{pub_key}\n")
            return True
        else:
            print(f"❌ No deploy key found for {args.repo}")
            return False
    elif args.command == "setup-deploy-key":
        return setup_deploy_keys(
            args.repo, args.org, setup=True, test=False, force_new=args.force
        )
    elif args.command == "test-deploy-key":
        return setup_deploy_keys(
            args.repo, args.org, setup=False, test=True, force_new=False
        )
    elif args.command == "install-deps":
        install_deps_uv(
            venv_dir, deploy_dir, dev=args.dev, only_external=args.only_external
        )
    elif args.command == "setup-git":
        setup_git(python_path)
    else:
        commands = {
            "deploy-full": deploy_full,
            "deploy-partial": deploy_partial,
            "deploy-fast": deploy_fast,
            "deploy-continuous": deploy_continuous,
            "reload-code-locations": reload_code_locations,
            "dev": dev,
        }
        try:
            commands[args.command]()
        except KeyError:
            print("Help: Run with --help for a list of commands.")


if __name__ == "__main__":
    # Enforce the minimum Python version for running this script.
    current_version = sys.version_info
    if current_version.major < REQUIRED_MAJOR or (
        current_version.major == REQUIRED_MAJOR
        and current_version.minor < REQUIRED_MINOR
    ):
        sys.exit(
            f"Error: At least Python {REQUIRED_MAJOR}.{REQUIRED_MINOR} is required but current version is {current_version.major}.{current_version.minor}"
        )
    main()
