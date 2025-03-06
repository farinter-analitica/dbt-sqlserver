#!/usr/bin/env python3
import os
import sys
import subprocess
import platform
import argparse

# Enforce the minimum Python version (3.11+)
REQUIRED_MAJOR = 3
REQUIRED_MINOR = 11
PRIVATE_REPOS_TO_SSH = [
    {
        "repo_name": "algoritmos-gf",
        "github_org": "farinter-analitica",
    }
]


# This needs the template in sudo visudo /etc/sudoers.d/github-actions
def get_python_candidates(required_version: str):
    """Return a list of candidate Python commands based on the required version."""
    major = required_version.split(".")[0]
    if platform.system() == "Windows":
        return [f"python{required_version}", "python"]
    return [f"python{required_version}", f"python{major}", "python3", "python"]


def run_cmd(
    cmd: list[str], error_msg: str = "Command failed", capture: bool = True
) -> str | None:
    """
    Run a subprocess command.

    Args:
        cmd: List of command arguments.
        error_msg: Error message prefix if command fails.
        capture: Whether to capture output.

    Returns:
        Command output (str) if captured.

    Raises:
        RuntimeError: If the command fails.
    """
    try:
        result = subprocess.run(cmd, check=True, capture_output=capture, text=True)
        if capture:
            return result.stdout.strip()
        else:
            return None
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"{error_msg}: {' '.join(cmd)}\nReturn code: {e.returncode}\nStdout: {e.stdout}\nStderr: {e.stderr}"
        ) from e


def check_python_available(
    required_version: str = f"{REQUIRED_MAJOR}.{REQUIRED_MINOR}",
    exact_match: bool = False,
):
    """
    Check if a Python interpreter matching the required version is available.

    Args:
        required_version: Version string (e.g., "3.12").
        exact_match: If True, only an exact major.minor match is accepted.

    Returns:
        True if a matching Python is found; otherwise, False.
    """
    required_major, required_minor = map(int, required_version.split(".")[:2])
    for cmd in get_python_candidates(required_version):
        try:
            version_str = run_cmd([cmd, "--version"])
            # Expect output like "Python X.Y.Z"
            if version_str:
                version = version_str.split()[-1]
                major, minor = map(int, version.split(".")[:2])

                if exact_match:
                    if major == required_major and minor == required_minor:
                        print(
                            f"Found Python: {version_str} (exact match for {required_version})"
                        )
                        return True
                else:
                    if major > required_major or (
                        major == required_major and minor >= required_minor
                    ):
                        print(
                            f"Found Python: {version_str} (meets or exceeds {required_version})"
                        )
                        return True
                    else:
                        print(
                            f"Found Python {major}.{minor} but it's older than required {required_version}"
                        )
        except (RuntimeError, FileNotFoundError, ValueError, IndexError):
            continue
    return False


def check_vars():
    """Ensure required environment variables are provided."""
    for var in ["ENV", "DEPLOY_DIR"]:
        if not os.environ.get(var):
            sys.exit(f"Error: {var} must be explicitly provided (e.g., {var}=value).")


def check_os():
    """Ensure the script is running on Linux (for deployment targets)."""
    if platform.system() != "Linux":
        sys.exit("This deployment procedure is intended for Linux systems only.")


def set_vars():
    """
    Set deployment variables based on the environment.

    Returns:
        Tuple containing (env, deploy_dir, venv_dir, python_bin, pip_bin).
    """
    env = os.environ["ENV"]
    deploy_dir = os.environ["DEPLOY_DIR"]
    venv_dir = os.path.join(deploy_dir, ".venv_main_dagster")
    python_bin = os.path.join(venv_dir, "bin", "python")
    pip_bin = os.path.join(venv_dir, "bin", "pip")
    return env, deploy_dir, venv_dir, python_bin, pip_bin


def generate_service(python_bin, deploy_dir):
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


def remove_venv(venv_dir):
    """Remove the existing virtual environment directory."""
    if os.path.exists(venv_dir):
        if platform.system() == "Windows":
            subprocess.run(["rmdir", "/S", "/Q", venv_dir], shell=True, check=True)
        else:
            subprocess.run(["rm", "-rf", venv_dir], check=True)


def manage_python_version(deploy_dir, venv_dir, python_bin, pip_bin):
    """
    Check and update the Python version used in the virtual environment.

    This function examines pyproject.toml for a required Python version, compares it
    with the venv's Python version, and recreates the venv if necessary.
    """
    pyproject_file = os.path.join(deploy_dir, "pyproject.toml")
    print(f"pyproject file: {pyproject_file}")
    if not os.path.exists(pyproject_file):
        print("pyproject.toml not found, skipping Python version check")
        return
    try:
        import tomllib  # Requires Python 3.11+

        with open(pyproject_file, "rb") as f:
            pyproject_data = tomllib.load(f)
        required_version = (
            pyproject_data.get("tool", {}).get("main_dagster", {}).get("python_version")
        )
        if not required_version:
            print("No Python version requirement found in pyproject.toml")
            return
        required_major_minor = ".".join(required_version.split(".")[:2])
        current_major_minor = None
        venv_exists = os.path.exists(venv_dir) and os.path.exists(python_bin)
        if venv_exists:
            try:
                current_version = run_cmd([python_bin, "--version"])
                if current_version:
                    current_major_minor = ".".join(current_version.split(".")[:2])
                if current_major_minor == required_major_minor:
                    print(f"Python version check passed: {current_version}")
                    return
                print(
                    f"Python version mismatch: current={current_version}, required={required_version}"
                )
            except RuntimeError:
                print(
                    "Error checking Python version, will recreate virtual environment"
                )
                venv_exists = False
        if not venv_exists or (
            venv_exists and current_major_minor != required_major_minor
        ):
            print("Creating/recreating virtual environment...")
            if not check_python_available(required_major_minor, exact_match=True):
                print("Error: Required Python version not found, exiting.")
                return
            remove_venv(venv_dir)
            for py_cmd in get_python_candidates(required_version):
                try:
                    version_str = run_cmd([py_cmd, "--version"])
                    print(f"Trying to create venv with {py_cmd} ({version_str})")
                    run_cmd(
                        [py_cmd, "-m", "venv", venv_dir],
                        error_msg="Failed to create venv",
                        capture=False,
                    )
                    activation_cmd = (
                        os.path.join(venv_dir, "Scripts", "activate")
                        if platform.system() == "Windows"
                        else f"source {os.path.join(venv_dir, 'bin', 'activate')}"
                    )
                    print(
                        f"Virtual environment created successfully for Python {required_version}"
                    )
                    print("To activate it, run:")
                    print(f"    {activation_cmd}")
                    return
                except (RuntimeError, FileNotFoundError):
                    continue
            print(
                "Failed to create virtual environment with any available Python version"
            )
            print("Deployment will likely fail")
    except Exception as e:
        print(f"Error during Python version management: {e}")
        print("Continuing with deployment...")


def upgrade_pip(python_bin, pip_bin):
    """Upgrade pip."""
    print("Upgrading pip...")
    run_cmd([python_bin, "-m", "ensurepip", "--upgrade"], error_msg="ensurepip failed")
    run_cmd(
        [python_bin, "-m", "pip", "install", "--upgrade", "pip"],
        error_msg="pip upgrade failed",
    )


def install_deps(pip_bin, dev=False, only_external=False):
    """
    Install required dependencies with fallback mechanism for external dependencies.

    Args:
        pip_bin: Path to pip executable
        dev: Whether to install development dependencies
        external: Whether to install external dependencies
    """
    if not only_external:
        print("Installing core dependencies...")
        packages = [
            [pip_bin, "install", "jinja2"],
            [pip_bin, "install", "python-dotenv"],
            [
                pip_bin,
                "install",
                "-U",
                "--upgrade-strategy",
                "eager",
                "-e",
                "dagster_shared_gf" + ("[dev]" if dev else ""),
                "--config-settings",
                "editable_mode=compat",
            ],
            [
                pip_bin,
                "install",
                "-e",
                "dagster_sap_gf",
                "--config-settings",
                "editable_mode=compat",
            ],
            [
                pip_bin,
                "install",
                "-e",
                "dagster_kielsa_gf",
                "--config-settings",
                "editable_mode=compat",
            ],
        ]

        # Install core packages
        for cmd in packages:
            run_cmd(cmd, error_msg="Dependency installation failed")

    # Try to install external dependencies, but don't fail if they're unavailable
    if only_external:
        print("\nAttempting to install external dependencies...")
        external_cmd = [
            pip_bin,
            "install",
            "-e",
            "dagster_shared_gf[external]",
            "--config-settings",
            "editable_mode=compat",
        ]

        try:
            run_cmd(
                external_cmd, error_msg="External dependency installation", capture=True
            )
            print("✅ External dependencies installed successfully")
        except RuntimeError as e:
            print(f"⚠️ External dependencies could not be installed: {e}")
            print("The system will continue to function without these dependencies.")
            print("To use these features, ensure the deploy keys are set up correctly.")


def manage_services(env, action: str = "restart"):
    """Restart the dagster services."""
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


def check_deploy_key(repo_name="algoritmos-gf"):
    """
    Check if a deploy key exists for the specified repository.

    Args:
        repo_name: The name of the repository

    Returns:
        tuple: (key_exists, key_path, pub_key_content)
    """
    # Determine paths (platform-specific)
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


def generate_deploy_key(key_path, repo_name):
    """
    Generate an SSH deploy key.

    Args:
        key_path: Path where to store the key
        repo_name: Name of the repository (for key comment)

    Returns:
        str: The public key content if successful, None otherwise
    """
    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(key_path), mode=0o700, exist_ok=True)

        # Generate the key using ssh-keygen
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
                "",  # Empty passphrase
            ],
            error_msg="Failed to generate SSH key",
            capture=False,
        )

        # Set proper permissions for the key
        if platform.system() != "Windows":
            os.chmod(key_path, 0o600)

        # Read and return the public key
        with open(f"{key_path}.pub", "r") as pub_file:
            return pub_file.read().strip()
    except Exception as e:
        print(f"Error generating SSH key: {e}")
        return None


def configure_ssh_for_repo(key_path, repo_name):
    """
    Configure SSH to use the specified key for a GitHub repository.

    Args:
        key_path: Path to the SSH key
        repo_name: The repository name

    Returns:
        tuple: (success, host_alias)
    """
    host_alias = f"github.com-{repo_name}"
    config_path = os.path.join(os.path.dirname(key_path), "config")

    config_entry = (
        f"Host {host_alias}\n"
        f"    HostName github.com\n"
        f"    User git\n"
        f"    IdentityFile {key_path}\n"
        f"    IdentitiesOnly yes\n"
    )

    # Check if config already contains our entry
    config_exists = False
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as config_file:
                if host_alias in config_file.read():
                    config_exists = True
        except Exception:
            pass

    # Update config if needed
    if not config_exists:
        try:
            # Create or append to config file
            mode = "a+" if os.path.exists(config_path) else "w"
            with open(config_path, mode) as config_file:
                # Add a newline if appending and the file doesn't end with one
                if mode == "a+":
                    config_file.seek(0, os.SEEK_END)
                    if config_file.tell() > 0:
                        config_file.seek(config_file.tell() - 1, os.SEEK_SET)
                        last_char = config_file.read(1)
                        if last_char != "\n":
                            config_file.write("\n")

                config_file.write(config_entry)

            # Set proper permissions for the config
            if platform.system() != "Windows":
                os.chmod(config_path, 0o600)

            return True, host_alias
        except Exception as e:
            print(f"Error updating SSH config: {e}")
            return False, host_alias

    return True, host_alias


def test_ssh_connection(host_alias):
    """
    Test the SSH connection to GitHub using the specified host alias.

    Args:
        host_alias: The SSH host alias to test

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure SSH directory exists
        ssh_dir = os.path.join(os.path.expanduser("~"), ".ssh")
        os.makedirs(ssh_dir, exist_ok=True)

        config_path = os.path.join(ssh_dir, "config")
        if not os.path.exists(config_path):
            print(f"⚠️ SSH config file doesn't exist at {config_path}")
            print("SSH connection test will likely fail.")

        # First ensure we have github.com in known_hosts
        known_hosts_path = os.path.join(ssh_dir, "known_hosts")
        add_to_known_hosts = False

        if os.path.exists(known_hosts_path):
            try:
                with open(known_hosts_path, "r") as kh_file:
                    if "github.com" not in kh_file.read():
                        add_to_known_hosts = True
            except Exception:
                add_to_known_hosts = True
        else:
            add_to_known_hosts = True

        if add_to_known_hosts:
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
            except Exception as e:
                print(f"Warning: Could not add GitHub to known_hosts: {e}")

        # Test SSH connection - use subprocess directly since GitHub returns exit code 1 even when successful
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

            # Combine stdout and stderr for checking since GitHub puts the success message in stderr
            output = result.stdout + result.stderr

            # Check for GitHub's successful authentication message which appears in stderr
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
    setup=True,
    test=True,
    force_new=False,
):
    """
    Check, setup and/or test SSH deploy keys for private GitHub repositories.

    Args:
        repo_name: The name of the repository
        github_org: The GitHub organization
        setup: Whether to perform the setup (generate keys and configure SSH)
        test: Whether to test the SSH connection
        force_new: Whether to force creation of a new key even if one exists

    Returns:
        bool: True if all requested operations were successful
    """
    if not repo_name or not github_org:
        repo_configs = PRIVATE_REPOS_TO_SSH
    else:
        repo_configs = [{"repo_name": repo_name, "github_org": github_org}]

    for repo in repo_configs:
        repo_name = repo["repo_name"]
        github_org = repo["github_org"]
        print(f"Managing deploy key for {github_org}/{repo_name}...")

        # Check if key already exists
        key_exists, key_path, pub_key = check_deploy_key(repo_name)

        if key_exists and not force_new:
            print(f"SSH key already exists at: {key_path}")
            if pub_key:
                print("\nExisting public key:")
                print(f"\n{pub_key}\n")
        elif setup or force_new:
            # Generate new key if requested or if force_new
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

        # Configure SSH if setup is requested
        if setup:
            success, host_alias = configure_ssh_for_repo(key_path, repo_name)
            if success:
                print(f"✅ SSH config updated for {host_alias}")
            else:
                print(f"❌ Failed to update SSH config for {host_alias}")
                return False
        else:
            # Just get the host alias for testing
            host_alias = f"github.com-{repo_name}"

        # Test connection if requested
        if test:
            print(f"\nTesting SSH connection to GitHub for {repo_name}...")
            if not test_ssh_connection(host_alias):
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

        # Provide instructions for using the key
        print(
            "\nTo use this deploy key in your pyproject.toml, add the dependency like this:"
        )
        print(
            f'"algoritmos-gf @ git+ssh://git@{host_alias}/{github_org}/{repo_name}.git",'
        )

    return True


def deploy_continuous():
    """Perform continuous deployment (code changes only)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_bin, pip_bin = set_vars()
    print("Performing continuous deployment (code changes only)...")
    print("TODO: This requires an API to reload code locations instead.")
    # restart_services(env)


def deploy_fast():
    """Perform fast deployment (all dependencies only)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_bin, pip_bin = set_vars()
    print("Performing fast deployment (dependencies only)...")
    install_deps(pip_bin)
    # Try external deps
    setup_deploy_keys()
    install_deps(pip_bin, only_external=True)
    manage_services(env)


def deploy_partial():
    """Perform partial deployment (update Python and all dependencies)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_bin, pip_bin = set_vars()
    print("Performing partial deployment (Python + dependencies)...")
    manage_services(env, action="stop")
    manage_python_version(deploy_dir, venv_dir, python_bin, pip_bin)
    upgrade_pip(python_bin, pip_bin)
    install_deps(pip_bin)
    # Try external deps
    setup_deploy_keys()
    install_deps(pip_bin, only_external=True)
    manage_services(env)


def deploy_full():
    """Perform full deployment (regenerate service templates, update Python and dependencies)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_bin, pip_bin = set_vars()
    print("Performing full deployment (service template, Python, and dependencies)...")
    manage_services(env, action="stop")
    manage_python_version(deploy_dir, venv_dir, python_bin, pip_bin)
    upgrade_pip(python_bin, pip_bin)
    install_deps(pip_bin)
    # Try external deps
    setup_deploy_keys()
    install_deps(pip_bin, only_external=True)
    generate_service(python_bin, deploy_dir)
    manage_services(env)


def setup_git(python_bin=None):
    """Automate Git configuration for commit templates and pre-commit hooks."""
    print("Configuring Git commit template and pre-commit...")
    template_path = os.path.join("templates", "commit-template.git.txt")
    python_bin = python_bin or sys.executable
    try:
        run_cmd(["git", "--version"], error_msg="Git not found")
        run_cmd(
            ["git", "config", "commit.template", template_path],
            error_msg="Failed to set Git commit template",
        )
        print("Git commit template configured successfully.")
        result = subprocess.run(
            [python_bin, "-m", "pip", "show", "pre-commit"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("Installing pre-commit...")
            run_cmd(
                [python_bin, "-m", "pip", "install", "pre-commit"],
                error_msg="Failed to install pre-commit",
            )
        precommit_bin = os.path.join(os.path.dirname(python_bin), "pre-commit")
        print("Installing pre-commit hooks...")
        run_cmd(
            [precommit_bin, "install"], error_msg="Failed to install pre-commit hooks"
        )
        print("Pre-commit hooks installed successfully.")
    except RuntimeError as e:
        print(f"Error setting up Git environment: {e}")
        return False
    return True


def get_dev_dirs():
    deploy_dir = os.path.abspath(os.getcwd())
    venv_dir = os.path.join(deploy_dir, ".venv_main_dagster")
    bin_dir = "Scripts" if platform.system() == "Windows" else "bin"
    python_bin = os.path.join(venv_dir, bin_dir, "python")
    pip_bin = os.path.join(venv_dir, bin_dir, "pip")
    print(f"Deploy dir: {deploy_dir}")
    print(f"Venv dir: {venv_dir}")
    print(f"Python bin: {python_bin}")
    print(f"Pip bin: {pip_bin}")
    return deploy_dir, venv_dir, python_bin, pip_bin


def dev():
    """Set up a platform-agnostic development environment."""
    print("Setting up development environment...")
    deploy_dir, venv_dir, python_bin, pip_bin = get_dev_dirs()
    manage_python_version(deploy_dir, venv_dir, python_bin, pip_bin)
    upgrade_pip(python_bin, pip_bin)
    install_deps(pip_bin, dev=True)
    # Try external deps
    setup_deploy_keys()
    install_deps(pip_bin, only_external=True)
    setup_git(python_bin)


def main():
    parser = argparse.ArgumentParser(description="Deployment script")
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
        ],
        help="Deployment command to run",
    )
    # Add optional arguments for the deploy key commands
    parser.add_argument("--repo", default=None, help="Repository name for deploy keys")
    parser.add_argument(
        "--org",
        default=None,
        help="setup-deploy-key GitHub organization for repository",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="setup-deploy-key Force creation of new deploy key",
    )
    parser.add_argument(
        "--only-external",
        action="store_true",
        help="install-deps Only install external dependencies",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="install-deps Install development dependencies",
    )

    args = parser.parse_args()

    # Map the command to the appropriate function with the right parameters
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
        deploy_dir, venv_dir, python_bin, pip_bin = get_dev_dirs()
        install_deps(pip_bin, dev=args.dev, only_external=args.only_external)
    else:
        # Handle existing commands
        commands = {
            "deploy-full": deploy_full,
            "deploy-partial": deploy_partial,
            "deploy-fast": deploy_fast,
            "deploy-continuous": deploy_continuous,
            "dev": dev,
            "setup-git": setup_git,
        }
        try:
            commands[args.command]()
        except KeyError:
            print("Help: Run with --help for a list of commands.")


if __name__ == "__main__":
    if not check_python_available():
        sys.exit(
            f"Error: At least Python {REQUIRED_MAJOR}.{REQUIRED_MINOR} is required but was not used to execute this script."
        )
    main()
