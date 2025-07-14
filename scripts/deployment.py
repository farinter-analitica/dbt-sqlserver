#!/usr/bin/env python3
import os
import sys
import subprocess
import platform
import argparse

# Minimum Python version for running this script
REQUIRED_MAJOR = 3
REQUIRED_MINOR = 11

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


###############################################################################
# Legacy Environment & SSH Management (Backwards Compatible)
###############################################################################


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
    dev: bool = False,
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
                if dev:
                    # Ask the user if they have added the key to GitHub
                    answer = (
                        input(
                            "\nDid you add the public key to the GitHub repository? [y/N]: "
                        )
                        .strip()
                        .lower()
                    )
                    if not answer.startswith("y"):
                        print(
                            "Aborting. Please add the deploy key to GitHub and try again."
                        )
                if not setup:
                    return False
        print(
            "\nTo use this deploy key in your pyproject.toml, add the dependency like this:"
        )
        print(
            f'"algoritmos-gf @ git+ssh://git@{host_alias}/{github_org}/{repo_name}.git",'
        )
    return True


def reload_code_locations():
    """
    Reload all Dagster code locations by sending the ReloadWorkspaceMutation GraphQL mutation.
    This is equivalent to pressing the "Reload all" button in the Dagster web UI.
    """
    import warnings

    warnings.filterwarnings("ignore")
    from dagster_shared_gf.shared_dagster_api import reload_workspace

    host = os.environ.get("DAGSTER_WEBSERVER_HOST", "localhost")
    port = int(
        os.environ.get(
            "DAGSTER_WEBSERVER_PORT", os.environ.get("DAGSTER_GRAPHQL_PORT", 3000)
        )
    )

    if not reload_workspace(host, port):
        print("Failed to reload code locations.")
        return False

    return True


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
            "check-deploy-key",
            "setup-deploy-key",
            "test-deploy-key",
            "reload-code-locations",
            "manage-services",
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
    parser.add_argument("--test", action="store_true", help="Test the deploy key")
    parser.add_argument("--env", help="Environment to manage")
    parser.add_argument(
        "--action",
        default="start",
        choices=["start", "stop", "restart", "status"],
        help="Action to perform on services",
    )
    parser.add_argument("--dev", action="store_true", help="Development mode")
    args = parser.parse_args()

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
            args.repo,
            args.org,
            setup=True,
            test=args.test or False,
            force_new=args.force,
            dev=args.dev,
        )
    elif args.command == "test-deploy-key":
        return setup_deploy_keys(
            args.repo, args.org, setup=False, test=True, force_new=False, dev=args.dev
        )
    elif args.command == "manage-services":
        return manage_services(env=args.env, action=args.action)
    else:
        commands = {
            "reload-code-locations": reload_code_locations,
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
