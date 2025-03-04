#!/usr/bin/env python3
import os
import sys
import subprocess
import platform
import argparse

# Enforce the minimum Python version (3.11+)
REQUIRED_MAJOR = 3
REQUIRED_MINOR = 11


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


def install_deps(pip_bin, dev=False):
    """Install required dependencies (with optional development extras)."""
    print("Installing dependencies...")
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
            "dagster_shared_gf[dev]" if dev else "dagster_shared_gf",
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
    for cmd in packages:
        run_cmd(cmd, error_msg="Dependency installation failed")


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


def deploy_continuous():
    """Perform continuous deployment (code changes only)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_bin, pip_bin = set_vars()
    print("Performing continuous deployment (code changes only)...")
    print("TODO: This requires an API to reload code locations instead.")
    # restart_services(env)


def deploy_fast():
    """Perform fast deployment (dependencies only)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_bin, pip_bin = set_vars()
    print("Performing fast deployment (dependencies only)...")
    install_deps(pip_bin)
    manage_services(env)


def deploy_partial():
    """Perform partial deployment (update Python and dependencies)."""
    check_vars()
    check_os()
    env, deploy_dir, venv_dir, python_bin, pip_bin = set_vars()
    print("Performing partial deployment (Python + dependencies)...")
    manage_services(env, action="stop")
    manage_python_version(deploy_dir, venv_dir, python_bin, pip_bin)
    upgrade_pip(python_bin, pip_bin)
    install_deps(pip_bin)
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


def dev():
    """Set up a platform-agnostic development environment."""
    print("Setting up development environment...")
    deploy_dir = os.path.abspath(os.getcwd())
    venv_dir = os.path.join(deploy_dir, ".venv_main_dagster")
    bin_dir = "Scripts" if platform.system() == "Windows" else "bin"
    python_bin = os.path.join(venv_dir, bin_dir, "python")
    pip_bin = os.path.join(venv_dir, bin_dir, "pip")
    print(f"Deploy dir: {deploy_dir}")
    print(f"Venv dir: {venv_dir}")
    print(f"Python bin: {python_bin}")
    print(f"Pip bin: {pip_bin}")
    manage_python_version(deploy_dir, venv_dir, python_bin, pip_bin)
    upgrade_pip(python_bin, pip_bin)
    install_deps(pip_bin, dev=True)
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
            "setup-git",
        ],
        help="Deployment command to run",
    )
    args = parser.parse_args()
    commands = {
        "deploy-full": deploy_full,
        "deploy-partial": deploy_partial,
        "deploy-fast": deploy_fast,
        "deploy-continuous": deploy_continuous,
        "dev": dev,
        "setup-git": setup_git,
    }
    commands[args.command]()


if __name__ == "__main__":
    if not check_python_available():
        sys.exit(
            f"Error: At least Python {REQUIRED_MAJOR}.{REQUIRED_MINOR} is required but was not used to execute this script."
        )
    main()
