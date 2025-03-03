#!/usr/bin/env python3
import os
import sys
import subprocess
import platform
import argparse

# Enforce the required Python version (3.12 or higher)
REQUIRED_MAJOR = 3
REQUIRED_MINOR = 12
if sys.version_info < (REQUIRED_MAJOR, REQUIRED_MINOR):
    sys.exit(
        f"Error: Python {REQUIRED_MAJOR}.{REQUIRED_MINOR} or higher is required (currently running {sys.version})."
    )


def check_vars():
    """Ensure required environment variables are provided."""
    if not os.environ.get("ENV"):
        sys.exit("Error: ENV must be explicitly provided (e.g., ENV=staging).")
    if not os.environ.get("DEPLOY_DIR"):
        sys.exit(
            "Error: DEPLOY_DIR must be explicitly provided (e.g., DEPLOY_DIR=/opt/main_dagster_staging)."
        )


def check_os():
    """Ensure the script is running on Linux (for deployment targets)."""
    if platform.system() != "Linux":
        sys.exit("This deployment procedure is intended for Linux systems only.")


def set_vars():
    """Set deployment variables based on the environment."""
    env = os.environ["ENV"]
    deploy_dir = os.environ["DEPLOY_DIR"]
    venv = os.path.join(deploy_dir, ".venv_main_dagster")
    python_bin = os.path.join(venv, "bin", "python")
    pip_bin = os.path.join(venv, "bin", "pip")
    return env, deploy_dir, venv, python_bin, pip_bin


def generate_service(python_bin, deploy_dir):
    """Regenerate service templates (full deployment only)."""
    print("Generating service template...")
    cmd = [
        "sudo",
        python_bin,
        os.path.join(deploy_dir, "scripts", "generate_dagster_service.py"),
    ]
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        sys.stdout.write(result.stdout)
    except subprocess.CalledProcessError as e:
        error_message = (
            f"Error executing command: {' '.join(cmd)}\n"
            f"Return code: {e.returncode}\n"
            f"Standard Output:\n{e.stdout}\n"
            f"Standard Error:\n{e.stderr}"
        )
        raise Exception(error_message) from e


def upgrade_pip(pip_bin):
    """Upgrade pip."""
    print("Upgrading pip...")
    cmd = [pip_bin, "install", "--upgrade", "pip"]
    subprocess.run(cmd, check=True)


def install_deps(pip_bin):
    """Install all required dependencies."""
    print("Installing dependencies...")
    # Check and install required packages if missing
    #   python -c "import jinja2" || pip install jinja2
    #   python -c "import dotenv" || pip install python-dotenv

    cmds = [
        [
            pip_bin,
            "install",
            "jinja2",
        ],
        [
            pip_bin,
            "install",
            "python-dotenv",
        ],
        [
            pip_bin,
            "install",
            "-U",
            "--upgrade-strategy",
            "eager",
            "-e",
            "dagster_shared_gf",
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
    for cmd in cmds:
        subprocess.run(cmd, check=True)


def restart_services(env):
    """Restart the dagster services."""
    print("Restarting services...")
    service1 = f"dagster_{env}_webserver"
    service2 = f"dagster_{env}_daemon"
    cmd = ["sudo", "systemctl", "restart", service1, service2]
    subprocess.run(cmd, check=True)


def deploy_continuous():
    """Perform continuous deployment: code changes only."""
    check_vars()
    check_os()
    env, deploy_dir, venv, python_bin, pip_bin = set_vars()
    print("Performing continuous deployment (code changes only)...")
    print("TODO: This requires an API to reload code locations instead.")
    # restart_services(env)


def deploy_fast():
    """Perform fast deployment: update dependencies only."""
    check_vars()
    check_os()
    env, deploy_dir, venv, python_bin, pip_bin = set_vars()
    print("Performing fast deployment (dependencies only)...")
    install_deps(pip_bin)
    restart_services(env)


def deploy_partial():
    """Perform partial deployment: upgrade pip then update dependencies."""
    check_vars()
    check_os()
    env, deploy_dir, venv, python_bin, pip_bin = set_vars()
    print("Performing partial deployment (Python + dependencies)...")
    upgrade_pip(pip_bin)
    install_deps(pip_bin)
    restart_services(env)


def deploy_full():
    """Perform full deployment: regenerate service templates, upgrade pip, update dependencies."""
    check_vars()
    check_os()
    env, deploy_dir, venv, python_bin, pip_bin = set_vars()
    print("Performing full deployment (service template, Python, and dependencies)...")
    upgrade_pip(pip_bin)
    install_deps(pip_bin)
    generate_service(python_bin, deploy_dir)
    restart_services(env)


def setup_git():
    """Automate Git configuration for commit templates and pre-commit hooks."""
    print("Configuring Git commit template and pre-commit...")

    # Use os.path.join for platform-agnostic path handling
    template_path = os.path.join("templates", "commit-template.git.txt")

    try:
        # Check if git is available
        subprocess.run(["git", "--version"], check=True, stdout=subprocess.PIPE)

        # Configure Git to use the commit template file
        subprocess.run(["git", "config", "commit.template", template_path], check=True)
        print("Git commit template configured successfully.")

        # Check if pre-commit is installed
        result = subprocess.run(
            [sys.executable, "-m", "pip", "show", "pre-commit"],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print("Installing pre-commit...")
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "pre-commit"], check=True
            )

        # Install pre-commit hooks
        print("Installing pre-commit hooks...")
        subprocess.run(["pre-commit", "install"], check=True)
        print("Pre-commit hooks installed successfully.")

    except subprocess.CalledProcessError as e:
        print(f"Error setting up Git environment: {e}")
        print(f"Command '{' '.join(e.cmd)}' failed with exit code {e.returncode}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        return False

    return True


def dev():
    """Set up a platform-agnostic development environment."""
    print("Setting up development environment...")
    venv_dir = os.path.join("..", ".venv_main_dagster")
    subprocess.run([sys.executable, "-m", "venv", venv_dir], check=True)
    # Upgrade Pip
    subprocess.run(
        [
            os.path.join(
                venv_dir,
                "Scripts" if platform.system() == "Windows" else "bin",
                "python",
            ),
            "-m",
            "pip",
            "install",
            "--upgrade",
            "pip",
        ],
        check=True,
    )

    # Determine pip path based on OS
    if platform.system() == "Windows":
        pip_path = os.path.join(venv_dir, "Scripts", "pip")
    else:
        pip_path = os.path.join(venv_dir, "bin", "pip")
    cmds = [
        [
            pip_path,
            "install",
            "-e",
            "dagster_shared_gf[dev]",
            "--config-settings",
            "editable_mode=compat",
        ],
        [
            pip_path,
            "install",
            "-e",
            "dagster_sap_gf",
            "--config-settings",
            "editable_mode=compat",
        ],
        [
            pip_path,
            "install",
            "-e",
            "dagster_kielsa_gf",
            "--config-settings",
            "editable_mode=compat",
        ],
    ]
    for c in cmds:
        subprocess.run(c, check=True)

    setup_git()


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

    if args.command == "deploy-full":
        deploy_full()
    elif args.command == "deploy-partial":
        deploy_partial()
    elif args.command == "deploy-fast":
        deploy_fast()
    elif args.command == "deploy-continuous":
        deploy_continuous()
    elif args.command == "dev":
        dev()
    elif args.command == "setup-git":
        setup_git()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
