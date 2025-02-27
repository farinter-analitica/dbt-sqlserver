import subprocess


def install_package_from_github(repo_url, branch="main"):
    # Install the package from the specified GitHub repository and branch
    subprocess.run([f"pip install git+{repo_url}@{branch}"], shell=True, check=True)


# Example usage
# Replace 'your_github_repo_url' with your forked repository URL and 'your_branch' with the branch name if not main
install_package_from_github(
    "https://github.com/your_username/your_repository.git", "your_branch"
)
