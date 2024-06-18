#!/bin/bash

REPO_URL="/opt/main_dagster_dev_bare"
TARGET_DIR="/opt/main_dagster_dev"
BRANCH="dev"

# Step 1: Initialize the existing directory as a Git repository (if not already initialized)
cd "$TARGET_DIR"
if [ ! -d .git ]; then
    git init
fi

# Step 2: Add the remote repository
git remote add origin "$REPO_URL" 2> /dev/null || git remote set-url origin "$REPO_URL"

# Step 3: Fetch the remote branches
git fetch origin

# Step 4: Checkout and reset the branch to match the remote
git checkout -B "$BRANCH" origin/"$BRANCH"
git reset --hard origin/"$BRANCH"

# Step 5: Ensure ignored files are preserved
git clean -fdX  # This removes untracked files, but keeps those listed in .gitignore

echo "Repository initialized and synchronized with the remote branch '$BRANCH'."
