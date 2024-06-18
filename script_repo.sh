#!/bin/bash

REPO_URL="ssh://bpadilla@farinter.net@172.16.2.235/opt/main_dagster_dev_bare"
TARGET_DIR="/opt/main_dagster_dev"
BRANCH="dev"
BACKUP_DIR="/tmp/backup_dev"

# Step 1: Initialize the existing directory as a Git repository (if not already initialized)
cd "$TARGET_DIR"
if [ ! -d .git ]; then
    git init
fi

# Step 2: Add the remote repository
git remote add origin "$REPO_URL" 2> /dev/null || git remote set-url origin "$REPO_URL"

# Step 3: Backup untracked and ignored files
mkdir -p "$BACKUP_DIR"
rsync -av --exclude='.git' "$TARGET_DIR/" "$BACKUP_DIR/"

# Step 4: Fetch the remote branches
git fetch origin

# Step 5: Checkout and reset the branch to match the remote
git checkout -B "$BRANCH" origin/"$BRANCH"
git reset --hard origin/"$BRANCH"

# Step 6: Restore the untracked and ignored files
rsync -av --exclude='.git' "$BACKUP_DIR/" "$TARGET_DIR/"

# Step 7: Clean up backup
rm -rf "$BACKUP_DIR"

echo "Repository initialized and synchronized with the remote branch '$BRANCH', preserving untracked files."
