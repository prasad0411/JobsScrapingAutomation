#!/usr/bin/env python3

import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

BACKUP_FOLDER = "../job-tracker-secrets"

FILES_TO_BACKUP = [
    "credentials.json",
    "gmail_credentials.json",
    "gmail_token.pickle",
    "jobright_cookies.json",
    "nu_cookies.json",
    "processed_emails.json",
    "workday_mapping.json",
    ".env",
]


def backup_to_private_repo():
    print("=" * 80)
    print("AUTOMATED BACKUP TO PRIVATE REPO")
    print("=" * 80)

    project_dir = Path(__file__).parent
    backup_dir = project_dir / BACKUP_FOLDER

    if not backup_dir.exists():
        print(f"✗ Backup directory not found: {backup_dir}")
        print(f"  Create private repo first!")
        return False

    backed_up = []
    missing = []

    for filename in FILES_TO_BACKUP:
        source = project_dir / filename
        destination = backup_dir / filename

        if source.exists():
            try:
                shutil.copy2(source, destination)
                backed_up.append(filename)
                print(f"  ✓ Backed up: {filename}")
            except Exception as e:
                print(f"  ✗ Failed to backup {filename}: {e}")
        else:
            missing.append(filename)

    timestamp_file = backup_dir / "last_backup.txt"
    with open(timestamp_file, "w") as f:
        f.write(f"Last backup: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Files backed up: {len(backed_up)}\n")
        f.write(f"Files: {', '.join(backed_up)}\n")

    print(f"\n  Backed up {len(backed_up)} files")
    if missing:
        print(f"  Skipped {len(missing)} missing files: {', '.join(missing)}")

    try:
        os.chdir(backup_dir)

        subprocess.run(["git", "add", "."], check=True, capture_output=True)

        commit_msg = f"Auto-backup {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        result = subprocess.run(
            ["git", "commit", "-m", commit_msg], capture_output=True, text=True
        )

        if "nothing to commit" in result.stdout:
            print(f"\n  ℹ No changes since last backup")
            return True

        subprocess.run(
            ["git", "push", "origin", "main"], check=True, capture_output=True
        )

        print(f"\n  ✓ Pushed to private GitHub repo")
        print("=" * 80)
        return True

    except subprocess.CalledProcessError as e:
        print(f"\n  ✗ Git error: {e}")
        print("=" * 80)
        return False
    except Exception as e:
        print(f"\n  ✗ Backup failed: {e}")
        print("=" * 80)
        return False


if __name__ == "__main__":
    success = backup_to_private_repo()
    exit(0 if success else 1)
