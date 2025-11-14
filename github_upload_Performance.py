import subprocess
import sys
from pathlib import Path

REPO_PATH = Path(__file__).resolve().parent  # folder where this script lives
BRANCH = "main"
REMOTE = "origin"


def run(cmd, cwd=None):
    print(f"\n> {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd or REPO_PATH, text=True)
    if result.returncode != 0:
        raise SystemExit(f"Command failed with code {result.returncode}")


def main():
    # Get commit message from command line or use a default
    if len(sys.argv) > 1:
        commit_message = " ".join(sys.argv[1:])
    else:
        commit_message = "Auto-commit from github_upload_Performance.py"

    # 1) git status (optional, just for info)
    run(["git", "status"])

    # 2) git add .
    run(["git", "add", "."])

    # 3) git commit -m "message"
    try:
        run(["git", "commit", "-m", commit_message])
    except SystemExit as e:
        print("\nNo changes to commit or commit failed:")
        print(e)
        print("\nTrying to push anyway…")

    # 4) git push -u origin main
    run(["git", "push", "-u", REMOTE, BRANCH])


if __name__ == "__main__":
    main()
