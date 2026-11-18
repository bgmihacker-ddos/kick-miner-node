import os
import subprocess
import threading
import time
from typing import List, Optional
from loguru import logger

DEFAULT_FILES_TO_SYNC = [
    "bets_history.csv",
    "learning_profile.json",
    "session_state.json"
]


class GitHubSync:
    """
    Automated background sync engine for Render / ephemeral environments.
    Pulls historical data & learning state on startup, and periodically commits & pushes
    updated bet history and model weights back to your private GitHub repository.
    """

    def __init__(self, config: Optional[dict] = None):
        cfg = (config or {}).get("GitHubSync", {})
        self.enabled = bool(
            cfg.get("enabled", True) and (
                os.environ.get("GITHUB_TOKEN")
                or os.environ.get("GH_TOKEN")
                or cfg.get("token")
            )
        )
        self.token = (
            os.environ.get("GITHUB_TOKEN")
            or os.environ.get("GH_TOKEN")
            or cfg.get("token", "")
        ).strip()
        self.repo = (
            os.environ.get("GITHUB_REPO")
            or cfg.get("repo", "")
        ).strip()
        self.branch = (
            os.environ.get("GITHUB_BRANCH")
            or cfg.get("branch", "main")
        ).strip()
        self.interval = int(cfg.get("interval", 600))  # default 10 mins
        self.files: List[str] = cfg.get("files", DEFAULT_FILES_TO_SYNC)

        self._lock = threading.Lock()
        self._sync_requested = threading.Event()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        if self.enabled:
            logger.info("🐙 GitHub Auto-Sync: ENABLED (Private repo persistence)")
            self._setup_git_credentials()
        else:
            logger.debug("🐙 GitHub Auto-Sync: Disabled (no GITHUB_TOKEN provided)")

    def _run_cmd(self, args: List[str], timeout: int = 40) -> tuple[int, str]:
        try:
            res = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False
            )
            out = (res.stdout or "") + (res.stderr or "")
            return res.returncode, out.strip()
        except Exception as e:
            return -1, str(e)

    def _setup_git_credentials(self):
        with self._lock:
            # Check if .git directory exists. If not, initialize it for persistence.
            if not os.path.isdir(".git"):
                logger.info("🐙 Initializing git repository for auto-sync persistence...")
                self._run_cmd(["git", "init"])
                self._run_cmd(["git", "config", "--global", "--add", "safe.directory", "*"])

            # Set git identity if not configured
            self._run_cmd(["git", "config", "user.name", "KickMiner-Bot"])
            self._run_cmd(["git", "config", "user.email", "bot@kickminer.local"])

            # Configure authenticated remote URL if token is present
            if self.token:
                ret, current_remote = self._run_cmd(["git", "remote", "get-url", "origin"])
                if ret == 0 and current_remote:
                    clean_remote = current_remote
                    # Strip any existing tokens in URL
                    if "@github.com" in clean_remote:
                        # Extract repo path
                        parts = clean_remote.split("@github.com")[-1].lstrip(":/")
                        if not self.repo:
                            self.repo = parts.removesuffix(".git")
                    elif "github.com/" in clean_remote:
                        parts = clean_remote.split("github.com/")[-1].lstrip("/")
                        if not self.repo:
                            self.repo = parts.removesuffix(".git")

                if self.repo:
                    auth_url = f"https://x-access-token:{self.token}@github.com/{self.repo}.git"
                    if ret == 0:
                        self._run_cmd(["git", "remote", "set-url", "origin", auth_url])
                    else:
                        self._run_cmd(["git", "remote", "add", "origin", auth_url])
                    logger.info(f"🐙 Git remote configured for repository: {self.repo}")

    def pull_on_startup(self):
        """Restores bets_history.csv and learning_profile.json on container startup."""
        if not self.enabled:
            return

        with self._lock:
            logger.info("🐙 Pulling latest persistent state from GitHub...")
            self._run_cmd(["git", "fetch", "origin", self.branch])
            code, out = self._run_cmd(["git", "pull", "--rebase", "origin", self.branch])
            if code == 0:
                logger.success("🐙 Latest data successfully restored from GitHub repository")
            else:
                c_code, c_out = self._run_cmd(["git", "checkout", "-B", self.branch, f"origin/{self.branch}"])
                if c_code == 0:
                    logger.success("🐙 Latest data successfully restored from GitHub repository")
                else:
                    logger.warning(f"🐙 Initial git pull warning: {out[:120] if out else c_out[:120]}")

    def sync_now(self):
        """Signals the background thread to trigger an immediate commit & push."""
        if self.enabled:
            self._sync_requested.set()

    def _commit_and_push(self):
        with self._lock:
            # Check which of our target files exist and are modified
            files_to_add = [f for f in self.files if os.path.exists(f)]
            if not files_to_add:
                return

            code, status = self._run_cmd(["git", "status", "--porcelain"] + files_to_add)
            if code != 0 or not status.strip():
                # No changes to sync
                return

            logger.info("🐙 Changes detected in persistent files, syncing to GitHub...")

            # Stage only our persistent data files
            self._run_cmd(["git", "add"] + files_to_add)

            commit_msg = "chore(sync): update bet history & learning state [skip ci]"
            c_code, c_out = self._run_cmd(["git", "commit", "-m", commit_msg])
            if c_code != 0 and "nothing to commit" not in c_out:
                logger.warning(f"🐙 Git commit note: {c_out[:100]}")
                return

            # Rebase against remote before push to prevent rejection
            self._run_cmd(["git", "pull", "--rebase", "-X", "theirs", "origin", self.branch])

            p_code, p_out = self._run_cmd(["git", "push", "origin", f"HEAD:{self.branch}"])
            if p_code == 0:
                logger.success("🐙 Successfully pushed bet history & learning state to GitHub!")
            else:
                logger.error(f"🐙 Git push failed: {p_out[:150]}")

    def _worker(self):
        last_sync = time.time()
        while not self._stop_event.is_set():
            # Wait for either interval timeout or an explicit sync request
            triggered = self._sync_requested.wait(timeout=5)
            if self._stop_event.is_set():
                break

            now = time.time()
            if triggered:
                self._sync_requested.clear()
                # Debounce rapid triggers (wait 5s before pushing)
                time.sleep(5)
                self._commit_and_push()
                last_sync = time.time()
            elif now - last_sync >= self.interval:
                self._commit_and_push()
                last_sync = now

    def start(self):
        if not self.enabled:
            return
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._worker, daemon=True, name="GitHubAutoSync")
        self._thread.start()
        logger.info(f"🐙 GitHub Auto-Sync background thread started (interval: {self.interval}s)")

    def stop(self):
        self._stop_event.set()
        self._sync_requested.set()
        if self._thread:
            self._thread.join(timeout=2)
