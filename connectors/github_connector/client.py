"""
GitHub REST API client for fetching issues, pull requests, and commits.
"""

import time
import logging
import requests
from typing import List, Dict, Any, Optional

from common.config import config

logger = logging.getLogger(__name__)


class GitHubClient:
    """Client for GitHub REST API v3 with pagination and rate-limit handling."""

    BASE_URL = "https://api.github.com"

    def __init__(self, token: Optional[str] = None):
        self.token = token or config.GITHUB_TOKEN
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "FederatedSearchEngine/1.0",
        })
        if self.token:
            self.session.headers["Authorization"] = f"token {self.token}"

    def _request(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """Make a request with rate-limit handling."""
        url = f"{self.BASE_URL}{endpoint}"
        response = self.session.get(url, params=params)

        # Handle rate limiting
        if response.status_code == 403:
            reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_time - int(time.time()), 1)
            logger.warning(f"Rate limited. Waiting {wait}s...")
            time.sleep(wait)
            response = self.session.get(url, params=params)

        response.raise_for_status()
        return response.json()

    def _paginate(self, endpoint: str, params: Optional[Dict] = None, max_pages: int = 10) -> List[Dict]:
        """Fetch all pages of a paginated endpoint."""
        params = params or {}
        params.setdefault("per_page", 100)
        all_items = []

        for page in range(1, max_pages + 1):
            params["page"] = page
            items = self._request(endpoint, params)

            if not items:
                break

            all_items.extend(items)
            logger.info(f"Fetched page {page} from {endpoint} ({len(items)} items)")

            if len(items) < params["per_page"]:
                break

        return all_items

    def get_repo(self, owner: str, repo: str) -> Dict:
        """Fetch repository metadata."""
        logger.info(f"Fetching repo: {owner}/{repo}")
        return self._request(f"/repos/{owner}/{repo}")

    def get_issues(self, owner: str, repo: str, state: str = "all", max_pages: int = 5) -> List[Dict]:
        """Fetch issues (excludes pull requests)."""
        logger.info(f"Fetching issues: {owner}/{repo}")
        items = self._paginate(
            f"/repos/{owner}/{repo}/issues",
            params={"state": state, "sort": "updated", "direction": "desc"},
            max_pages=max_pages,
        )
        # GitHub API returns PRs as issues too; filter them out
        return [item for item in items if "pull_request" not in item]

    def get_pull_requests(self, owner: str, repo: str, state: str = "all", max_pages: int = 5) -> List[Dict]:
        """Fetch pull requests."""
        logger.info(f"Fetching PRs: {owner}/{repo}")
        return self._paginate(
            f"/repos/{owner}/{repo}/pulls",
            params={"state": state, "sort": "updated", "direction": "desc"},
            max_pages=max_pages,
        )

    def get_commits(self, owner: str, repo: str, max_pages: int = 3) -> List[Dict]:
        """Fetch recent commits."""
        logger.info(f"Fetching commits: {owner}/{repo}")
        return self._paginate(
            f"/repos/{owner}/{repo}/commits",
            max_pages=max_pages,
        )
