"""
Entry point for the GitHub connector (Step 1).
Fetches issues, PRs, and commits from GitHub and saves them locally as JSON.

Usage:
    python -m connectors.github_connector.main --repos owner/repo1 owner/repo2
"""

import argparse
import logging

from connectors.github_connector.client import GitHubClient
from connectors.github_connector.producer import LocalProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def ingest_repo(client: GitHubClient, producer: LocalProducer, owner: str, repo: str):
    """Fetch and save all data from a single GitHub repo."""
    logger.info(f"{'='*50}")
    logger.info(f"Ingesting: {owner}/{repo}")
    logger.info(f"{'='*50}")

    # 1. Fetch issues
    logger.info("Fetching issues...")
    issues = client.get_issues(owner, repo, max_pages=2)
    if issues:
        producer.publish_issues(issues)
        logger.info(f"  Found {len(issues)} issues")
        # Preview first 3
        for issue in issues[:3]:
            logger.info(f"    #{issue['number']} - {issue['title'][:60]}")
    else:
        logger.info("  No issues found")

    # 2. Fetch pull requests
    logger.info("Fetching pull requests...")
    prs = client.get_pull_requests(owner, repo, max_pages=2)
    if prs:
        producer.publish_pull_requests(prs)
        logger.info(f"  Found {len(prs)} PRs")
        for pr in prs[:3]:
            logger.info(f"    #{pr['number']} - {pr['title'][:60]}")
    else:
        logger.info("  No PRs found")

    # 3. Fetch commits
    logger.info("Fetching commits...")
    commits = client.get_commits(owner, repo, max_pages=1)
    if commits:
        producer.publish_commits(commits)
        logger.info(f"  Found {len(commits)} commits")
        for commit in commits[:3]:
            msg = commit['commit']['message'].split('\n')[0][:60]
            logger.info(f"    {commit['sha'][:7]} - {msg}")
    else:
        logger.info("  No commits found")

    total = len(issues) + len(prs) + len(commits)
    logger.info(f"Total documents collected: {total}")
    return total


def main():
    parser = argparse.ArgumentParser(
        description="GitHub Connector - Fetches data from GitHub repos"
    )
    parser.add_argument(
        "--repos",
        nargs="+",
        required=True,
        help="GitHub repos to ingest (format: owner/repo)",
    )
    args = parser.parse_args()

    client = GitHubClient()
    producer = LocalProducer()

    grand_total = 0
    try:
        for repo_str in args.repos:
            parts = repo_str.split("/")
            if len(parts) != 2:
                logger.error(f"Invalid repo format: {repo_str}. Use: owner/repo")
                continue
            owner, repo = parts
            grand_total += ingest_repo(client, producer, owner, repo)
    finally:
        filepath = producer.close()

    logger.info(f"\n{'='*50}")
    logger.info(f"DONE! {grand_total} total documents saved to: {filepath}")
    logger.info(f"{'='*50}")


if __name__ == "__main__":
    main()
