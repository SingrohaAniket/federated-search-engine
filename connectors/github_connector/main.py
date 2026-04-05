"""
Entry point for the GitHub connector (Step 2).
Fetches issues, PRs, and commits from GitHub and publishes them to Kafka.

Usage:
    python -m connectors.github_connector.main --repos owner/repo1 owner/repo2
"""

import argparse
import logging

from connectors.github_connector.client import GitHubClient
from connectors.github_connector.producer import GitHubKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def ingest_repo(client: GitHubClient, producer: GitHubKafkaProducer, owner: str, repo: str):
    """Fetch all data from a GitHub repo and publish each doc to Kafka."""
    logger.info(f"{'='*50}")
    logger.info(f"Ingesting: {owner}/{repo}")
    logger.info(f"{'='*50}")

    # 1. Issues
    logger.info("Fetching issues...")
    issues = client.get_issues(owner, repo, max_pages=2)
    if issues:
        producer.publish_issues(issues)
        logger.info(f"  → {len(issues)} issues sent to Kafka")
        for issue in issues[:3]:
            logger.info(f"    #{issue['number']} - {issue['title'][:60]}")
    else:
        logger.info("  No issues found")

    # 2. Pull Requests
    logger.info("Fetching pull requests...")
    prs = client.get_pull_requests(owner, repo, max_pages=2)
    if prs:
        producer.publish_pull_requests(prs)
        logger.info(f"  → {len(prs)} PRs sent to Kafka")
        for pr in prs[:3]:
            logger.info(f"    #{pr['number']} - {pr['title'][:60]}")
    else:
        logger.info("  No PRs found")

    # 3. Commits
    logger.info("Fetching commits...")
    commits = client.get_commits(owner, repo, max_pages=1)
    if commits:
        producer.publish_commits(commits)
        logger.info(f"  → {len(commits)} commits sent to Kafka")
        for commit in commits[:3]:
            msg = commit['commit']['message'].split('\n')[0][:60]
            logger.info(f"    {commit['sha'][:7]} - {msg}")
    else:
        logger.info("  No commits found")

    total = len(issues) + len(prs) + len(commits)
    logger.info(f"Total documents published to Kafka: {total}")
    return total


def main():
    parser = argparse.ArgumentParser(
        description="GitHub Connector — publishes to Kafka"
    )
    parser.add_argument(
        "--repos",
        nargs="+",
        required=True,
        help="GitHub repos to ingest (format: owner/repo)",
    )
    args = parser.parse_args()

    client = GitHubClient()
    producer = GitHubKafkaProducer()

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
        producer.close()

    logger.info(f"\n{'='*50}")
    logger.info(f"DONE! {grand_total} documents published to Kafka topic 'raw-documents'")
    logger.info(f"{'='*50}")


if __name__ == "__main__":
    main()
