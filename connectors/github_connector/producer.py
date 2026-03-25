"""
Simple local producer that saves raw documents to a JSON file.
This replaces Kafka for Step 1 — we just want to see the data.
Kafka will be added in a later step.
"""

import json
import os
import logging
from datetime import datetime
from typing import List, Dict

from common.schemas import RawDocument, SourceType, DocType

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")


class LocalProducer:
    """Saves raw GitHub data to local JSON files instead of Kafka."""

    def __init__(self, output_dir: str = OUTPUT_DIR):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.documents: List[Dict] = []

    def _save(self, raw_doc: RawDocument):
        """Collect a raw document."""
        self.documents.append(raw_doc.model_dump(mode="json"))

    def publish_issues(self, issues: List[Dict]):
        """Collect raw GitHub issues."""
        for issue in issues:
            raw_doc = RawDocument(
                source=SourceType.GITHUB,
                doc_type=DocType.ISSUE,
                raw_id=str(issue["id"]),
                data=issue,
            )
            self._save(raw_doc)
        logger.info(f"Collected {len(issues)} issues")

    def publish_pull_requests(self, prs: List[Dict]):
        """Collect raw GitHub pull requests."""
        for pr in prs:
            raw_doc = RawDocument(
                source=SourceType.GITHUB,
                doc_type=DocType.PULL_REQUEST,
                raw_id=str(pr["id"]),
                data=pr,
            )
            self._save(raw_doc)
        logger.info(f"Collected {len(prs)} PRs")

    def publish_commits(self, commits: List[Dict]):
        """Collect raw GitHub commits."""
        for commit in commits:
            raw_doc = RawDocument(
                source=SourceType.GITHUB,
                doc_type=DocType.COMMIT,
                raw_id=commit["sha"],
                data=commit,
            )
            self._save(raw_doc)
        logger.info(f"Collected {len(commits)} commits")

    def flush(self):
        """Write all collected documents to a JSON file."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.output_dir, f"github_raw_{timestamp}.json")

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.documents, f, indent=2, default=str)

        logger.info(f"Saved {len(self.documents)} documents to {filepath}")
        return filepath

    def close(self):
        """Flush remaining documents."""
        if self.documents:
            return self.flush()
