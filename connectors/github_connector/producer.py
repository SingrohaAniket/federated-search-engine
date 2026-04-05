"""
Kafka producer for publishing raw GitHub documents to the 'raw-documents' topic.
Step 2: This replaces LocalProducer from Step 1.
"""

import json
import logging
from typing import List, Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError

from common.config import config
from common.schemas import RawDocument, SourceType, DocType

logger = logging.getLogger(__name__)


class GitHubKafkaProducer:
    """Publishes raw GitHub data to Kafka topic 'raw-documents'."""

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            # Serialize values to JSON bytes
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            # Key is a string like "github:issue:12345" — used by Kafka for partitioning
            key_serializer=lambda k: k.encode("utf-8"),
            # Wait for all replicas to acknowledge (safe delivery)
            acks="all",
            retries=3,
        )
        self.topic = config.KAFKA_RAW_TOPIC
        logger.info(f"Kafka producer connected to {config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Publishing to topic: {self.topic}")

    def _publish(self, raw_doc: RawDocument):
        """Publish a single raw document to Kafka."""
        # Key format: "github:issue:12345"
        # Kafka uses this key to decide which partition the message goes to.
        # Same key = always same partition = order guaranteed per document.
        key = f"{raw_doc.source.value}:{raw_doc.doc_type.value}:{raw_doc.raw_id}"

        self.producer.send(
            self.topic,
            key=key,
            value=raw_doc.model_dump(mode="json"),
        )

    def publish_issues(self, issues: List[Dict]):
        """Publish raw GitHub issues to Kafka."""
        for issue in issues:
            raw_doc = RawDocument(
                source=SourceType.GITHUB,
                doc_type=DocType.ISSUE,
                raw_id=str(issue["id"]),
                data=issue,
            )
            self._publish(raw_doc)

        # flush() blocks until all pending messages are delivered
        self.producer.flush()
        logger.info(f"Published {len(issues)} issues to Kafka topic '{self.topic}'")

    def publish_pull_requests(self, prs: List[Dict]):
        """Publish raw GitHub pull requests to Kafka."""
        for pr in prs:
            raw_doc = RawDocument(
                source=SourceType.GITHUB,
                doc_type=DocType.PULL_REQUEST,
                raw_id=str(pr["id"]),
                data=pr,
            )
            self._publish(raw_doc)

        self.producer.flush()
        logger.info(f"Published {len(prs)} PRs to Kafka topic '{self.topic}'")

    def publish_commits(self, commits: List[Dict]):
        """Publish raw GitHub commits to Kafka."""
        for commit in commits:
            raw_doc = RawDocument(
                source=SourceType.GITHUB,
                doc_type=DocType.COMMIT,
                raw_id=commit["sha"],
                data=commit,
            )
            self._publish(raw_doc)

        self.producer.flush()
        logger.info(f"Published {len(commits)} commits to Kafka topic '{self.topic}'")

    def close(self):
        """Cleanly close the Kafka connection."""
        self.producer.close()
        logger.info("Kafka producer closed")
