"""
Shared document schemas for the Federated Search Engine.
All data flowing through the pipeline uses these models.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


class SourceType(str, Enum):
    GITHUB = "github"
    NOTION = "notion"
    SLACK = "slack"
    GOOGLE_DOCS = "google_docs"
    GMAIL = "gmail"


class DocType(str, Enum):
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    COMMIT = "commit"
    REPOSITORY = "repository"
    PAGE = "page"
    MESSAGE = "message"
    DOCUMENT = "document"
    EMAIL = "email"


class RawDocument(BaseModel):
    """Raw document as fetched from a source platform, before normalization."""
    source: SourceType
    doc_type: DocType
    raw_id: str = Field(description="Original ID from the source platform")
    data: Dict[str, Any] = Field(description="Raw JSON payload from the source")
    fetched_at: datetime = Field(default_factory=datetime.utcnow)


class NormalizedDocument(BaseModel):
    """Common document format used across the entire pipeline after normalization."""
    id: str = Field(description="Unique ID: {source}:{doc_type}:{raw_id}")
    source: SourceType
    doc_type: DocType
    title: str
    content: str
    url: str
    timestamp: datetime
    author: str
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional source-specific fields (labels, state, language, etc.)"
    )

    def to_es_doc(self) -> Dict[str, Any]:
        """Convert to Elasticsearch-compatible dict."""
        return {
            "id": self.id,
            "source": self.source.value,
            "doc_type": self.doc_type.value,
            "title": self.title,
            "content": self.content,
            "url": self.url,
            "timestamp": self.timestamp.isoformat(),
            "author": self.author,
            "metadata": self.metadata,
        }
