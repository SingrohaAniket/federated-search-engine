"""
Centralized configuration loaded from environment variables.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # GitHub
    GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_RAW_TOPIC: str = os.getenv("KAFKA_RAW_TOPIC", "raw-documents")
    KAFKA_NORMALIZED_TOPIC: str = os.getenv("KAFKA_NORMALIZED_TOPIC", "normalized-documents")

    # Elasticsearch
    ELASTICSEARCH_URL: str = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    ELASTICSEARCH_INDEX: str = os.getenv("ELASTICSEARCH_INDEX", "federated-search")

    # Postgres
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "federated_search")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "fse_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "fse_password")

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


config = Config()
