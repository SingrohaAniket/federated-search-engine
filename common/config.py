"""
Centralized configuration loaded from environment variables.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # GitHub (Step 1)
    GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")

    # Kafka (Step 2)
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_RAW_TOPIC: str = os.getenv("KAFKA_RAW_TOPIC", "raw-documents")


config = Config()
