import os

from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 데이터베이스 설정
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB"),
}

# Kafka 설정
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP"),
    "group_id": os.getenv("KAFKA_GROUP", "voc-analyzer-v1"),
    "num_workers": int(os.getenv("WORKER_COUNT", 3)),
    "topic": os.getenv("MAIN_TOPIC"),
    "dlq_topic": os.getenv("DLQ_TOPIC"),
}

# AWS 설정
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("AWS_REGION"),
    "bucket": os.getenv("AWS_S3_BUCKET_NAME"),
}

# NLP 모델 설정
NLP_CONFIG = {
    "model_name": os.getenv("MODEL_NAME"),
    "model_path": os.getenv("MODEL_PATH"),
    "tokenizer": os.getenv("TOKENIZER"),
}

