import json
import logging

from aiokafka import AIOKafkaProducer

from config import KAFKA_CONFIG

# 로그 설정
logger = logging.getLogger("dlq_producer")

# DLQ Kafka 프로듀서 (전역)
dlq_producer: AIOKafkaProducer = None


async def init_dlq_producer() -> None:
    """
    DLQ Kafka Producer를 초기화한다.
    """
    global dlq_producer

    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        enable_idempotence=True,
    )

    await dlq_producer.start()
    logger.info("DLQ Kafka Producer 생성 완료.")


async def send_to_dlq(message: dict) -> None:
    """
    메시지를 DLQ 토픽으로 전송한다.

    Args:
        message (dict): 전송할 Kafka 메시지
    """
    if dlq_producer is None:
        raise RuntimeError("DLQ Producer가 초기화되지 않았습니다.")

    try:
        metadata = await dlq_producer.send_and_wait(
            KAFKA_CONFIG["dlq_topic"], message
        )
        logger.warning(
            f"DLQ 전송 완료: {metadata.topic}[{metadata.partition}]@{metadata.offset}"
        )
    except Exception as e:
        logger.error(f"DLQ 전송 실패: {e}")

