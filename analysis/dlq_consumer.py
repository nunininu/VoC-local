import asyncio
import json
import logging
from datetime import datetime

from aiokafka import AIOKafkaConsumer

from config import KAFKA_CONFIG
from s3 import save_dlq_to_s3

# 로그 설정
logger = logging.getLogger("dlq_consumer")


async def handle_dlq_message(payload: dict) -> None:
    """
    DLQ 메시지를 처리한다.

    Args:
        payload (dict): Kafka로부터 수신된 실패 상담 데이터
    """
    consulting_id = payload.get("consulting_id", "unknown")
    logger.warning(f"DLQ 처리: consulting_id={consulting_id} 실패 메시지 기록")

    dt_timestamp = datetime.now()
    dt = dt_timestamp.fromisoformat(payload["consulting_datetime"])

    await save_dlq_to_s3(payload, dt.year, f"{dt.month:02d}")


async def start_dlq_consumer() -> None:
    """
    DLQ 토픽을 수신하는 Kafka Consumer를 비동기로 실행한다.
    """
    consumer = AIOKafkaConsumer(
        KAFKA_CONFIG["dlq_topic"],
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        group_id=f"{KAFKA_CONFIG['group_id']}-dlq",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info("DLQ Consumer 시작됨")

    try:
        async for message in consumer:
            payload = json.loads(message.value.decode("utf-8"))
            await handle_dlq_message(payload)
    finally:
        await consumer.stop()


async def main() -> None:
    """
    DLQ Consumer 실행 엔트리 포인트
    """
    await start_dlq_consumer()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"❌ DLQ Consumer 에러 발생: {e}")

