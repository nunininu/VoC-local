import asyncio
import logging

from consumer import start_kafka_workers
from db import init_db_pool
from dlq_producer import init_dlq_producer

# 로그 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("main")


async def main() -> None:
    """
    서비스 초기화 및 Kafka Consumer 시작
    """
    await init_db_pool()
    logger.info("PostgreSQL 풀 생성 완료")

    await init_dlq_producer()
    await start_kafka_workers()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"❌ 에러 발생. 서비스 종료\n{e}")

