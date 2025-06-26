import asyncio
import json
import logging
import time

from aiokafka import AIOKafkaConsumer

from analysis import handle_analysis
from config import KAFKA_CONFIG
from dlq_producer import send_to_dlq

# 로그 설정
logger = logging.getLogger("consumer")

# 설정
MAX_RETRY = 3


class SubmitMonitor:
    """
    Submit 분석 추적용 클래스
    """
    is_first = False
    start_time = time.time()

    def __init__(self, submit_size: int, label: str):
        self.submit_size = submit_size
        self.label = label
        self.count = 0
        self.success = 0
        self.fail = 0

    def start_new(self, label: str) -> None:
        self.label = label
        self.count = 0
        self.success = 0
        self.fail = 0

    def log_result(self, success: bool = True) -> bool:
        self.count += 1
        if success:
            self.success += 1
        else:
            self.fail += 1

        if self.count >= self.submit_size:
            elapsed = time.time() - self.start_time
            print(f"\n📦 Submit '{self.label}' 완료")
            print(f"   ✅ 성공: {self.success}개")
            print(f"   ❌ 실패: {self.fail}개")
            print(f"   ⏱ 소요 시간: {elapsed:.2f}초\n")
            return True
        return False


# 전역 모니터 인스턴스 생성
monitor = SubmitMonitor(submit_size=10000, label="Submit 1")
submit_number = 1


async def process_message(message: dict, retry_cnt: int = 0) -> None:
    """
    메시지 처리

    Args:
        message (dict): Consumer가 받은 메시지
        retry_cnt (int): 재시도 카운트
    """
    global submit_number, monitor
    if not monitor.is_first:
        monitor.start_time = time.time()
        monitor.is_first = True

    try:
        data = json.loads(message.value.decode("utf-8"))
        await handle_analysis(data)
        success = True
    except Exception as e:
        logger.error(f"❌ [{retry_cnt}/{MAX_RETRY}] 메시지 처리 실패: {e}")
        if retry_cnt >= MAX_RETRY:
            logger.info(f"[{data.get('consulting_id', 'unknown')}] 분석 재시도 초과. DLQ로 전달")
            await send_to_dlq(data)
        else:
            await process_message(message, retry_cnt + 1)
        success = False

    is_submit_done = monitor.log_result(success)
    if is_submit_done:
        submit_number += 1
        monitor.start_new(label=f"Submit {submit_number}")


async def consume_worker(worker_id: int) -> None:
    """
    비동기 Consumer 생성 및 메시지 수신 대기

    Args:
        worker_id (int): Worker ID
    """
    logger.info(f"워커 #{worker_id} 시작")
    consumer = AIOKafkaConsumer(
        KAFKA_CONFIG["topic"],
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        group_id=KAFKA_CONFIG["group_id"],
        enable_auto_commit=True,
        auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        async for message in consumer:
            logger.info(f"[# {worker_id}] 메시지 수신: {message.key} {message.offset}")
            await process_message(message)
    finally:
        await consumer.stop()


async def start_kafka_workers() -> None:
    """
    Worker 수 대로 Consumer 작업 시작
    """
    workers = [consume_worker(i) for i in range(KAFKA_CONFIG["num_workers"])]
    await asyncio.gather(*workers)

