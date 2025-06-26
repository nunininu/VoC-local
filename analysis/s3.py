import io
import logging

import aioboto3
import pandas as pd

from config import AWS_CONFIG

# 로그 설정
logger = logging.getLogger("s3")


async def save_to_s3_parquet(data: dict, path: str, year: int, month: str) -> None:
    """
    데이터를 Parquet 형식으로 변환하여 S3에 저장한다.

    Args:
        data (dict): 저장할 데이터
        path (str): S3 내 저장 경로
        year (int): 연도
        month (str): 월
    """
    try:
        session = aioboto3.Session()
        async with session.client(
            "s3",
            aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
            aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"],
            region_name=AWS_CONFIG["region_name"],
        ) as s3:
            buffer = io.BytesIO()
            df = pd.DataFrame([data])
            df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            await s3.put_object(
                Bucket=AWS_CONFIG["bucket"],
                Key=path,
                Body=buffer.getvalue(),
            )

    except Exception as e:
        logger.error(f"S3 저장 실패: {e}")


async def save_analysis_result_to_s3(data: dict, year: int, month: str) -> None:
    """
    분석 결과를 S3에 저장한다.

    Args:
        data (dict): 분석 결과 데이터
        year (int): 연도
        month (str): 월
    """
    path = (
        f"meta/analysis_result/year={year}/month={month}/"
        f"{data['analysis_result_id']}.parquet"
    )
    await save_to_s3_parquet(data, path, year, month)


async def save_consulting_to_s3(data: dict, year: int, month: str) -> None:
    """
    상담 데이터를 S3에 저장한다.

    Args:
        data (dict): 상담 데이터
        year (int): 연도
        month (str): 월
    """
    path = (
        f"meta/consultings/year={year}/month={month}/"
        f"{data['consulting_id']}.parquet"
    )
    await save_to_s3_parquet(data, path, year, month)


async def save_dlq_to_s3(data: dict, year: int, month: str) -> None:
    """
    DLQ 데이터를 S3에 저장한다.

    Args:
        data (dict): 실패한 상담 데이터
        year (int): 연도
        month (str): 월
    """
    path = (
        f"meta/dlq/year={year}/month={month}/"
        f"{data['consulting_id']}.parquet"
    )
    data["analysis_status"] = "FAILED"
    await save_to_s3_parquet(data, path, year, month)

