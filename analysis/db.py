import asyncpg

from config import DB_CONFIG

# DB 커넥션 풀 (전역)
pool = None


async def init_db_pool() -> None:
    """
    PostgreSQL 커넥션 풀을 초기화한다.
    """
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(**DB_CONFIG)


def get_db_pool():
    """
    커넥션 풀을 반환한다.

    Returns:
        asyncpg.Pool: DB 커넥션 풀 객체
    """
    return pool


async def save_analysis_result(result: dict) -> None:
    """
    분석 결과를 RDS에 저장한다.

    Args:
        result (dict): 분석 결과 데이터
    """
    pool = get_db_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():
            # 분석 결과 저장
            await conn.execute(
                """
                INSERT INTO analysis_result (
                    analysis_result_id, consulting_id, client_id,
                    keywords, is_negative, created_datetime
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                result["analysis_result_id"],
                result["consulting_id"],
                result["client_id"],
                result["keywords"],
                result["is_negative"],
                result["created_datetime"],
            )

            # 키워드 개별 저장
            for keyword in result.get("keyword_list", []):
                await conn.execute(
                    """
                    INSERT INTO analysis_keyword (
                        analysis_result_id, keyword
                    )
                    VALUES ($1, $2)
                    """,
                    result["analysis_result_id"],
                    keyword,
                )

            # 상담 상태 업데이트
            await conn.execute(
                """
                UPDATE consulting
                SET analysis_status = 'SUCCESSED'
                WHERE consulting_id = $1
                """,
                result["consulting_id"],
            )

