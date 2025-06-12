import os
import json
import uuid
import random
from datetime import datetime, timedelta


# Constants
CONSULTING_JSON_DIR = "/data/txt_to_json/"
CLIENTS_DIR = "/data/client/"
OUTPUT_DIR = "/data/consulting_add_client/"

START_DATE = datetime(2020, 5, 27)
END_DATE = datetime(2025, 6, 30)
TOTAL_DAYS = (END_DATE - START_DATE).days


def load_client_ids(directory: str) -> list:
    """
    클라이언트 JSON 파일에서 client_id 목록을 추출한다.

    Args:
        directory (str): 클라이언트 JSON 파일들이 위치한 경로

    Returns:
        list: client_id 문자열 리스트
    """
    client_ids = []
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            path = os.path.join(directory, filename)
            with open(path, "r", encoding="utf-8") as f:
                client_data = json.load(f)
                client_ids.append(client_data["client_id"])
    return client_ids


def assign_consultings_to_clients(consulting_files: list, client_ids: list) -> dict:
    """
    상담 파일을 클라이언트에 순차적으로 할당한다.

    Args:
        consulting_files (list): 상담 파일 리스트
        client_ids (list): 클라이언트 ID 리스트

    Returns:
        dict: client_id를 키로 하고 파일 이름 리스트를 값으로 갖는 딕셔너리
    """
    client_sessions = {cid: [] for cid in client_ids}
    num_clients = len(client_ids)

    for idx, file_name in enumerate(consulting_files):
        assigned_client = client_ids[idx % num_clients]
        client_sessions[assigned_client].append(file_name)

    return client_sessions


def enrich_consulting_files(
    consulting_json_dir: str,
    output_dir: str,
    client_sessions: dict
) -> None:
    """
    상담 파일에 client_id, consulting_datetime 등을 추가하고 저장한다.

    Args:
        consulting_json_dir (str): 원본 상담 JSON 디렉토리
        output_dir (str): 결과 저장 디렉토리
        client_sessions (dict): client_id -> 파일 목록 딕셔너리
    """
    os.makedirs(output_dir, exist_ok=True)

    for client_id, files in client_sessions.items():
        current_dt = START_DATE + timedelta(
            days=random.randint(0, TOTAL_DAYS - len(files) * 10)
        )

        for file_name in files:
            file_path = os.path.join(consulting_json_dir, file_name)

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                content = data.get("consulting_content", "")
                turn_count = content.count("고객:") + content.count("상담사:")

                time_gap = timedelta(
                    days=random.randint(1, 10),
                    hours=random.randint(9, 19),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                current_dt += time_gap

                enriched = {
                    "consulting_id": str(uuid.uuid4()),
                    "client_id": client_id,
                    "category_id": data.get("consulting_category", None),
                    "channel_id": random.choice([0, 1]),
                    "consulting_datetime": current_dt.isoformat(),
                    "turns": turn_count,
                    "content": content,
                    "analysis_status": "pending"
                }

            except Exception as e:
                print(f"❌ {file_name} 처리 실패: {e}")
                enriched = {
                    "consulting_id": str(uuid.uuid4()),
                    "client_id": client_id,
                    "category_id": None,
                    "channel_id": None,
                    "consulting_datetime": None,
                    "turns": 0,
                    "content": "",
                    "analysis_status": "fail"
                }

            out_path = os.path.join(output_dir, file_name)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(enriched, f, ensure_ascii=False, indent=4)

            print(f"✔ saved: {file_name}")


def main() -> None:
    client_ids = load_client_ids(CLIENTS_DIR)
    consulting_files = sorted([
        f for f in os.listdir(CONSULTING_JSON_DIR) if f.endswith(".json")
    ])
    client_sessions = assign_consultings_to_clients(consulting_files, client_ids)
    enrich_consulting_files(CONSULTING_JSON_DIR, OUTPUT_DIR, client_sessions)
    print("✅ 전체 상담 파일 처리 및 상태 부여 완료!")


if __name__ == "__main__":
    main()

