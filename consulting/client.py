import os
import json
import random
from datetime import datetime
from faker import Faker


# Constants
OUTPUT_FOLDER = "/data/client/"
AGE_GROUPS = [
    "10대", "20대", "30대", "40대", "50대", "60대", "70대 이상"
]
AGE_GROUP_WEIGHTS = [1, 3, 3, 3, 2, 2, 1]
GENDERS = ["남성", "여성"]
NUM_CLIENTS = 2000


def generate_clients(num_clients: int, output_folder: str) -> None:
    """
    Faker를 이용해 랜덤한 고객 데이터를 생성하고 JSON 파일로 저장한다.

    Args:
        num_clients (int): 생성할 고객 수
        output_folder (str): 저장할 디렉토리 경로
    """
    faker = Faker("ko_KR")
    os.makedirs(output_folder, exist_ok=True)

    client_ids = set()

    for _ in range(num_clients):
        client_id = faker.uuid4()

        while client_id in client_ids:
            client_id = faker.uuid4()
        client_ids.add(client_id)

        age_group = random.choices(AGE_GROUPS, weights=AGE_GROUP_WEIGHTS, k=1)[0]
        gender = random.choice(GENDERS)

        client = {
            "client_id": client_id,
            "client_name": faker.name(),
            "age": age_group,
            "gender": gender,
            "signup_datetime": faker.date_time_between(
                start_date="-5y", end_date="now"
            ).isoformat(),
            "latest_consulting_datetime": faker.date_time_between(
                start_date="-1y", end_date="now"
            ).isoformat(),
            "is_terminated": random.choice([True, False])
        }

        file_path = os.path.join(output_folder, f"{client_id}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(client, f, ensure_ascii=False, indent=4)

    print("✅ 고객 데이터 생성 완료!")


if __name__ == "__main__":
    generate_clients(NUM_CLIENTS, OUTPUT_FOLDER)

