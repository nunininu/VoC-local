import os
import json
from google.cloud import storage
from kafka import KafkaProducer

BUCKET_NAME = "wh04-voc"
RAW_DATA_DIR = "tmp/raw/consulting/"
KAFKA_TOPIC = "voc-json"
KAFKA_SERVER = "localhost:9092"

def load_json_from_gcs(bucket_name, prefix):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.name.endswith(".json"):
            content = blob.download_as_text(encoding="utf-8")
            try:
                # 단일 JSON 객체 처리
                data = json.loads(content)

                # 단일 JSON 객체라면 바로 반환
                if isinstance(data, dict):
                    yield data
                else:
                    print(f"  지원되지 않는 형식 (단일 객체가 아님): {blob.name}")

            except json.JSONDecodeError as e:
                print(f"❌ JSON 로딩 오류 ({blob.name}): {e}")

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for record in load_json_from_gcs(BUCKET_NAME, RAW_DATA_DIR):
        try:
            # 단일 JSON 객체 전송
            producer.send(KAFKA_TOPIC, record)
            print(f"✅ 메시지 전송: {record.get('consulting_id', 'UNKNOWN')}")
        except Exception as e:
            print(f"❌ 메시지 전송 오류: {e}")

    #producer.flush()
    print("\n✅ 모든 메시지 전송 완료")

if __name__ == "__main__":
    main()
