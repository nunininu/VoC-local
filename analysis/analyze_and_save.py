import os
import re
import io
import json
import uuid
import torch
import boto3
import logging
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
from konlpy.tag import Okt
from keybert import KeyBERT
from sklearn.metrics.pairwise import cosine_similarity
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sentence_transformers import SentenceTransformer

from psycopg2.extensions import connection as PgConnection

load_dotenv()

logging.getLogger("transformers").setLevel(logging.ERROR)

okt = Okt()
kw_model = KeyBERT()
sentence_model = SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS")
kw_model_extended = KeyBERT("snunlp/KR-SBERT-V40K-klueNLI-augSTS")
sentiment_model_path = "/sentiment_model/checkpoint-150"
sentiment_model = AutoModelForSequenceClassification.from_pretrained(sentiment_model_path)
sentiment_tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")
sentiment_model.eval()

KOREAN_STOPWORDS = [
    "그냥", "그리고", "근데", "음", "아", "어", "뭐", "그죠", "그러니까",
    "그거", "저기", "부터", "그게", "전화", "전화기", "수고", "대해"
]

category_map = {
    0: "요금 안내", 1: "요금 납부", 2: "요금제 변경", 3: "선택약정 할인", 4: "납부 방법 변경",
    5: "부가서비스 안내", 6: "소액 결제", 7: "휴대폰 정지/분실/파손", 8: "기기변경", 9: "내역 조회",
    10: "명의/번호/유심 해지", 11: "결합할인", 12: "가입 안내", 13: "통화품질", 14: "장비 안내",
    15: "인터넷 속도/품질", 16: "요금 할인", 17: "인터넷 장애/고장", 18: "정보변경", 19: "청구서",
    20: "복지할인 등록", 21: "납부확인서", 22: "군입대 일시정지", 23: "기기할부 약정", 24: "멤버십 혜택",
    25: "보험 안내", 26: "해외로밍", 27: "명의/번호/유심 일시정지", 28: "개통점 안내", 29: "명의/번호/유심 변경",
    30: "상품 안내", 31: "제휴카드로 할인", 32: "배송문의", 33: "장기고객 혜택", 34: "기타 할인 혜택",
    35: "번호 변경", 36: "데이터 옵션 안내", 37: "세금계산서", 38: "인터넷 가입 및 변경", 39: "위약금 안내",
    40: "해지 안내", 41: "보험 해지", 42: "보험 청구", 43: "자동 이체", 44: "명의/번호/유심 일시정지",
    45: "한도 안내", 46: "파손보험 안내", 47: "약정 안내", 48: "카드 배송", 49: "보험 혜택",
    50: "납부 확인서", 51: "청구지 변경", 52: "번호 이동", 53: "인터넷전화가입 및 변경", 54: "환불 안내",
    55: "이전 설치", 56: "유심 문제", 57: "반품 안내", 58: "보험 신청", 59: "번호이동"
}

DOMAIN_KEYWORDS = [
    "약정", "할인", "데이터", "기기", "변경", "해지", "요금제", "멤버십", "위약금",
    "LTE", "무제한", "이동통신", "통신사", "재약정", "고객", "계약", "인증", "가입",
    "문자", "스팸", "단말기", "설정", "납부", "요금", "할부금", "변경", "정지", "일시정지",
    "유심", "서류", "번호", "명의", "개통", "로밍"
]

VERB_SUFFIXES = re.compile(r"(다|요|데)$")

consumer = KafkaConsumer(
    "voc-consulting-raw",
    bootstrap_servers="43.200.162.199:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
    session_timeout_ms=60000,
    heartbeat_interval_ms=15000,
    max_poll_interval_ms=1200000
)

def get_db_connection() -> PgConnection:
    return psycopg2.connect(
        host=os.getenv("host"),
        port=os.getenv("port"),
        user=os.getenv("user"),
        password=os.getenv("password"),
        database=os.getenv("database")
    )

def validate_keywords(keywords: list) -> list:
    return [kw if kw != "금제" else "요금제" for kw in keywords]

def predict_sentiment(text: str) -> bool:
    inputs = sentiment_tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
    with torch.no_grad():
        outputs = sentiment_model(**inputs)
        return torch.argmax(outputs.logits, dim=1).item() == 1

def preprocess_korean(text: str) -> str:
    tokens = [word for word, tag in okt.pos(text, stem=True) if tag in ["Noun", "Adjective"]]
    filtered = [t for t in tokens if t not in KOREAN_STOPWORDS]
    final = [t for t in filtered if not VERB_SUFFIXES.search(t)]
    return " ".join(final)

def similarity_filter(keywords: list, category_id: int, category_map: dict, domain_keywords: list = None, threshold: float = 0.1, fallback_top_n: int = 3) -> list:
    category_name = category_map.get(category_id)
    if not category_name:
        return []
    if not keywords:
        return okt.morphs(category_name)[:fallback_top_n]
    cat_embedding = sentence_model.encode([category_name])
    keyword_embeddings = sentence_model.encode(keywords)
    sims = cosine_similarity(cat_embedding, keyword_embeddings)[0]
    keyword_with_sim = list(zip(keywords, sims))
    sorted_keywords = sorted(keyword_with_sim, key=lambda x: x[1], reverse=True)[:fallback_top_n]
    filtered = [kw for kw, _ in sorted_keywords]
    if not filtered:
        domain_embedding = sentence_model.encode(domain_keywords)
        domain_sims = cosine_similarity(cat_embedding, domain_embedding)[0]
        domain_with_sim = list(zip(domain_keywords, domain_sims))
        sorted_domain = sorted(domain_with_sim, key=lambda x: x[1], reverse=True)
        filtered = [kw for kw, _ in sorted_domain[:fallback_top_n]]
    return filtered

def extract_customer_content(text: str) -> str:
    return " ".join(re.findall(r"고객:\s*(.*)", text))

def extract_keywords(text: str, category_id: int, top_n: int = 5, threshold: float = 0.3) -> list:
    content = extract_customer_content(text)
    filtered = preprocess_korean(content)
    raw_keywords = [kw[0] for kw in kw_model_extended.extract_keywords(filtered, top_n=top_n)]
    return similarity_filter(raw_keywords, category_id, category_map, DOMAIN_KEYWORDS, threshold, top_n)

def save_analysis_result_to_rds(result: dict) -> None:
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO analysis_result (analysis_result_id, consulting_id, client_id, keywords, is_negative, created_datetime)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            result['analysis_result_id'], result['consulting_id'], result['client_id'],
            result['keywords'], result['is_negative'], result['created_datetime']
        ))
        conn.commit()
        cur.close()
        conn.close()
        print("✅ RDS 저장 완료")
    except Exception as e:
        print(f"❌ RDS 저장 실패: {e}")

def save_analysis_result_to_s3_parquet(result: dict, bucket_name: str) -> None:
    import traceback
    try:
        s3 = boto3.client("s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        dt = datetime.fromisoformat(result["created_datetime"]) if isinstance(result["created_datetime"], str) else result["created_datetime"]
        prefix = f"meta/analysis_result/year={dt.year}/month={dt.month:02d}/"
        key = f"{prefix}{result['analysis_result_id']}.parquet"
        buffer = io.BytesIO()
        pd.DataFrame([result]).to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
        print(f"✅ S3 저장 완료: s3://{bucket_name}/{key}")
    except Exception as e:
        print(f"❌ S3 저장 실패: {e}")
        traceback.print_exc()

def save_consulting_to_s3(data: dict, bucket_name: str, status: str = "SUCCESSED") -> None:
    try:
        dt = datetime.fromisoformat(data["consulting_datetime"])
        key = f"meta/consultings/year={dt.year}/month={dt.month:02d}/{data['consulting_id']}.parquet"
        buffer = io.BytesIO()
        pd.DataFrame([{**data, "analysis_status": status}]).to_parquet(buffer, index=False)
        buffer.seek(0)
        boto3.client("s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        ).put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
        print(f"✅ 상담 데이터 저장 완료: {status}")
    except Exception as e:
        print(f"❌ 상담 S3 저장 실패: {e}")

def update_consulting_status_to_successed(consulting_id: str) -> None:
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE consulting
            SET analysis_status = 'SUCCESSED'
            WHERE consulting_id = %s AND EXISTS (
                SELECT 1 FROM analysis_result WHERE consulting_id = %s
            )
        """, (consulting_id, consulting_id))
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ 상담 상태 업데이트 완료: {consulting_id}")
    except Exception as e:
        print(f"❌ 상담 상태 업데이트 실패: {e}")

def consume_from_kafka() -> None:
    print("📡 Kafka 메시지 수신 대기 중...")
    for message in consumer:
        data = message.value
        print(f"📩 수신 데이터: {data}")
        required = {"consulting_id", "content", "category_id", "client_id", "consulting_datetime"}
        if not required.issubset(data):
            print(f"⚠ 필수 필드 누락: {data}")
            continue
        try:
            keywords = validate_keywords(extract_keywords(data["content"], data["category_id"]))
            is_negative = predict_sentiment(extract_customer_content(data["content"]))
            result = {
                "analysis_result_id": str(uuid.uuid4()),
                "consulting_id": data["consulting_id"],
                "client_id": data["client_id"],
                "keywords": ", ".join(keywords),
                "is_negative": is_negative,
                "created_datetime": datetime.now()
            }
            print(f"📊 분석 결과: {result}")
            save_analysis_result_to_rds(result)
            save_analysis_result_to_s3_parquet(result, os.getenv("AWS_S3_BUCKET_NAME"))
            save_consulting_to_s3(data, os.getenv("AWS_S3_BUCKET_NAME"), status="pending")
            update_consulting_status_to_successed(data["consulting_id"])
        except Exception as e:
            print(f"❌ 분석 실패: {e}")
            save_consulting_to_s3(data, os.getenv("AWS_S3_BUCKET_NAME"), status="pending")

def main() -> None:
    consume_from_kafka()

if __name__ == "__main__":
    main()

