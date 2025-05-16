import os
import re
import time
import random
from datetime import datetime, timedelta
import json
import pandas as pd
import logging
from kafka import KafkaConsumer
from google.cloud import storage
from konlpy.tag import Okt
from transformers import pipeline, XLMRobertaTokenizer
from keybert import KeyBERT
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from pathlib import Path

# 로그 줄이기
logging.getLogger("transformers").setLevel(logging.ERROR)

# 모델 로딩
model_name = "joeddav/xlm-roberta-large-xnli"
tokenizer = XLMRobertaTokenizer.from_pretrained(model_name, use_fast=False)
classifier = pipeline("zero-shot-classification", model=model_name, tokenizer=tokenizer, device=0)
okt = Okt()
sentence_model = SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS", device="cuda")
kw_model_extended = KeyBERT(SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS", device="cuda"))

# 불용어 리스트
KOREAN_STOPWORDS = [
    "그냥", "그리고", "근데", "음", "아", "어", "뭐", "그죠", "그러니까", "그거", "저기", "부터", "그게", "전화", "전화기", "고객", "상담사"
]

# 도메인 키워드 리스트
DOMAIN_KEYWORDS = [
    "약정", "할인", "데이터", "기기", "변경", "해지", "요금제", "멤버십", "위약금",
    "LTE", "무제한", "이동통신", "통신사", "재약정", "고객", "계약",
    "인증", "가입", "문자", "스팸", "단말기", "설정", "납부", "요금", "할부금", "변경",
    "정지", "일시정지", "유심", "서류", "번호", "명의", "개통", "로밍"
]
VERB_SUFFIXES = re.compile(r"(다|요|데)$")

# Kafka Consumer 설정
consumer = KafkaConsumer(
    "voc-json",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="voc-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
    session_timeout_ms=60000,
    heartbeat_interval_ms=15000,
    max_poll_interval_ms=1200000
    )

# GCP 설정
BUCKET_NAME = "wh04-voc"
PROCESSED_DATA_DIR = "meta/analysis_result/"

def save_parquet_to_gcs(bucket_name, data, consulting_date):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    try:
        dt = pd.to_datetime(consulting_date)
        hour = random.randint(0, 23)
        minute = random.randint(0, 11) * 5  # 5분 단위

        # 최종 파티셔닝 경로
        partition_path = f"year={dt.year}/month={dt.month:02}/day={dt.day:02}/hour={hour:02}/minute={minute:02}/"
        file_name = f"{dt.strftime('%Y%m%d')}_consulting_{data['consulting_id']}.parquet"
        blob_path = f"{PROCESSED_DATA_DIR}{partition_path}{file_name}"
        blob = bucket.blob(blob_path)

        if blob.exists():
            print(f"🛑 이미 존재하는 파일입니다. 스킵: {blob_path}")
            return

        # Parquet 파일 저장
        df = pd.DataFrame([data])
        parquet_file = Path(f"~/temp/voc/{file_name}").expanduser()

        # 로컬 파일이 이미 있는지 확인
        if parquet_file.exists():
            print("🛑 이미 로컬에 존재하는 파일입니다. 스킵함.")
        else:
            parquet_file.parent.mkdir(parents=True, exist_ok=True)  # 디렉토리 생성
            df.to_parquet(parquet_file, index=False)
            blob.upload_from_filename(str(parquet_file))
            print(f"✅ GCS 업로드 완료: {blob_path}")

    except Exception as e:
        print(f"❌ 파일 저장 오류: {e}")

def extract_customer_content(text):
    # 고객 발화만 추출
    customer_lines = re.findall(r"고객:\s*(.*)", text)
    return " ".join(customer_lines)

def preprocess_korean(text):
    # 형태소 분석 및 불용어&어미 제거
    tokens = [word for word, tag in okt.pos(text, stem=True) if tag == "Noun"]
    filtered_tokens = [token for token in tokens if token not in KOREAN_STOPWORDS]
    final_tokens = [token for token in filtered_tokens if not VERB_SUFFIXES.search(token)]
    return " ".join(final_tokens)

def similarity_filter(keywords, category, threshold=0.3, fallback_top_n=10):
    # 카테고리 유사도 필터
    cat_embedding = sentence_model.encode([category])
    keyword_embeddings = sentence_model.encode(keywords)
    sims = cosine_similarity(cat_embedding, keyword_embeddings)[0]
    filtered = [kw for kw, sim in zip(keywords, sims) if sim >= threshold]
    return filtered if filtered else keywords[:fallback_top_n]

def extract_keywords(text, category, top_n=20, threshold=0.3):
    # 1. 고객 발화만 추출
    customer_content = extract_customer_content(text)
    # 2. 형태소 분리 및 불용어 제거 (문맥 유지)
    filtered_text = preprocess_korean(customer_content)
    # 3. 키워드 추출 (KeyBERT)
    raw_keywords = [kw[0] for kw in kw_model_extended.extract_keywords(filtered_text, top_n=top_n)]
    # 4. 카테고리 유사도 필터 적용
    final_keywords = similarity_filter(raw_keywords, category, threshold=threshold, fallback_top_n=top_n)
    return final_keywords

def process_message(data):
    try:
        consulting_id = data.get("consulting_id", "UNKNOWN")
        client_id = data.get("client_id", "UNKNOWN")
        consulting_content = data.get("consulting_content", "")
        consulting_category = data.get("consulting_category", "")
        consulting_date = data.get("consulting_date", "2020-01-01")
        duration = data.get("duration", None)

        text = extract_customer_content(consulting_content)
        # 키워드 추출
        keywords = extract_keywords(text, consulting_category, 20, 0.3)

        # 감정 분석
        result = classifier(text, candidate_labels=["불만", "감사"])
        negative_ratio = result["scores"][result["labels"].index("불만")]
        positive_ratio = result['scores'][result['labels'].index("감사")]

        # 결과 합치기
        analysis_result = {
            "consulting_id": consulting_id,
            "client_id": client_id,
            "keywords": ", ".join(keywords),
            "positive_ratio": positive_ratio,
            "negative_ratio": negative_ratio,
            "duration": duration if duration else None
        }

        # GCP 저장
        save_parquet_to_gcs(BUCKET_NAME, analysis_result, consulting_date)
        print(analysis_result)
    except Exception as e:
        print(f"❌ 메시지 처리 오류: {e}")

def main():
    for message in consumer:
        try:
            data = message.value
            if data is None:
                print("  빈 메시지, 스킵합니다.")
                continue
            process_message(data)
        except Exception as e:
            print(f"❌ 메시지 처리 오류: {e}")

if __name__ == "__main__":
    main()
