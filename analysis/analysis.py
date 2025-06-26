import asyncio
import logging
import re
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import torch
from dotenv import load_dotenv
from konlpy.tag import Okt
from keybert import KeyBERT
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from config import NLP_CONFIG
from db import save_analysis_result
from s3 import save_analysis_result_to_s3, save_consulting_to_s3

# .env 파일 로드
load_dotenv()

# 로깅 설정
logger = logging.getLogger("analysis")

# 디바이스 설정
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f"모델 {device}로 작동중...")

# 모델 로딩
okt = Okt()
kw_model = KeyBERT()
sentence_model = SentenceTransformer(NLP_CONFIG["model_name"], device=device)
kw_model_extended = KeyBERT(NLP_CONFIG["model_name"])
sentiment_model = AutoModelForSequenceClassification.from_pretrained(NLP_CONFIG["model_path"]).to(device)
sentiment_tokenizer = AutoTokenizer.from_pretrained(NLP_CONFIG["tokenizer"])

if torch.cuda.is_available():
    sentiment_model.to("cuda")
    logger.info("모델 GPU로 작동중...")

sentiment_model.eval()

# 쓰레드 풀
executor = ThreadPoolExecutor(max_workers=4)

# 불용어 리스트
KOREAN_STOPWORDS = [
    "그냥", "그리고", "근데", "음", "아", "어", "뭐", "그죠", "그러니까",
    "그거", "저기", "부터", "그게", "전화", "전화기", "수고", "대해"
]

# 카테고리 맵
CATEGORY_MAP = {
    0: "요금 안내", 1: "요금 납부", 2: "요금제 변경", 3: "선택약정 할인", 4: "납부 방법 변경",
    5: "부가서비스 안내", 6: "소액 결제", 7: "휴대폰 정지/분실/파손", 8: "기기변경", 9: "내역 조회",
    10: "명의/번호/유심 해지", 11: "결합할인", 12: "가입 안내", 13: "통화품질", 14: "장비 안내",
    15: "인터넷 속도/품질", 16: "요금 할인", 17: "인터넷 장애/고장", 18: "정보변경", 19: "청구서",
    20: "복지할인 등록", 21: "납부확인서", 22: "군입대 일시정지", 23: "기기할부 약정", 24: "멤버십 혜택",
    25: "보험 안내", 26: "해외로밍", 27: "명의/번호/유심 일시정지", 28: "개통점 안내",
    29: "명의/번호/유심 변경", 30: "상품 안내", 31: "제휴카드로 할인", 32: "배송문의",
    33: "장기고객 혜택", 34: "기타 할인 혜택", 35: "번호 변경", 36: "데이터 옵션 안내",
    37: "세금계산서", 38: "인터넷 가입 및 변경", 39: "위약금 안내", 40: "해지 안내",
    41: "보험 해지", 42: "보험 청구", 43: "자동 이체", 44: "명의/번호/유심 일시정지",
    45: "한도 안내", 46: "파손보험 안내", 47: "약정 안내", 48: "카드 배송", 49: "보험 혜택",
    50: "납부 확인서", 51: "청구지 변경", 52: "번호 이동", 53: "인터넷전화가입 및 변경",
    54: "환불 안내", 55: "이전 설치", 56: "유심 문제", 57: "반품 안내", 58: "보험 신청",
    59: "번호이동"
}

# 도메인 키워드 리스트
DOMAIN_KEYWORDS = [
    "약정", "할인", "데이터", "기기", "변경", "해지", "요금제", "멤버십", "위약금",
    "LTE", "무제한", "이동통신", "통신사", "재약정", "고객", "계약", "인증", "가입",
    "문자", "스팸", "단말기", "설정", "납부", "요금", "할부금", "변경", "정지", "일시정지",
    "유심", "서류", "번호", "명의", "개통", "로밍"
]

# 정규 표현식
VERB_SUFFIXES = re.compile(r"(다|요|데)$")


def _predict_sentiment_sync(text: str) -> bool:
    inputs = sentiment_tokenizer(
        text,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=512
    )
    inputs = {k: v.to(device) for k, v in inputs.items()}
    with torch.no_grad():
        outputs = sentiment_model(**inputs)
        prediction = torch.argmax(outputs.logits, dim=1).item()
        return prediction == 1


async def predict_sentiment(text: str) -> bool:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, _predict_sentiment_sync, text)


def _sentence_model_encode_sync(texts):
    return sentence_model.encode(texts)


async def sentence_model_encode(texts):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, _sentence_model_encode_sync, texts)


def _keybert_extract_sync(text: str, top_n: int):
    return [kw[0] for kw in kw_model_extended.extract_keywords(text, stop_words=None, top_n=top_n)]


async def keybert_extract(text: str, top_n: int):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, _keybert_extract_sync, text, top_n)


def validate_keywords(keywords: list) -> list:
    return ["요금제" if keyword == "금제" else keyword for keyword in keywords]


def preprocess_korean(text: str) -> str:
    tokens = [word for word, tag in okt.pos(text, stem=True) if tag in ["Noun", "Adjective"]]
    filtered_tokens = [token for token in tokens if token not in KOREAN_STOPWORDS]
    final_tokens = [token for token in filtered_tokens if not VERB_SUFFIXES.search(token)]
    return " ".join(final_tokens)


def extract_customer_content(text: str) -> str:
    customer_lines = re.findall(r"고객:\s*(.*)", text)
    return " ".join(customer_lines)


async def similarity_filter(
    keywords: list,
    category_id: int,
    category_map: dict,
    domain_keywords: list = None,
    threshold: float = 0.1,
    fallback_top_n: int = 3
) -> list:
    category_name = category_map.get(category_id)
    if not category_name:
        return []

    if not keywords:
        keywords = okt.morphs(category_name)
        return keywords[:fallback_top_n]

    cat_embedding = await sentence_model_encode([category_name])
    keyword_embeddings = await sentence_model_encode(keywords)
    sims = cosine_similarity(cat_embedding, keyword_embeddings)[0]
    keyword_with_sim = list(zip(keywords, sims))
    sorted_keywords = sorted(keyword_with_sim, key=lambda x: x[1], reverse=True)[:fallback_top_n]
    filtered = [kw for kw, _ in sorted_keywords]

    if not filtered:
        domain_embedding = await sentence_model_encode(domain_keywords)
        domain_sims = cosine_similarity(cat_embedding, domain_embedding)[0]
        domain_with_sim = list(zip(domain_keywords, domain_sims))
        sorted_domain_keywords = sorted(domain_with_sim, key=lambda x: x[1], reverse=True)[:fallback_top_n]
        filtered = [kw for kw, _ in sorted_domain_keywords]

    return filtered[:fallback_top_n]


async def extract_keywords(text: str, category_id: int, top_n: int = 5, threshold: float = 0.3) -> list:
    customer_content = extract_customer_content(text)
    filtered_text = preprocess_korean(customer_content)
    raw_keywords = await keybert_extract(filtered_text, top_n)
    final_keywords = await similarity_filter(
        raw_keywords,
        category_id,
        CATEGORY_MAP,
        domain_keywords=DOMAIN_KEYWORDS,
        threshold=threshold,
        fallback_top_n=top_n
    )
    return final_keywords


async def handle_analysis(data: dict):
    """
    데이터 분석 (키워드 추출 및 불만 여부 판정 등)

    Args:
        data (dict): 상담 데이터
    """
    consulting_id = data["consulting_id"]
    content = data["content"]
    category_id = data["category_id"]
    client_id = data["client_id"]
    try:
        logger.info(f"[{consulting_id}] 데이터 분석 시작")
        start_time = time.time()

        extracted_keywords = await extract_keywords(content, category_id, top_n=5, threshold=0.3)
        keywords = validate_keywords(extracted_keywords)
        keywords = [kw for kw in keywords if "/" not in kw]
        client_text = extract_customer_content(content)
        is_negative = await predict_sentiment(client_text)

        dt_timestamp = datetime.now()
        dt_str = dt_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        dt = dt_timestamp.fromisoformat(data["consulting_datetime"])

        analysis_result = {
            "analysis_result_id": str(uuid.uuid4()),
            "consulting_id": consulting_id,
            "client_id": client_id,
            "keywords": ", ".join(keywords),
            "keyword_list": keywords,
            "is_negative": is_negative,
            "created_datetime": dt_timestamp
        }
        logger.info(f"[{consulting_id}] 데이터 분석 완료.\n{analysis_result}")

        await save_analysis_result(analysis_result)
        logger.info(f"[{consulting_id}] RDS 저장 완료.")

        analysis_result["created_datetime"] = dt_str
        await save_analysis_result_to_s3(analysis_result, dt.year, f"{dt.month:02d}")
        logger.info(f"[{consulting_id}] 분석 결과 S3 저장 완료.")

        data["analysis_status"] = "SUCCESSED"
        await save_consulting_to_s3(data, dt.year, f"{dt.month:02d}")
        logger.info(f"[{consulting_id}] 상담 데이터 S3 저장 완료.")

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"[{consulting_id}] 분석 소요 시간: {elapsed_time:.2f}초")

    except Exception as e:
        logger.error(f"[{consulting_id}] 분석 실패.\n{e}")
        raise

