import os
import re
import json
import random
import requests
from tqdm import tqdm

# Constants
OLLAMA_MODEL = "exaone3.5:7.8b"
OLLAMA_URL = "http://localhost:11434/api/generate"
OUTPUT_PATH = "data/generated_data.txt"
OUTPUT_JSON_DIR = "data/txt_to_json/"
TOTAL = 100000
BATCH_SIZE = 100

CATEGORY_RATIO = {
    "요금 안내": 9.28, "요금 납부": 5.11, "요금제 변경": 3.69,
    "선택약정 할인": 10.56, "납부 방법 변경": 8.81, "부가서비스 안내": 5.58,
    "소액 결제": 4.97, "휴대폰 정지/분실/파손": 4.44, "기기변경": 2.61,
    "내역 조회": 2.25, "명의/번호/유심 해지": 2.25, "결합할인": 1.86,
    "가입 안내": 1.53, "통화품질": 0.64, "장비 안내": 0.58,
    "인터넷 속도/품질": 0.53, "요금 할인": 0.5, "인터넷 장애/고장": 0.42,
    "정보변경": 0.42, "청구서": 0.39, "복지할인 등록": 0.36,
    "납부확인서": 0.31, "군입대 일시정지": 0.25, "기기할부 약정": 0.25,
    "멤버십 혜택": 0.22, "보험 안내": 0.19, "해외로밍": 0.17,
    "명의/번호/유심 일시정지": 0.14, "개통점 안내": 0.14,
    "명의/번호/유심 변경": 0.11, "상품 안내": 0.11,
    "제휴카드로 할인": 0.08, "배송문의": 0.08, "장기고객 혜택": 0.08,
    "기타 할인 혜택": 0.08, "번호 변경": 0.08, "데이터 옵션 안내": 0.08,
    "세금계산서": 0.08, "인터넷 가입 및 변경": 0.06, "위약금 안내": 0.06,
    "해지 안내": 0.06, "보험 해지": 0.06, "보험 청구": 0.06,
    "자동 이체": 0.03, "명 의/번호/유심 일시정지": 0.03,
    "한도 안내": 0.03, "파손보험 안내": 0.03, "약정 안내": 0.03,
    "카드 배송": 0.03, "보험 혜택": 0.03, "납부 확인서": 0.03,
    "청구지 변경": 0.03, "번호 이동": 0.03, "인터넷전화가입 및 변경": 0.03,
    "환불 안내": 0.03, "이전 설치": 0.03, "유심 문제": 0.03,
    "반품 안내": 0.03, "보험 신청": 0.03, "번호이동": 0.03
}

def assign_categories(total: int) -> list[str]:
    categories = list(CATEGORY_RATIO.keys())
    weights = list(CATEGORY_RATIO.values())
    return random.choices(categories, weights=weights, k=total)

def make_prompt(category: str) -> str:
    return (
        f"""다음은 LGU+ 고객센터의 상담 예시입니다.

상담 주제: {category}

조건:
- 고객과 상담사의 대화 형식으로 작성해 주세요.
- 총 8~15턴의 현실적인 대화여야 합니다.
- 말투는 자연스럽고 실제 상담처럼 구성해 주세요.
- 고객이 짜증을 내거나 불만을 표현하는 반응도 포함해 주세요.

아래 형식으로 출력해 주세요:

{{
  \"consulting_category\": \"{category}\",
  \"consulting_content\": \"상담사: ...\\n고객: ...\\n상담사: ...\"
}}"""
    )

def generate_data_batch(batch: list[str]) -> list[str]:
    lines = []
    for category in batch:
        prompt = make_prompt(category)
        try:
            response = requests.post(
                OLLAMA_URL,
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}
            )
            if response.status_code == 200:
                output = response.json().get("response", "").strip()
                match = re.search(r'"consulting_content"\s*:\s*"(.+?)"', output, re.DOTALL)
                if match:
                    content = match.group(1).replace('\n', '\\n').replace('"', '\\"')
                else:
                    content = output.replace('\n', '\\n').replace('"', '\\"')

                line = (
                    f'{{"consulting_category": "{category}", '
                    f'"consulting_content": "{content}"}}'
                )
                lines.append(line)
            else:
                print(f"❌ 응답 오류: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"❌ 생성 실패: {e}")
    return lines

def convert_to_json_files(txt_path: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    with open(txt_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for idx, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            category = obj["consulting_category"]
            content = obj["consulting_content"]
        except (json.JSONDecodeError, KeyError) as e:
            print(f"❌ JSON 파싱 오류 또는 필드 누락 (라인 {idx}): {e}")
            continue

        output_path = os.path.join(output_dir, f"{idx}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({
                "consulting_category": category,
                "consulting_content": content
            }, f, ensure_ascii=False, indent=4)

        print(f"✅ 파일 저장 완료: {output_path}")

def main() -> None:
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for offset in tqdm(range(0, TOTAL, BATCH_SIZE)):
            batch = assign_categories(min(BATCH_SIZE, TOTAL - offset))
            lines = generate_data_batch(batch)
            for line in lines:
                f.write(line + "\n")

    print(f"🎉 전체 완료! → {OUTPUT_PATH}")
    convert_to_json_files(OUTPUT_PATH, OUTPUT_JSON_DIR)

if __name__ == "__main__":
    main()

