import os
import json

# 한글 → 숫자 코드 매핑
CATEGORY_MAP = {
    '요금 안내': 0, '요금 납부': 1, '요금제 변경': 2, '선택약정 할인': 3, '납부 방법 변경': 4,
    '부가서비스 안내': 5, '소액 결제': 6, '휴대폰 정지/분실/파손': 7, '기기변경': 8, '내역 조회': 9,
    '명의/번호/유심 해지': 10, '결합할인': 11, '가입 안내': 12, '통화품질': 13, '장비 안내': 14,
    '인터넷 속도/품질': 15, '요금 할인': 16, '인터넷 장애/고장': 17, '정보변경': 18, '청구서': 19,
    '복지할인 등록': 20, '납부확인서': 21, '군입대 일시정지': 22, '기기할부 약정': 23, '멤버십 혜택': 24,
    '보험 안내': 25, '해외로밍': 26, '명의/번호/유심 일시정지': 27, '개통점 안내': 28, '명의/번호/유심 변경': 29,
    '상품 안내': 30, '제휴카드로 할인': 31, '배송문의': 32, '장기고객 혜택': 33, '기타 할인 혜택': 34,
    '번호 변경': 35, '데이터 옵션 안내': 36, '세금계산서': 37, '인터넷 가입 및 변경': 38, '위약금 안내': 39,
    '해지 안내': 40, '보험 해지': 41, '보험 청구': 42, '자동 이체': 43, '한도 안내': 44,
    '파손보험 안내': 45, '약정 안내': 46, '카드 배송': 47, '보험 혜택': 48, '납부 확인서': 49,
    '청구지 변경': 50, '번호 이동': 51, '인터넷전화가입 및 변경': 52, '환불 안내': 53, '이전 설치': 54,
    '유심 문제': 55, '반품 안내': 56, '보험 신청': 57, '번호이동': 58
}

# 대상 디렉토리
TARGET_DIR = "/data/txt_to_json"


def update_category_codes_in_json(directory_path: str) -> None:
    """
    JSON 파일 내 consulting_category 값을 한글에서 숫자 코드로 변경한다.

    Args:
        directory_path (str): JSON 파일들이 존재하는 디렉토리 경로
    """
    for filename in os.listdir(directory_path):
        if not filename.endswith(".json"):
            continue

        file_path = os.path.join(directory_path, filename)

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict) and "consulting_category" in data:
            category_name = data["consulting_category"]

            if isinstance(category_name, int):
                continue

            if category_name in CATEGORY_MAP:
                data["consulting_category"] = CATEGORY_MAP[category_name]
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"✔ updated: {filename}")
            else:
                print(f"❓ unknown category: {category_name} in {filename}")

    print("✅ 모든 파일 처리가 완료되었습니다.")


if __name__ == "__main__":
    update_category_codes_in_json(TARGET_DIR)

