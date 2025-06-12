import os
import json

TXT_FILE_PATH = "data/generated_data.txt"
OUTPUT_DIR = "data/txt_to_json/"


def parse_and_save_lines(txt_path: str, output_dir: str) -> None:
    """
    각 라인을 JSON으로 파싱하여 파일로 저장하는 함수.

    Args:
        txt_path (str): 원본 .txt 파일 경로
        output_dir (str): 저장할 디렉토리 경로
    """
    os.makedirs(output_dir, exist_ok=True)

    with open(txt_path, "r", encoding="utf-8") as f:
        raw_lines = f.readlines()

    for idx, line in enumerate(raw_lines, start=1):
        line = line.strip()
        if not line:
            continue

        try:
            json_data = json.loads(line)
            consulting_category = json_data["consulting_category"]
            consulting_content = json_data["consulting_content"]
        except (json.JSONDecodeError, KeyError) as e:
            print(f"❌ JSON 파싱 오류 또는 필드 누락 (라인 {idx}): {e}")
            continue

        output_path = os.path.join(output_dir, f"{idx}.json")
        with open(output_path, "w", encoding="utf-8") as out_file:
            json.dump(
                {
                    "consulting_category": consulting_category,
                    "consulting_content": consulting_content
                },
                out_file,
                ensure_ascii=False,
                indent=4
            )

        print(f"✅ 파일 저장 완료: {output_path}")


if __name__ == "__main__":
    parse_and_save_lines(TXT_FILE_PATH, OUTPUT_DIR)

