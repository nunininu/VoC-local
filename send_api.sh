#!/bin/bash

TARGET_DIR="/consulting/data/consulting_add_client"URL="http://43.200.162.199/submit"

for file in "$TARGET_DIR"/*.json; do
  echo "📤 전송 중: $file"
  curl -s -o /dev/null -w "%{http_code}" -X POST "$URL" \
    -H "accept: application/json" \
    -H "Content-Type: multipart/form-data" \
    -F "json_file=@$file;type=application/json"

  echo " ✅ 완료"
  sleep 0.3  # 서버 과부하 방지용 잠깐 대기
done

echo "🎉 모든 파일 전송 완료"
