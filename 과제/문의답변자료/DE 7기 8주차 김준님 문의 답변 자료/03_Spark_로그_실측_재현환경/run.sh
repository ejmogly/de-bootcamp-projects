#!/usr/bin/env bash
# 김준님 과제 05 코드(process_stocks.py)를 제출하신 Dockerfile 환경에서 세 가지 방식으로 실행하고,
# 제출하신 spark_submit.txt 와 비교합니다.
# 사용법: 이 폴더에서  bash run.sh      (요구사항: Docker)
set -euo pipefail

cd "$(dirname "$0")"
DIR="$(pwd)"
IMAGE="junkim-q5-env"
OUT="$DIR/my_results"
mkdir -p "$OUT"
export MSYS_NO_PATHCONV=1   # Windows Git Bash 경로 변환 방지 (macOS/Linux 영향 없음)

echo "[0/3] 제출하신 Dockerfile(env/)로 이미지 빌드"
docker build -q -t "$IMAGE" ./env > /dev/null

# boto3 대역(stub/)을 먼저 import 하도록 PYTHONPATH 지정 → AWS 자격증명 없이 끝까지 실행
STUB="export PYTHONPATH=/work/stub"

run() {  # $1: 컨테이너 안에서 실행할 명령, $2: 결과 파일명
  docker run --rm --entrypoint bash -v "$DIR":/work -w /work "$IMAGE" -c "$1" > "$OUT/$2" 2>&1
}

echo "[1/3] spark-submit 실행 (기본 설정)"
run "$STUB; spark-submit /work/code/process_stocks.py 2>&1" "1_spark-submit_기본설정.txt"

echo "[2/3] python 직접 실행 (기본 설정)"
run "$STUB; python /work/code/process_stocks.py 2>&1" "2_python직접_기본설정.txt"

echo "[3/3] python 직접 실행 (로그 설정 파일 + 진행률 표시 설정 변경)"
run "$STUB; export SPARK_CONF_DIR=/work/conf_python_info; \
     export PYSPARK_SUBMIT_ARGS='--conf spark.ui.showConsoleProgress=false pyspark-shell'; \
     python /work/code/process_stocks.py 2>&1" "3_python직접_설정변경.txt"

SUBMITTED="$DIR/results/0_제출하신_spark_submit.txt"

echo
echo "=== 로그 특성 요약 ==="
for f in "$SUBMITTED" "$OUT"/*.txt; do
  printf '%-36s | 줄 수=%s | WARN 기본값 메시지=%s | INFO 줄=%s | Submitted application=%s | [Stage 진행률=%s\n' \
    "$(basename "$f")" \
    "$(grep -c '' "$f" || true)" \
    "$(grep -c 'Setting default log level' "$f" || true)" \
    "$(grep -c ' INFO ' "$f" || true)" \
    "$(grep -c 'Submitted application' "$f" || true)" \
    "$(grep -c '\[Stage' "$f" || true)"
done

# 실행할 때마다 바뀌는 값(시각, 커널/CPU, 호스트·포트, 임시 폴더·파일 ID, 메모리 수치)을 지우고,
# os.walk 순서에 따라 바뀌는 'Uploaded:' 줄은 정렬해서 비교합니다.
norm() {
  sed -E 's/^[0-9]{2}\/[0-9]{2}\/[0-9]{2} [0-9:]{8} //;
          s/OS info Linux, [^,]+, [a-z0-9_]+/OS info Linux, KERNEL, ARCH/;
          s/[0-9a-f]{12}/HOST/g; s/port [0-9]+/port N/; s/:[0-9]{4,5}/:N/g;
          s/(driver, HOST, )[0-9]+/\1N/g; s/[0-9]+\.[0-9]+ MiB/X MiB/g;
          s/blockmgr-[0-9a-f-]+/blockmgr-ID/; s/@[0-9a-f]+ /@ID /;
          s/part-00000-[0-9a-f-]+/part-00000-ID/' "$1" > "$1.norm.tmp"
  grep -v '^Uploaded:' "$1.norm.tmp" || true
  grep '^Uploaded:' "$1.norm.tmp" | sort || true
  rm -f "$1.norm.tmp"
}

echo
echo "=== 제출하신 spark_submit.txt 와 비교 (실행마다 바뀌는 값 제외) ==="
for f in "$OUT"/*.txt; do
  n=$(diff <(norm "$SUBMITTED") <(norm "$f") | grep -c '^[<>]' || true)
  if [ "$n" -eq 0 ]; then r="동일"; else r="다름 (차이 ${n}줄)"; fi
  printf '%-36s → %s\n' "$(basename "$f")" "$r"
done
echo "결과 파일 위치: $OUT"
