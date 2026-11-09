#!/usr/bin/env bash
# 과제 08 (3) — 종목별 집계 결과를 파드에 하나씩 적재하고 파드별 DNS 로 확인한다.
#
#   NS=stock-<본인이름> NAME=<본인이름> COHORT=<기수> bash load_results.sh
#
# 수치는 stock_stats.csv(과제 05 집계 결과)에서 읽는다. 스크립트에 값을 직접 쓰지 않는다.
set -euo pipefail

NS="${NS:-stock-honggildong}"
STS="${STS:-stock}"
SVC="${SVC:-stock-svc}"
NAME="${NAME:-홍길동}"
COHORT="${COHORT:-3기}"
RESULTS="${RESULTS:-$(dirname "$0")/stock_stats.csv}"

REPLICAS=$(kubectl get statefulset "$STS" -n "$NS" -o jsonpath='{.spec.replicas}')
ROWS=$(tail -n +2 "$RESULTS" | grep -c .)
if [ "$ROWS" -ne "$REPLICAS" ]; then
  echo "종목 수($ROWS)와 파드 수($REPLICAS)가 다릅니다: $RESULTS" >&2
  exit 1
fi

i=0
while IFS=, read -r TICKER AVG HIGH LOW VOL; do
  POD="${STS}-${i}"
  echo "[load] ${POD} <- ${TICKER}"
  # 파드마다 볼륨이 다르므로 같은 경로에 써도 내용이 파드마다 다르다.
  kubectl exec -i -n "$NS" "$POD" -- sh -c 'cat > /usr/share/nginx/html/index.html' <<HTML
<!doctype html>
<html lang="ko">
<meta charset="utf-8">
<title>${TICKER} — 주식 집계 결과</title>
<body>
  <h1>${TICKER}</h1>
  <ul>
    <li>평균종가: ${AVG}</li>
    <li>최고가: ${HIGH}</li>
    <li>최저가: ${LOW}</li>
    <li>총거래량: ${VOL}</li>
  </ul>
  <p>제출자: ${NAME} (${COHORT}) — pod ${POD}</p>
</body>
</html>
HTML
  i=$((i + 1))
done < <(tail -n +2 "$RESULTS")

echo
echo "[check] ${STS}-0 안에서 <파드>.<서비스> DNS 로 ${REPLICAS}개 조회 — 응답이 서로 달라야 한다"
kubectl exec -n "$NS" "${STS}-0" -- sh -c "
  for i in \$(seq 0 $((REPLICAS - 1))); do
    echo \"--- ${STS}-\$i.${SVC} ---\"
    wget -qO- http://${STS}-\$i.${SVC}/ | grep -E '<h1>|<li>|제출자'
  done"
