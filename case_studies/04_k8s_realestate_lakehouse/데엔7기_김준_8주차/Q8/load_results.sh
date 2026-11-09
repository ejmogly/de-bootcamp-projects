#!/usr/bin/env bash
set -e

NAMESPACE="stock-junekim"

echo "Injecting stock analytics results into pods in namespace: $NAMESPACE"

# Pod 0: AAPL
kubectl exec -i stock-0 -n $NAMESPACE -- sh -c 'cat << "INNER_EOF" > /usr/share/nginx/html/index.html
[AAPL] 종목분석 결과
- 종목코드: AAPL
- 평균종가: 204.01
- 최고가: 288.62
- 최저가: 124.17
- 총거래량: 42740740000
- 수강생: 데엔7기 김준
INNER_EOF'

# Pod 1: JNJ
kubectl exec -i stock-1 -n $NAMESPACE -- sh -c 'cat << "INNER_EOF" > /usr/share/nginx/html/index.html
[JNJ] 종목분석 결과
- 종목코드: JNJ
- 평균종가: 161.98
- 최고가: 215.19
- 최저가: 140.68
- 총거래량: 6814007700
- 수강생: 데엔7기 김준
INNER_EOF'

# Pod 2: JPM
kubectl exec -i stock-2 -n $NAMESPACE -- sh -c 'cat << "INNER_EOF" > /usr/share/nginx/html/index.html
[JPM] 종목분석 결과
- 종목코드: JPM
- 평균종가: 209.70
- 최고가: 330.86
- 최저가: 123.11
- 총거래량: 7299229900
- 수강생: 데엔7기 김준
INNER_EOF'

# Pod 3: TSLA
kubectl exec -i stock-3 -n $NAMESPACE -- sh -c 'cat << "INNER_EOF" > /usr/share/nginx/html/index.html
[TSLA] 종목분석 결과
- 종목코드: TSLA
- 평균종가: 268.25
- 최고가: 498.83
- 최저가: 101.81
- 총거래량: 82661596500
- 수강생: 데엔7기 김준
INNER_EOF'

# Pod 4: XOM
kubectl exec -i stock-4 -n $NAMESPACE -- sh -c 'cat << "INNER_EOF" > /usr/share/nginx/html/index.html
[XOM] 종목분석 결과
- 종목코드: XOM
- 평균종가: 111.11
- 최고가: 126.34
- 최저가: 95.77
- 총거래량: 12490305000
- 수강생: 데엔7기 김준
INNER_EOF'

echo "All 5 pods successfully configured with unique stock results!"
