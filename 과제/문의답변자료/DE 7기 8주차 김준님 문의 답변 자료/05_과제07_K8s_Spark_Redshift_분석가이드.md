# 과제 07 · 08 실무 분석 가이드

김준님이 질문하신 과제 07(Spark on K8s → Redshift)과 과제 08(StatefulSet) 관련 실무 주제를 정리했습니다.

---

## 1. Spark-Redshift Connector(직접 쓰기) vs S3 경유 COPY

### 결론
오픈소스 `spark-redshift` 커넥터도 **쓰기 시 내부적으로 S3 임시 경로(tempdir)에 파일을 쓰고 Redshift COPY를 실행**합니다(읽기는 UNLOAD → S3).
즉 네트워크 경로는 과제 방식(S3 저장 → COPY)과 거의 같고, 차이는 이 과정을 커넥터가 자동화해 주느냐입니다.
실제로 비교해야 할 대상은 **JDBC로 직접 INSERT하는 방식**입니다.

| 방식 | 데이터 경로 | 대용량 성능 | Redshift 부하 |
|---|---|---|---|
| S3 → COPY (과제 방식) | Spark → S3 → 컴퓨트 노드가 S3에서 병렬로 읽음 | 좋음. 파일 수가 많을수록 slice별 병렬 적재 | 리더 노드 부담 적음 |
| spark-redshift 커넥터 | 위와 동일 (커넥터가 tempdir·COPY 자동 처리) | 위와 동일 | 위와 동일 + 임시 파일 정리 필요 |
| JDBC 직접 INSERT | Spark executor → 리더 노드로 행 단위 전송 | 매우 느림 | 리더 노드에 집중 |

### 네트워크 I/O와 동시성
- COPY는 컴퓨트 노드가 S3에서 직접 병렬로 읽으므로, Spark → Redshift 직접 전송보다 네트워크 병목이 적습니다.
  압축 후 수십~수백 MB 크기의 파일 여러 개로 나누면 slice별로 고르게 적재됩니다.
- 동시성 측면에서는 **작은 COPY를 자주, 많이** 보내는 것이 커밋 대기열 경합을 만듭니다. 가능한 한 묶어서 적은 횟수로 적재하세요.
- 재실행 안전성을 위해 **staging 테이블에 COPY → 한 트랜잭션 안에서 본 테이블 교체** 패턴을 씁니다(예시 답안 `Q7/redshift_loader.py`).

---

## 2. K8s Pod의 Redshift 적재와 커넥션 관리

- 과제에서 사용한 **Redshift Data API는 HTTP 호출**이라 DB 커넥션을 유지하지 않습니다. 커넥션 풀을 직접 관리할 필요가 없고, 짧게 실행되는 Job에 잘 맞습니다.
- JDBC/psycopg2로 여러 Pod가 동시에 붙는 구조라면:
  - **Pod 수 × 풀 크기**가 Redshift 최대 연결 수와 워크로드 관리(WLM) 동시 실행 수를 넘지 않게 풀을 작게 둡니다.
  - 작업이 끝나면 반드시 커넥션을 반환·종료합니다(Job 종료 시 정리).
  - 대량 적재는 커넥션 수를 늘리기보다 COPY 한 번에 모으는 편이 효과적입니다.

---

## 3. EC2 IAM Role 임시 자격 증명과 장기 실행 Pod

### 자동 갱신
- 인스턴스 프로파일 자격 증명은 boto3, AWS SDK for Java(S3A가 사용) 모두 **만료 전에 IMDS에서 자동으로 갱신**합니다.
- 갱신이 깨지는 대표적인 경우는 **코드에서 키를 한 번 꺼내 환경 변수나 설정에 고정**하는 것입니다. 항상 SDK 기본 자격 증명 체인을 쓰세요.
  예시 답안은 코드·이미지·YAML 어디에도 키를 두지 않습니다.

### 컨테이너에서 자주 겪는 문제: IMDSv2 hop limit
- IMDSv2는 토큰 요청 응답의 hop limit(기본 1)을 넘으면 응답을 받지 못합니다. Pod 네트워크를 한 번 더 거치는 컨테이너에서는 자격 증명을 못 받는 경우가 있습니다.
- 해결: EC2 인스턴스 메타데이터 옵션에서 `HttpPutResponseHopLimit`을 **2**로 올립니다.
  ```bash
  aws ec2 modify-instance-metadata-options --instance-id <id> --http-put-response-hop-limit 2 --http-endpoint enabled
  ```

### 권한 범위
- 인스턴스 Role은 그 노드의 **모든 Pod가 같은 권한**을 갖게 됩니다.
- EKS에서는 **IRSA(IAM Roles for Service Accounts)**나 **EKS Pod Identity**로 ServiceAccount 단위의 최소 권한을 주는 것이 운영 표준입니다.
- 과제 07의 S3A 출력 커밋은 임시 경로 정리를 위해 `s3:DeleteObject` 권한이 필요합니다. 권한이 없으면 커밋이 실패하니 Role 정책에 포함하세요.

---

## 4. StatefulSet 헤드리스 서비스 DNS 트러블슈팅 (과제 08)

### 파드 DNS가 생기는 조건
- `stock-0.stock-svc` 같은 레코드는 **파드가 Ready일 때** 생깁니다. readinessProbe가 실패하면 DNS 조회도 실패합니다.
- 예시 답안은 TCP readinessProbe를 씁니다. 적재 전에는 볼륨이 비어 있어 nginx가 `/`에 403을 반환하므로, HTTP 검사로 하면 파드가 Ready가 되지 않고 DNS도 생기지 않습니다.
- Ready 전에도 레코드가 필요하면 Service에 `publishNotReadyAddresses: true`를 씁니다.

### 캐시
- CoreDNS는 응답을 일정 시간 캐시하고(k3s 기본 설정 30초), 실패 응답도 캐시합니다.
  파드 재생성 직후 잠깐 조회가 안 되거나 예전 IP가 보이는 현상이 이 때문입니다.
- 클라이언트 쪽 캐시도 확인합니다. JVM은 `networkaddress.cache.ttl`, nginx는 `upstream`을 시작 시 한 번만 해석하므로 `resolver` 지시어와 변수를 함께 써야 갱신됩니다.

### 점검 순서
```bash
kubectl get pods -n stock-honggildong -o wide           # Ready 여부, IP
kubectl get endpoints stock-svc -n stock-honggildong    # Ready 파드 IP 가 엔드포인트에 있는지
kubectl run dns-test --rm -it --restart=Never --image=busybox:1.36 -n stock-honggildong -- \
  nslookup stock-0.stock-svc.stock-honggildong.svc.cluster.local
kubectl logs -n kube-system -l k8s-app=kube-dns         # CoreDNS 오류
```

### 자주 빠지는 함정: busybox `nslookup`과 짧은 이름
busybox의 `nslookup`은 Pod의 검색 도메인(`stock-honggildong.svc.cluster.local` 등)을 적용하지 않습니다.
그래서 정상적인 레코드인데도 짧은 이름으로 조회하면 실패합니다. k3s 클러스터에서 실측한 결과입니다.

| 조회 | 결과 |
|---|---|
| `nslookup stock-0.stock-svc` (busybox) | `NXDOMAIN` |
| `nslookup stock-0.stock-svc.stock-honggildong.svc.cluster.local` (busybox) | `10.42.0.6` 정상 응답 |
| `wget http://stock-0.stock-svc/` (nginx 파드 안) | 정상 응답 (일반 resolver는 검색 도메인 적용) |

DNS 문제를 의심할 때는 **전체 이름(FQDN)으로 먼저 조회**해, 레코드 문제인지 도구 문제인지부터 가르세요.
