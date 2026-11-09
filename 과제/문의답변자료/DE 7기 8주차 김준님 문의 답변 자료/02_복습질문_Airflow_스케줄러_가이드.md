# 복습 질문 — Dynamic Task Mapping과 TaskGroup 혼용 시 스케줄러 부하

> 참고: 7주차 Q3는 4개 task를 순차로 연결하는 DAG를 만드는 문항으로, Dynamic Task Mapping과 TaskGroup은 사용하지 않았습니다.
> 아래는 일반적인 실무 관점의 답변입니다.

## 부하가 생기는 이유
- Dynamic Task Mapping은 실행 시점에 입력 개수만큼 task instance를 만듭니다. TaskGroup 전체를 매핑하면(`@task_group` + `.expand()`) **그룹 안 task 수 × 매핑 개수**만큼 생깁니다.
- task instance가 많아질수록 메타DB 쓰기와 스케줄러의 상태 확인 루프가 늘어나, 스케줄링 지연과 메타DB 부하가 커집니다.

## 대응 방법

| 대상 | 방법 |
|---|---|
| 매핑 개수 | `max_map_length`(기본 1024)로 상한. 입력이 많으면 배치 단위로 묶어 매핑 개수 자체를 줄임 |
| 동시 실행 | `parallelism`(전체), `max_active_tasks_per_dag`(DAG별), `max_active_tis_per_dag`(task별)로 제한 |
| 외부 시스템 보호 | 외부 API·DB를 호출하는 매핑 task는 **Pool**로 동시 접근 수를 묶음 |
| DAG 파싱 | DAG 파일 최상단에서 API 호출·무거운 연산을 하지 않음. 매핑 입력 목록은 task 안에서 생성 |
| XCom | 매핑 입력·결과를 XCom에 크게 싣지 않음. 큰 데이터는 S3 경로만 넘김 |
| 실행 기록 | 오래된 DAG run·task instance는 `airflow db clean`으로 정리해 메타DB 크기 관리 |

## 8주차 Q1에 적용한다면
과제 01은 6개 시군구를 TaskGroup으로 고정 생성했습니다. 시군구 목록이 바뀔 수 있다면 `expand(lawd_cd=[…])`로 매핑하고,
data.go.kr 호출 task에 Pool(예: 슬롯 3개)을 걸어 API 동시 호출 수를 제한하는 구성이 실무에 가깝습니다.
