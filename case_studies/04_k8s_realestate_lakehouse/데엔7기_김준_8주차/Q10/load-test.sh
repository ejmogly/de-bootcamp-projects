#!/bin/bash
# ==============================================================================
# 과제 10 — HPA 부하 생성 스크립트 (load-test.sh)
# 대상: http://realestate-svc.realestate-junekim.svc.cluster.local
# ==============================================================================

NAMESPACE="realestate-junekim"
SVC_URL="http://realestate-svc.${NAMESPACE}.svc.cluster.local/"

echo "=== [HPA 부하 테스트 시작] realestate-nginx 서비스 대상 ==="
kubectl delete pod load-gen load-gen-2 -n ${NAMESPACE} --ignore-not-found=true >/dev/null 2>&1

echo "1. 부하 생성 파드(load-gen) 기동 중..."
kubectl run load-gen --image=busybox -n ${NAMESPACE} --restart=Never -- /bin/sh -c "while sleep 0.002; do wget -q -O- ${SVC_URL} >/dev/null; done"

echo "2. 부하 생성 파드(load-gen-2) 병렬 기동 중..."
kubectl run load-gen-2 --image=busybox -n ${NAMESPACE} --restart=Never -- /bin/sh -c "while sleep 0.002; do wget -q -O- ${SVC_URL} >/dev/null; done"

echo "부하 생성 파드가 백그라운드에서 동작 중입니다."
echo "확인 명령어:"
echo "  kubectl get hpa realestate-hpa -n ${NAMESPACE} -w"
echo "  kubectl top pod -n ${NAMESPACE}"
echo "부하 중단 시:"
echo "  kubectl delete pod load-gen load-gen-2 -n ${NAMESPACE}"
