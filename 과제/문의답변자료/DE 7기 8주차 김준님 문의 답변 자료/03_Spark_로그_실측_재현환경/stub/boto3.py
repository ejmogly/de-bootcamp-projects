# 실측용 boto3 대역: AWS 자격증명 없이 process_stocks.py 를 끝까지 실행하기 위한 로컬 대체.
# Spark 동작에는 관여하지 않으며, S3 다운로드는 제공 CSV 복사로, 업로드·삭제는 아무 동작도 하지 않는다.
import shutil


class _FakeS3:
    def download_file(self, bucket, key, filename):
        shutil.copy("/work/data/stock_prices.csv", filename)

    def list_objects_v2(self, **kwargs):
        return {}

    def delete_objects(self, **kwargs):
        return {}

    def upload_file(self, filename, bucket, key):
        return None


def client(service_name, *args, **kwargs):
    return _FakeS3()
