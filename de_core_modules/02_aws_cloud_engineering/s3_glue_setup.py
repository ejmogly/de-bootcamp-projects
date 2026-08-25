import boto3

def setup_s3_data_lake(bucket_name, region='ap-northeast-2'):
    s3 = boto3.client('s3', region_name=region)
    # Create bucket with server-side encryption & lifecycle rules
    print(f'Setting up S3 bucket: {bucket_name}')

if __name__ == '__main__':
    setup_s3_data_lake('my-analytics-lake-2026')
