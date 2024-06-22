import logging
import os
import boto3
from datetime import datetime

logging.basicConfig(
    format='%(name)s: %(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger('data2aws')

class ImageSaverToS3:
    def __init__(self, bucket_name, folder_name):
        try:
            self.AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
            self.AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
            self.AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
        except KeyError as aws_credentials_error:
            logger.error(f'AWS credentials error. Not set: {aws_credentials_error.args}.')
            raise aws_credentials_error
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.client = boto3.client(
            service_name='s3',
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            aws_session_token=self.AWS_SESSION_TOKEN,
        )
    def save_image(self, image):
        timestamp_string = datetime.now().strftime('%Y%m%d%H%M%S')
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=self.folder_name+timestamp_string,
            Body=image,
        )
        print(timestamp_string)
        return f'{self.client.meta.endpoint_url}/{self.bucket_name}/{self.folder_name}{timestamp_string}'

    def get_image(self, image_name, file_name):
        with open(file_name, 'wb') as f:
            self.client.download_fileobj(self.bucket_name, self.folder_name+image_name, f)

