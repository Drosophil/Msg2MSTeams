import logging
import os
import boto3
import psycopg2
from psycopg2 import sql
from psycopg2.errors import UniqueViolation
from datetime import datetime

import logging_config

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
        if not self.client:
            logger.error('Cannot connect to S3 service.')
        else:
            logger.info('Connected to S3.')

    def save_image_to_S3(self, image, timestamp_string: str):
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=self.folder_name+timestamp_string,
            Body=image
        )
        return f'{self.client.meta.endpoint_url}/{self.bucket_name}/{self.folder_name}{timestamp_string}'


    def get_image_from_S3(self, image_name, file_name):
        with open(file_name, 'wb') as f:
            self.client.download_fileobj(self.bucket_name, self.folder_name+image_name, f)

class DataLoaderToRDS():
    def __init__(self):
        try:
            self.DB_HOST = 'de-database.ccpquxzb7z8t.us-east-2.rds.amazonaws.com'  # os.environ['DB_HOST']
            self.DB_PORT = int(os.environ['DB_PORT'])
            self.DB_NAME = os.environ['DB_NAME']
            self.DB_USER = os.environ['DB_USER']
            self.DB_PASSWORD = os.environ['DB_PASSWORD']
        except KeyError as db_credentials_error:
            logger.error(f'DB credentials error. Not set: {db_credentials_error.args}.')
            raise db_credentials_error

        self.db = psycopg2.connect(
            host=self.DB_HOST,
            port=self.DB_PORT,
            dbname=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD,
        )
        if not self.db:
            logger.error('Cannot connect to RDS.')
        else:
            logger.info('Connected to DB.')

        create_table_query = sql.SQL('''CREATE TABLE IF NOT EXISTS dfilyukov.daily_quotes (
                                id SERIAL PRIMARY KEY,
                                quote VARCHAR,
                                author VARCHAR,
                                sending_time VARCHAR,
                                picture_url VARCHAR,
                                UNIQUE (quote, author)
                                );
                                ''')

        with self.db.cursor() as cursor:
            cursor.execute(create_table_query)

        self.db.commit()

    def insert_data_to_RDS(self,
                           quote: str,
                           author: str,
                           sending_time: str,
                           picture_url: str,
                           ):
        insert_query = sql.SQL('''
            INSERT INTO dfilyukov.daily_quotes (
                                quote,
                                author,
                                sending_time,
                                picture_url)
                                VALUES (%s, %s, %s, %s);
                                ''')
        try:
            with self.db.cursor() as cursor:
                cursor.execute(insert_query, (quote, author, sending_time, picture_url))
        except UniqueViolation as unv:
            logger.warning(f'{unv} Duplicate quote: {quote} by {author}')
            self.db.commit()
            return False
        self.db.commit()
        return True


class AWSLoader():  #  main class to be called for data management
    def __init__(self, bucket_name: str, folder_name: str):
        self.S3 = ImageSaverToS3(bucket_name, folder_name)
        self.RDS = DataLoaderToRDS()

    def load_quote_to_aws(self,
                          quote: str,
                          author: str,
                          picture_url: str,
                          image,
                          ):

        sending_time = datetime.now().strftime('%Y.%m.%d__%H-%M-%S')  #  timestamp for the quote and image name

        if not self.RDS.insert_data_to_RDS(
                quote,
                author,
                sending_time,
                picture_url,
        ):
            return False
        else:
            self.S3.save_image_to_S3(image, sending_time)
            return True
