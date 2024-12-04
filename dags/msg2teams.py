import logging
import os
from random import randint
from time import sleep

import pymsteams
import requests
from requests.exceptions import HTTPError, ConnectionError

from data2aws import AWSLoader
import logging_config

def get_quote():
    try:
        response = requests.get('https://zenquotes.io/api/random')
        if response.status_code == 200:
            quote, author = response.json()[0]['q'], response.json()[0]['a']
            return {'status': 200, 'quote': quote, 'author': author}
        else:
            logger.error(f'failed to get a quote. Status: {response.status_code}')
            return {'status': response.status_code}
    except (HTTPError, ConnectionError, TimeoutError) as emsg:
        logger.error(f'Network error: {emsg}')

def get_toad():
    logger.info('No toads to send yet')
    return {'status': 404}

def get_image(rand_start=1, rand_stop=100):
    image_url = f'https://picsum.photos/400/300/?random={randint(rand_start,rand_stop)}'
    try:
        response = requests.get(image_url)
    except (HTTPError, ConnectionError, TimeoutError) as emsg:
        logger.error(f'HTTP error while fetching the image: {emsg}')
        return None
    else:
        if response.status_code == 200:
            return {'image_url': image_url, 'image': response.content}
        else:
            return None


def send_message(quote: str, author: str, img_url: str):
    message = pymsteams.connectorcard(os.environ['MSTEAMS_WEBHOOK'])
    message.title('Quote of the Day (from Dmitriy Filyukov):')
    message.text(f'{quote}\n\n_{author}_\n\n![Image]({img_url})')
    try:
        message.send()
        logger.info(f'Message sent.')
    except pymsteams.TeamsWebhookException as emsg:
        logger.error(f'MSTeams webhook error: {emsg}')
    except (HTTPError, ConnectionError, TimeoutError) as emsg:
        logger.error(f'Network error: {emsg}')


def daily_quote():
    image = None
    for count in range(10):  # Trying to download image 10 times
        image = get_image()
        if image:
            break
        else:
            logger.error(f'No image to send on step {count+1}. Trying again...')
            sleep(1)  #  wait for some time just in case something wrong with the network or images API
    if image:
        no_duplicates = False
        for count in range(10):  # Trying to receive quote 10 times
            quote = get_quote()
            if (quote["status"] == 200) and \
                    (no_duplicates := aws_loader.load_quote_to_aws(
                        quote['quote'],
                        quote['author'],
                        image["image_url"],
                        image["image"]
                    )):
                logger.info(f'{quote["quote"]}, {quote["author"]}, {image["image_url"]}')
                #  send message to MSTeams
                send_message(quote['quote'], quote['author'], image["image_url"])
                break
            else:
                logger.error(f'No quote to send on try {count+1}. Trying again...')
                sleep(1)  #  wait for some time just in case something wrong with the network or quotes API
                continue
        if not (quote["status"] == 200) or not no_duplicates:
            logger.error('No quote to send. Aborted.')
    else:
        logger.error('Cannot access image. Aborted.')

# script execution entry point

logger = logging.getLogger('msg2msteams')
aws_loader = AWSLoader(os.environ['BUCKET_NAME'], os.environ['FOLDER_NAME'])
