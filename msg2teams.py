import logging
import os
from random import randint

import pymsteams
import requests
from requests.exceptions import HTTPError, ConnectionError


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


def get_image_url(rand_start=1, rand_stop=100):
    #  response = requests.get('https://yesno.wtf/api')
    image_url = f'https://picsum.photos/400/300/?random={randint(rand_start,rand_stop)}'
    return image_url


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


def main():
    quote = get_quote()
    image_url = get_image_url()
    if quote["status"] == 200:
        logger.info(f'{quote["quote"]}, {quote["author"]}, {image_url}')
        send_message(quote['quote'], quote['author'], image_url)
    else:
        logger.error('No quote to send. Aborted.')

# script execution entry point
logging.basicConfig(
    filename='msg2msteams.log',
    filemode='a',
    format='%(name)s: %(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger('msg2msteams')
main()
