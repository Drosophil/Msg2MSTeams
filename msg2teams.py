import logging
import os
from random import randint

import pymsteams
import requests


def get_quote():
    response = requests.get('https://zenquotes.io/api/random')
    quote, author = response.json()[0]['q'], response.json()[0]['a']
    return {'quote': quote, 'author': author}

def get_image():
    #  response = requests.get('https://yesno.wtf/api')
    image_url = f'https://picsum.photos/400/300/?random={randint(1,100)}'
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
    except Exception as emsg:
        logger.error(f'Something went wrong: {emsg}')

def main():
    quote = get_quote()
    image_url = get_image()
    send_message(quote['quote'], quote['author'], image_url)

# script execution entry point
logging.basicConfig(filename='msg2msteams.log',
                        filemode='a',
                        format='%(name)s: %(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)
logger = logging.getLogger('msg2msteams')
main()
