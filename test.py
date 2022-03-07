from sqlalchemy import create_engine

from cisticola.base import Channel
from cisticola.scraper import (
    ScraperController,
    BitchuteScraper,
    GabScraper,
    GettrScraper,
    OdyseeScraper,
    RumbleScraper,
    TelegramSnscrapeScraper,
    TwitterScraper)

test_channels = [
    Channel(
        id=0, 
        name="Logan Williams (test)", 
        platform_id=891729132,
        category="test", 
        followers=None, 
        platform="Twitter",
        url="https://twitter.com/obtusatum", 
        screenname="obtusatum", 
        country="US",
        influencer=None, 
        public=True, 
        chat=False,
        notes=""),
    Channel(
        id=1, 
        name="South West Ohio Proud Boys (test)", 
        platform_id=-1001276612436,
        category="test", 
        followers=None, 
        platform="Telegram",
        url="https://t.me/SouthwestOhioPB", 
        screenname="SouthwestOhioPB", 
        country="US",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=2, 
        name="LizardRepublic (test)", 
        platform_id='lizardrepublic',
        category="test", 
        followers=None, 
        platform="Gettr",
        url="https://www.gettr.com/user/lizardrepublic", 
        screenname="lizardrepublic", 
        country="US",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=4, 
        name="bestonlinejewelrystoresusa@gmail.com (test)", platform_id='bestonlinejewelrystoresusagmailcom',
        category="test", 
        followers=None, 
        platform="Bitchute",
        url="https://www.bitchute.com/channel/bestonlinejewelrystoresusagmailcom/", screenname=None, 
        country="US",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=5, 
        name="Mak1n' Bacon (test)", 
        platform_id='Mak1nBacon',
        category="test", 
        followers=None, 
        platform="Odysee",
        url="https://odysee.com/@Mak1nBacon", 
        screenname='Mak1nBacon', 
        country="US",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=6, 
        name="Capt. Marc Simon (test)", 
        platform_id='marc_capt',
        category="test", 
        followers=None, 
        platform="Gab",
        url="https://gab.com/marc_capt", 
        screenname='marc_capt', 
        country="CA",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=7, 
        name="we are uploading videos wow products and problem solving products.please share like and subscribe our channelwe are uploading videos wow products and problem solving products.please share like and subscribe our channel", platform_id='c-916305',
        category="test", 
        followers=None, 
        platform="Rumble",
        url="https://rumble.com/c/c-916305", 
        screenname='we are uploading', 
        country="CA",
        influencer=None, 
        public=True, 
        chat=False, 
        notes="")]

controller = ScraperController()

scrapers = [
    BitchuteScraper(),
    GabScraper(),
    GettrScraper(),
    OdyseeScraper(),
    RumbleScraper(),
    TelegramSnscrapeScraper(),
    TwitterScraper()]

controller.register_scrapers(scrapers)

engine = create_engine('sqlite:///test3.db')
controller.connect_to_db(engine)

controller.scrape_channels(test_channels)