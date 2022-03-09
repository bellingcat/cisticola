import sys

from sqlalchemy import create_engine
from loguru import logger

from cisticola.base import Channel
from cisticola.scraper import (
    ScraperController,
    TelegramSnscrapeScraper)

logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("../russian_telegram_ingest.log")

test_channels = [
    # Channel(
    #     id=0, 
    #     name="QAnon Россия", 
    #     platform_id=-1001319637748,
    #     category="Qanon", 
    #     followers=94048, 
    #     platform="Telegram",
    #     url="https://t.me/qanonrus", 
    #     screenname="qanonrus", 
    #     country="RU",
    #     influencer=None, 
    #     public=True, 
    #     chat=False,
    #     notes=""),
    # Channel(
    #     id=1, 
    #     name="The Great Awakening | Q", 
    #     platform_id=-1001325597521,
    #     category="Qanon", 
    #     followers=5715,
    #     platform="Telegram",
    #     url="https://t.me/greatawakin", 
    #     screenname="greatawakin", 
    #     country="RU",
    #     influencer=None, 
    #     public=True, 
    #     chat=False, 
    #     notes=""),
    # Channel(
    #     id=2, 
    #     name="Великое Пробуждение", 
    #     platform_id=-1001285898079,
    #     category="Qanon", 
    #     followers=5861, 
    #     platform="Telegram",
    #     url="https://t.me/greatawakeningrus", 
    #     screenname="greatawakeningrus", 
    #     country="RU",
    #     influencer=None, 
    #     public=True, 
    #     chat=False, 
    #     notes=""),
    Channel(
        id=3, 
        name="T🕊Редакция Президент Гордон🕊", 
        platform_id=-1001101170442,
        category="Qanon", 
        followers=5743, 
        platform="Telegram",
        url="https://t.me/prezidentgordonteam", 
        screenname="prezidentgordonteam", 
        country="RU",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=4, 
        name="ПРОЕКТ АВРОРА", 
        platform_id=-1001279171101,
        category="Qanon", 
        followers=5930, 
        platform="Telegram",
        url="https://t.me/project_aurora", 
        screenname="project_aurora", 
        country="RU",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=5, 
        name="Сон Разума", 
        platform_id=-1001202338312,
        category="Qanon", 
        followers=27099, 
        platform="Telegram",
        url="https://t.me/error_288", 
        screenname="error_288", 
        country="RU",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=6, 
        name="Пробуждающий Мир - официальный канал", 
        platform_id=-1001492521207,
        category="Qanon", 
        followers=19097, 
        platform="Telegram",
        url="https://t.me/promirru", 
        screenname="promirru", 
        country="RU",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),
    Channel(
        id=7, 
        name="ЦЕЛЬНОЗОР", 
        platform_id=-1001642737506,
        category="Qanon", 
        followers=13654, 
        platform="Telegram",
        url="https://t.me/tselnozor", 
        screenname="tselnozor", 
        country="RU",
        influencer=None, 
        public=True, 
        chat=False, 
        notes=""),]

controller = ScraperController()

telegram = TelegramSnscrapeScraper()
controller.register_scraper(telegram)

engine = create_engine('sqlite:///russian_telegram.db')
controller.connect_to_db(engine)

controller.scrape_channels(test_channels)

