# TODO/TODECIDE:
#   should 'username' be a part of the Channel definition somehow?
#   still need to do some planning for handling media

import cisticola
import cisticola.scraper.twitter
from sqlalchemy import create_engine


test_channels = [cisticola.base.Channel(id=0, name="Logan Williams (test)", platform_id=891729132,
                                   category="test", followers=None, platform="Twitter",
                                   url="https://twitter.com/obtusatum", country="US",
                                   influencer=None, public=True, chat=False,
                                   notes=""),
                 cisticola.base.Channel(id=1, name="JQHN SPARTAN", platform_id=-1001181961026,
                                   category="qanon", followers=None, platform="Telegram",
                                   url="https://t.me/jqhnspartan", country="FR",
                                   influencer="JQNH SPARTAN", public=True, chat=False, notes=""),
                 cisticola.base.Channel(id=2, name="LizardRepublic", platform_id='lizardrepublic',
                                   category="qanon", followers=None, platform="Gettr",
                                   url="https://www.gettr.com/user/lizardrepublic", country="US",
                                   influencer=None, public=True, chat=False, notes=""),]


controller = cisticola.ScraperController()

scraper = cisticola.scraper.twitter.TwitterScraper()
controller.register_scraper(scraper)

engine = create_engine('sqlite:///test.db')
controller.connect_to_db(engine)

controller.scrape_channels(test_channels)

