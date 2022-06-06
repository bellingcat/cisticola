import gspread
import time
from loguru import logger

from cisticola.base import Channel, ChannelInfo

def sync_channels(args, session):
    logger.info("Synchronizing channels")

    gc = gspread.service_account(filename="service_account.json")

    # Open a sheet from a spreadsheet in one go
    wks = gc.open_by_url(args.gsheet).worksheet("channels")
    channels = wks.get_all_records()
    row = 2

    for c in channels:
        if c["public"] == "":
            c["public"] = False
        if c["chat"] == "":
            c["chat"] = False

        for k in c.keys():
            if c[k] == "TRUE" or c[k] == "yes":
                c[k] = True
            if c[k] == "FALSE" or c[k] == "no":
                c[k] = False

            if c[k] == "":
                c[k] = None

        # add new channel
        if c["id"] == "" or c["id"] is None:
            del c["id"]

            # check to see if this already exists,
            platform_id = None
            if c["platform_id"] != "":
                platform_id = c["platform_id"]

            channel = (
                session.query(Channel)
                .filter_by(
                    platform_id=str(platform_id), platform=str(c["platform"])
                )
                .first()
            )

            if not channel:
                channel = session.query(Channel).filter_by(platform=str(c["platform"]), url=str(c["url"])).first()

            if not channel and c["screenname"] != '' and c["screenname"] is not None:
                channel = session.query(Channel).filter_by(platform=str(c["platform"]), screenname=str(c["screenname"])).first()

            if not channel:
                channel = Channel(**c, source="researcher")
                logger.debug(f"{channel} does not exist, adding")
                session.add(channel)
                session.flush()
                session.commit()

                wks.update_cell(row, 1, channel.id)
                time.sleep(1)
            else:
                logger.info(f"Channel found, updating channel {channel}")
                was_researcher = channel.source == "researcher"

                channel.name = c["name"]
                channel.category = c["category"]
                channel.platform = c["platform"]
                channel.url = c["url"]
                channel.screenname = c["screenname"]
                channel.country = c["country"]
                channel.influencer = c["influencer"]
                channel.public = c["public"]
                channel.chat = c["chat"]
                channel.notes = c["notes"]
                channel.source = "researcher"

                session.flush()
                session.commit()

                wks.update_cell(row, 1, channel.id)

                # this likely means that the channel was duplicated in the Google Sheet, so add a red highlight
                if was_researcher:
                    wks.format(f"A{str(row)}:A{str(row)}", {
                        "backgroundColor": {
                            "red": 1.0,
                            "green": 0.0,
                            "blue": 0.0
                    }})

                time.sleep(1)

        # channel has ID
        else:
            cid = int(c["id"])

            channel = session.query(Channel).filter_by(id=cid).first()
            channel_info = session.query(ChannelInfo).filter_by(channel=cid).order_by(ChannelInfo.date_archived.desc()).first()

            logger.info(f"Updating channel {channel}")
            logger.info(channel_info)

            channel.name = c["name"]
            channel.category = c["category"]
            channel.platform = c["platform"]
            channel.url = c["url"]
            channel.screenname = c["screenname"]
            channel.country = c["country"]
            channel.influencer = c["influencer"]
            channel.public = c["public"]
            channel.chat = c["chat"]
            channel.notes = c["notes"]
            channel.source = "researcher"

            if channel_info:
                channel.screenname = channel_info.screenname
                wks.update_cell(row, 7, channel_info.screenname)

                channel.platform_id = channel_info.platform_id
                wks.update_cell(row, 3, channel_info.platform_id)

            session.flush()
            session.commit()

        row += 1

    session.commit()
