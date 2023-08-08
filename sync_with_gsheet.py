import os
import time

import gspread
from loguru import logger

from cisticola.base import Channel, ChannelInfo

expected_headers = [
    "id",
    "name",
    "platform_id",
    "category",
    "platform",
    "url",
    "screenname",
    "country",
    "source",
    "influencer",
    "public",
    "chat",
    "notes",
    "normalized_url",
    "to_remove",
]


def standardize_country(s):
    _s = s.split("(")[0].split("?")[0]
    return _s.strip()


def sync_channels(args, session):
    logger.info("Synchronizing channels")

    gc = gspread.service_account(filename="service_account.json")

    # Open a sheet from a spreadsheet in one go
    wks = gc.open_by_url(os.environ["GSHEET"]).worksheet("channels")
    channels = wks.get_all_records(expected_headers=expected_headers)
    row = 2

    for c in channels:
        # defaults for unset values
        if c["public"] == "":
            c["public"] = True
        if c["chat"] == "":
            c["chat"] = False

        # normalize the values slightly from the Google Sheet
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
            del c["normalized_url"]
            del c["to_remove"]

            # check to see if this already exists,
            platform_id = None
            if c["platform_id"] != "":
                platform_id = c["platform_id"]

            channel = (
                session.query(Channel)
                .filter_by(platform_id=str(platform_id), platform=str(c["platform"]))
                .first()
            )

            if not channel:
                channel = (
                    session.query(Channel)
                    .filter_by(platform=str(c["platform"]), url=str(c["url"]))
                    .first()
                )

            if not channel and c["screenname"] != "" and c["screenname"] is not None:
                channel = (
                    session.query(Channel)
                    .filter_by(
                        platform=str(c["platform"]), screenname=str(c["screenname"])
                    )
                    .first()
                )

            if not channel:
                if all([k in [None, True, False, ""] for k in c.values()]):
                    # end sync if completely empty row is encountered
                    break

                channel = Channel(**c)

                logger.debug(f"{channel} does not exist, adding")
                session.add(channel)
                session.flush()
                session.commit()

                wks.update_cell(row, 1, channel.id)
                time.sleep(1)
            else:
                logger.info(f"Channel found, updating channel {channel}")
                was_researcher = channel.source == "researcher"

                # Update only non-empty/none values from the sheet
                if c["name"]:
                    channel.name = c["name"]
                if c["category"]:
                    channel.category = c["category"]
                if c["platform"]:
                    channel.platform = c["platform"]
                if c["url"]:
                    channel.url = c["url"]
                if c["screenname"]:
                    channel.screenname = c["screenname"]
                if c["country"]:
                    channel.country = (
                        None
                        if c["country"] is None
                        else list(map(standardize_country, c["country"].split("/")))
                    )
                if c["influencer"]:
                    channel.influencer = c["influencer"]
                if c["public"]:
                    channel.public = c["public"]
                if c["chat"]:
                    channel.chat = c["chat"]
                if c["notes"]:
                    channel.notes = c["notes"]
                if c["source"]:
                    channel.source = c["source"]

                session.flush()
                session.commit()

                wks.update_cell(row, 1, channel.id)
                time.sleep(1)

                # this likely means that the channel was duplicated in the Google Sheet, so add a red highlight
                if was_researcher:
                    logger.warning(
                        f"This channel (ID {channel.id}) is possibly a duplicate."
                    )

                    wks.format(
                        f"A{str(row)}:A{str(row)}",
                        {"backgroundColor": {"red": 1.0, "green": 0.0, "blue": 0.0}},
                    )
                    time.sleep(1)

        # channel has ID
        else:
            cid = int(c["id"])

            channel = session.query(Channel).filter_by(id=cid).first()
            channel_info = (
                session.query(ChannelInfo)
                .filter_by(channel=cid)
                .order_by(ChannelInfo.date_archived.desc())
                .first()
            )

            logger.info(f"Updating channel {channel}")
            logger.info(f"Found info {channel_info}")

            channel.name = c["name"]
            channel.category = c["category"]
            channel.platform = c["platform"]
            channel.url = c["url"]
            channel.screenname = c["screenname"]
            channel.country = (
                None
                if c["country"] is None
                else list(map(standardize_country, c["country"].split("/")))
            )
            channel.influencer = c["influencer"]
            channel.public = c["public"]
            channel.chat = c["chat"]
            channel.notes = c["notes"]
            channel.source = c["source"]

            if channel_info and channel.screenname != channel_info.screenname:
                channel.screenname = channel_info.screenname
                wks.update_cell(row, 7, channel_info.screenname)
                time.sleep(1)

            if channel_info and str(channel.platform_id) != str(
                channel_info.platform_id
            ):
                channel.platform_id = channel_info.platform_id
                wks.update_cell(row, 3, channel_info.platform_id)
                time.sleep(1)

            session.flush()
            session.commit()

        row += 1

    session.commit()
