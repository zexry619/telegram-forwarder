import time

from telethon import events, utils
from telethon.sync import TelegramClient
from telethon.tl import types

from FastTelethon import download_file, upload_file

api_id: int = 
api_hash: str = ""
token = ""
client = TelegramClient("bot", api_id, api_hash)

client.start(bot_token=token)
file_to_upload = "bunny.mp4"


class Timer:
    def __init__(self, time_between=2):
        self.start_time = time.time()
        self.time_between = time_between

    def can_send(self):
        if time.time() > (self.start_time + self.time_between):
            self.start_time = time.time()
            return True
        return False


@client.on(events.NewMessage())
async def download_or_upload(event):
    type_of = ""
    msg = None
    timer = Timer()

    async def progress_bar(current, total):
        if timer.can_send():
            await msg.edit("{} {}%".format(type_of, current * 100 / total))

    if event.document:
        type_of = "download"
        msg = await event.reply("downloading started")
        with open(event.file.name, "wb") as out:
            await download_file(event.client, event.document, out, progress_callback=progress_bar)
        await msg.edit("Finished downloading")

    else:
        type_of = "upload"
        msg = await event.reply("uploading started")
        with open(file_to_upload, "rb") as out:
            res = await upload_file(client, out, progress_callback=progress_bar)
            # result is InputFile()
            # you can add more data to it
            attributes, mime_type = utils.get_attributes(
                file_to_upload,
            )
            media = types.InputMediaUploadedDocument(
                file=res,
                mime_type=mime_type,
                attributes=attributes,
                # not needed for most files, thumb=thumb,
                force_file=False
            )
            await msg.edit("Finished uploading")
            await event.reply(file=media)
            # or just send it as it is
            await event.reply(file=res)


client.run_until_disconnected()