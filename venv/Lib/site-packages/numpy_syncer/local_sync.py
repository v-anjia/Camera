import logging
import os
import json
import aiofiles
from peewee_syncer import SyncManager, AsyncProcessor

log = logging.getLogger(__name__)


class LocalSyncManager(SyncManager):

    def __init__(self, name):
        super().__init__()
        self.sync_file = '{}.sync'.format(name)

    async def load(self):
        if os.path.exists(self.sync_file):

            async with aiofiles.open(self.sync_file, mode='r') as f:
                data = json.loads(await f.read())

            self.set_last_offset(**data)

        else:
            self.set_last_offset(value=0, offset=0)

    async def save(self):
        async with aiofiles.open(self.sync_file, mode='w') as f:
            await f.write(json.dumps(self.get_last_offset(), indent=True))

class LocalProcessor(AsyncProcessor):

    async def save(self):
        log.info("Save offset={}".format(self.sync_manager.get_meta()['value']))
        await self.sync_manager.save()

async def get_local_sync_processor(name, it, process):

    sync_manager = LocalSyncManager(name)
    await sync_manager.load()

    return LocalProcessor(
        sync_manager=sync_manager,
        it_function=it,
        # A process function (iterates over the iterator)
        process_function=process,
        # Pause up to 1 seconds on each iteration (percentage of records vs limit processed)
        sleep_duration=1,
        # todo: fixme
        object=None
    )
