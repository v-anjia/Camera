import os
import asyncio
import logging
import humanize
import numpy as np

from .local_sync import get_local_sync_processor

log = logging.getLogger()


class BaseNumpySync:

    def __init__(self, model, loop=None):
        self.model = model
        name = self.model.__name__.lower()
        self.filename = '{}.npy'.format(name)
        self.array = self.load_or_create()

        self.loop = loop if loop else asyncio.get_event_loop()
        self.sync_task = None
        self.sync_enabled = True

    def start_sync_task(self, interval):
        self.sync_task = asyncio.Task(self._sync(interval), loop=self.loop)

    def stop_sync_task(self):
        self.sync_enabled = False

    def load_or_create(self):

        if os.path.exists(self.filename):
            log.info("loading {}".format(self.filename))
            return np.load(self.filename, allow_pickle=False)

        return np.array([], dtype=self.model.get_numpy_dtypes())

    def save(self):
        np.save(self.filename, self.array, allow_pickle=False)
        log.debug("Saved {} ({})".format(self.filename, humanize.naturalsize(os.stat(self.filename).st_size)))

    async def _sync(self, interval):
        log.debug("Starting sync on {} @ {}s".format(self.model, interval))

        while True:
            for _ in range(interval):
                await asyncio.sleep(1, loop=self.loop)
                if not self.sync_enabled:
                    log.debug("Stopped sync on {}".format(self.model))
                    return

            await self.sync()

    async def sync(self):

        total = 0

        async def process(items):
            nonlocal total

            items = list(items)

            total += len(items)

            additions = []

            # pull out existing values, much faster than numpy.where()
            # probably a bit inefficient on small batches and large existing array :P
            existing = {v: i for i, v in enumerate(self.array['id'])}

            for item in items:
                i = existing.get(item[0])

                if i is not None:
                    self.array[i] = item
                else:
                    additions.append(item)

            if additions:
                data = np.array(additions, dtype=self.array.dtype)

                log.debug("Added {} items @ {} bytes => {}".format(data.size, data.itemsize,
                                                                   humanize.naturalsize(data.size * data.itemsize)))

                self.array = np.append(self.array, data, axis=0)

            log.debug("Currently {} items => {}".format(self.array.size, humanize.naturalsize(self.array.size * self.array.itemsize)))

        processor = await get_local_sync_processor(model=self.model,
                                                   object=self.model.get_async_manager(),
                                                   process=process,
                                                   row_output_fun=self.model.to_numpy_row
                                                   )

        await processor.process(limit=50000, i=10, stop_when_caught_up=True)

        # todo: peewee-sync issue. using "is_unique_key" causes it to get last record again when caught up.
        if total > 1:
            log.debug("total records is {}".format(total))
            await asyncio.get_event_loop().run_in_executor(None, self.save)

        return total > 0


# todo: these are data specific

class NumpySync(BaseNumpySync):

    def get_item(self, field, value):
        indexes = np.where(np.logical_and(self.array[field] == value, self.array['active'] == True))[0]

        if indexes.size > 1:
            raise NotImplementedError("Multiple found. Should only be one")
        elif indexes.size == 0:
            return None

        return self.array[indexes[0]]

    def get_item_model(self, field, value):
        row = self.get_item(field=field, value=value)
        if row:
            return self.model.from_numpy_row(row)

    def get_item_dict(self, field, value):
        row = self.get_item(field=field, value=value)
        if row:
            return self.model.from_numpy_row_to_dict(row)

    def filter_by_field(self, field, value):
        return np.array(self.array)[np.where(np.logical_and(self.array[field] == value, self.array['active'] == True))]

    def get_items_dicts(self, field, value):
        return [
            self.model.from_numpy_row_to_dict(row) for row in self.filter_by_field(field=field, value=value)
        ]

