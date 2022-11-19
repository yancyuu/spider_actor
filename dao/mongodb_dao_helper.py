# -*- coding: utf-8 -*-

import asyncio

import motor

from common_sdk.base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from common_sdk.logging.logger import logger
import os
from motor.motor_asyncio import AsyncIOMotorClient


class MongodbClientHelper(metaclass=SingletonMetaclass):

    def __init__(self, db, coll):
        host = os.environ.get("MONGODB_ADDRESS")
        port = int(os.environ.get("MONGODB_PORT"))
        username = os.environ.get("MONGODB_USER_NAME")
        password = os.environ.get("MONGODB_ROOT_PASSWORD")
        replica_set = os.environ.get("MONGODB_REPLICA_SET")
        logger.info('mongodb://{}:{}@{}:{}/'.format(username, password, host, port, replica_set))
        if replica_set:
            self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
                'mongodb://{}:{}@{}:{}/?replicaSet={}'.format(username, password, host, port, replica_set))
        else:
            self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
                'mongodb://{}:{}@{}:{}/'.format(username, password, host, port))
        self.db = self.mongo_client[db]
        self.coll = coll
        self.mongo_client.get_io_loop = asyncio.get_event_loop

    async def insert(self, data):
        c = self.db[self.coll]

        if isinstance(data, dict):
            res = await c.insert_one(data)
            print(f'inserted {len(res.inserted_ids)} docs in {self.coll}')
        elif isinstance(data, list):
            res = await c.insert_many(data)
            print(f'inserted {len(res.inserted_ids)} docs in {self.coll}')

        else:
            print(f"data type only is List or Dict，no {type(data)}")

    async def find_one(self, *args, **kwargs):
        c = self.db[self.coll]
        return await c.find_one(*args, **kwargs)

    async def find(self, *args, **kwargs):
        c = self.db[self.coll]
        cursor = c.find(*args, **kwargs)
        return [dic async for dic in cursor]

    async def do_replace(self, *args, **kwargs):
        c = self.db[self.coll]
        print("data do replace {} {}".format(*args, **kwargs))
        result = await c.replace_one(*args, **kwargs)
        print("after replace {}".format(result))

    def close(self):
        self.mongo_client.close()
