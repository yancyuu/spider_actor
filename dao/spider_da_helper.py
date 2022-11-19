# -*- coding: utf-8 -*-

import proto.spider.spider_pb2 as spider_pb

from dao.constants import DBConstants
from dao.mongodb_dao_helper import MongodbClientHelper
from common_sdk.data_transform import protobuf_transformer


class SpiderDAHelper(MongodbClientHelper):
    def __init__(self):
        db = DBConstants.MONGODB_SPIDER_DB_NAME
        coll = DBConstants.SPIDER_COLLECTION_NAME
        super().__init__(db, coll)

    @property
    def _spider_collection(self):
        return self

    async def add_or_update_spider(self, spider):
        matcher = {"id": spider.id}
        json_data = protobuf_transformer.protobuf_to_dict(spider)
        await self._spider_collection.do_replace(matcher, json_data, upsert=True)

    async def get_spider(self, id=None):
        matcher = {}
        self.__set_matcher_id(matcher, id)
        self.__set_matcher_not_parsed_status(matcher)
        if not matcher:
            return
        spider = await self._spider_collection.find_one(matcher)
        return protobuf_transformer.dict_to_protobuf(spider, spider_pb.spiderMessage)

    async def list_spiders(self, status=None):
        matcher = {}
        self.__set_matcher_status(matcher, status)
        self.__set_matcher_not_parsed_status(matcher)
        if not matcher:
            return []
        return await self._spider_collection.find(matcher)

    @staticmethod
    def __set_matcher_ids(matcher, ids):
        if ids is None:
            return
        matcher.update({"id": {"$in": ids}})

    @staticmethod
    def __set_matcher_id(matcher, id):
        if id is None:
            return
        matcher.update({"id": id})

    @staticmethod
    def __set_matcher_status(matcher, status):
        if status is None:
            return
        if isinstance(status, str):
            matcher.update({"status": status})
        elif isinstance(status, spider_pb.SpiderMessage.SpiderStatus):
            matcher.update({"status": spider_pb.SpiderMessage.SpiderStatus.Name(status)})
        matcher.update({"status": status})

    @staticmethod
    def __set_matcher_not_parsed_status(matcher):
        matcher.update({"status": {"$ne": "PARSED"}})

