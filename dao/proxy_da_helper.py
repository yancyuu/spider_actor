# -*- coding: utf-8 -*-

import proto.proxy.proxy_pb2 as proxy_pb

from dao.constants import DBConstants
from dao.mongodb_dao_helper import MongodbClientHelper
from common_sdk.data_transform import protobuf_transformer


class ProxyDAHelper(MongodbClientHelper):
    def __init__(self):
        db = DBConstants.MONGODB_SPIDER_DB_NAME
        coll = DBConstants.PROXY_COLLECTION_NAME
        super().__init__(db, coll)

    @property
    def _proxy_collection(self):
        return self

    async def add_or_update_proxy(self, proxy):
        matcher = {"id": proxy.id}
        json_data = protobuf_transformer.protobuf_to_dict(proxy)
        print(proxy)
        await self._proxy_collection.do_replace(matcher, json_data, upsert=True)

    async def get_proxy(self, id=None):
        matcher = {}
        self.__set_matcher_id(matcher, id)
        self.__set_matcher_not_used_status(matcher)
        if not matcher:
            return
        proxy = await self._proxy_collection.find_one(matcher)
        return protobuf_transformer.dict_to_protobuf(proxy, proxy_pb.ProxyMessage)

    async def list_proxies(self, status=None):
        matcher = {}
        self.__set_matcher_status(matcher, status)
        self.__set_matcher_not_used_status(matcher)
        if not matcher:
            return []
        return await self._proxy_collection.find(matcher)

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
        elif isinstance(status, proxy_pb.ProxyMessage.ProxyStatus):
            matcher.update({"status": proxy_pb.ProxyMessage.ProxyStatus.Name(status)})
        matcher.update({"status": status})

    @staticmethod
    def __set_matcher_not_used_status(matcher):
        matcher.update({"status": {"$ne": "USED"}})

