# -*- coding: utf-8 -*-

import spider_common.proto.cookie.cookie_pb2 as cookie_pb

from dao.constants import DBConstants
from dao.mongodb_dao_helper import MongodbClientHelper
from common_sdk.data_transform import protobuf_transformer


class CookieDAHelper(MongodbClientHelper):
    def __init__(self):
        db = DBConstants.MONGODB_SPIDER_DB_NAME
        coll = DBConstants.COOKIE_COLLECTION_NAME
        super().__init__(db, coll)

    @property
    def _cookie_collection(self):
        return self

    async def add_or_update_cookie(self, cookie):
        matcher = {"id": cookie.id}
        json_data = protobuf_transformer.protobuf_to_dict(cookie)
        print(cookie)
        await self._cookie_collection.do_replace(matcher, json_data, upsert=True)

    async def get_cookie(self, id=None):
        matcher = {}
        self.__set_matcher_id(matcher, id)
        self.__set_matcher_not_used_status(matcher)
        if not matcher:
            return
        cookie = await self._cookie_collection.find_one(matcher)
        return protobuf_transformer.dict_to_protobuf(cookie, cookie_pb.CookieMessage)

    async def list_cookies(self, status=None):
        matcher = {}
        self.__set_matcher_status(matcher, status)
        self.__set_matcher_not_used_status(matcher)
        if not matcher:
            return []
        cookie_list = await self._cookie_collection.find(matcher)
        return protobuf_transformer.batch_dict_to_protobuf(cookie_list, cookie_pb.CookieMessage)

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
        elif isinstance(status, cookie_pb.CookieMessage.CookieStatus):
            matcher.update({"status": cookie_pb.CookieMessage.CookieStatus.Name(status)})
        matcher.update({"status": status})

    @staticmethod
    def __set_matcher_not_used_status(matcher):
        matcher.update({"status": {"$ne": "USED"}})

