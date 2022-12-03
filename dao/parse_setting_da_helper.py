# -*- coding: utf-8 -*-

from common_sdk.data_transform import protobuf_transformer
from dao.constants import DBConstants
from dao.mongodb_dao_helper import MongodbClientHelper
import proto.spider.parse_setting_pb2 as parse_setting_pb


class ParseSettingDAHelper(MongodbClientHelper):
    def __init__(self):
        db = DBConstants.MONGODB_SPIDER_DB_NAME
        coll = DBConstants.PARSE_SETTING_COLLECTION_NAME
        super().__init__(db, coll)

    @property
    def _parse_setting_collection(self):
        return self

    async def add_or_update_parse_setting(self, parse_setting):
        matcher = {"id": parse_setting.id}
        json_data = protobuf_transformer.protobuf_to_dict(parse_setting)
        await self._parse_setting_collection.do_replace(matcher, json_data, upsert=True)

    async def get_parse_setting(self, id=None):
        matcher = {}
        self.__set_active_status(matcher)
        self.__set_matcher_id(matcher, id)
        if not matcher:
            return
        parse_setting = await self._parse_setting_collection.find_one(matcher)
        return protobuf_transformer.dict_to_protobuf(parse_setting, parse_setting_pb.ParseSettingMessage)

    async def list_parse_settings(self, status=None, ids=None):
        matcher = {}
        self.__set_active_status(matcher)
        self.__set_matcher_status(matcher, status)
        self.__set_matcher_ids(matcher, ids)
        if not matcher:
            return []
        return await self._parse_setting_collection.find(matcher)

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
    def __set_active_status(matcher):
        matcher.update({"status": {"$ne": "DELETED"}})

    @staticmethod
    def __set_matcher_status(matcher, status):
        if status is None:
            return
        if isinstance(status, str):
            matcher.update({"status": status})
        elif isinstance(status, parse_setting_pb.ParseSettingMessage.ParseStatus):
            matcher.update({"status": parse_setting_pb.ParseSettingMessage.ParseStatus.Name(status)})
        matcher.update({"status": status})
