# -*- coding: utf-8 -*-

import time
from common_sdk.util.id_generator import generate_common_id
from dao.parse_setting_da_helper import ParseSettingDAHelper
from manager.manager_base import ManagerBase, ignore_none_param
import proto.spider.parse_setting_pb2 as parse_setting_pb


class ParseSettingManager(ManagerBase):
    def __init__(self):
        super().__init__()
        self._da_helper = None

    @property
    def da_helper(self):
        if not self._da_helper:
            self._da_helper = ParseSettingDAHelper()
        return self._da_helper

    @staticmethod
    def create_parse_setting(parse_setting):
        parse_setting.id = generate_common_id()
        parse_setting.create_time = int(time.time())
        return parse_setting

    async def get_parse_setting(self, id=None):
        return await self.da_helper.get_parse_setting(id=id)

    @ignore_none_param
    def update_parse_setting(self, parse_setting, parse_type=None, parse_rules=None, next_spider_rules=None, status=None, request_method=None, enable_next_spider_repeated=None):
        self.__update_status(parse_setting, status)
        self.__update_parse_type(parse_setting, parse_type)
        self.__update_parse_rules(parse_setting, parse_rules)
        self.__update_next_spider_rules(parse_setting, next_spider_rules)
        self.__update_request_method(parse_setting, request_method)
        self.__update_enable_next_spider_repeated(parse_setting, enable_next_spider_repeated)

    async def list_parse_settings(self, status=None, ids=None):
        proxies = await self.da_helper.list_parse_settings(
            status=status,
            ids=ids,
        )
        return proxies

    def delete_parse_setting(self, parse_setting):
        self.__update_status(parse_setting, parse_setting_pb.ParseSettingMessage.ParseStatus.DELETED)

    async def add_or_update_parse_setting(self, parse_setting):
        await self.da_helper.add_or_update_parse_setting(parse_setting)

    @ignore_none_param
    def __update_status(self, parse_setting, status):
        if isinstance(status, str):
            status = parse_setting_pb.ParseSettingMessage.ParseStatus.Value(status)
        if status == parse_setting_pb.ParseSettingMessage.ParseStatus.DELETED:
            parse_setting.delete_time = int(time.time())
        parse_setting.status = status

    @ignore_none_param
    def __update_parse_rules(self, parse_setting, parse_rules):
        parse_setting.parse_rules = parse_rules

    @ignore_none_param
    def __update_enable_next_spider_repeated(self, parse_setting, enable_next_spider_repeated):
        parse_setting.enable_next_spider_repeated = enable_next_spider_repeated

    @ignore_none_param
    def __update_parse_type(self, parse_setting, parse_type):
        if isinstance(parse_type, str):
            parse_type = parse_setting_pb.ParseSettingMessage.ParseType.Value(parse_type)
        parse_setting.parse_type = parse_type

    @ignore_none_param
    def __update_request_method(self, parse_setting, request_method):
        if isinstance(request_method, str):
            request_method = parse_setting_pb.ParseSettingMessage.RequestMethod.Value(request_method)
        parse_setting.request_method = request_method

    @ignore_none_param
    def __update_next_spider_rules(self, parse_setting, next_spider_rules):
        parse_setting.next_spider_rules = next_spider_rules
