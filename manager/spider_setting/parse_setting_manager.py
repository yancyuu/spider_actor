# -*- coding: utf-8 -*-

import time
from common_sdk.util.id_generator import generate_common_id
from dao.parse_setting_da_helper import ParseSettingDAHelper
from manager.manager_base import ManagerBase
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

    def update_parse_setting(self, parse_setting, status=None):
        self.__update_status(parse_setting, status)

    async def list_parse_settings(self, status=None):
        proxies = await self.da_helper.list_parse_settings(
            status=status
        )
        return proxies

    def delete_parse_setting(self, parse_setting):
        self.__update_status(parse_setting, parse_setting_pb.ParseSettingMessage.ParseStatus.DELETED)

    async def add_or_update_parse_setting(self, parse_setting):
        await self.da_helper.add_or_update_parse_setting(parse_setting)

    @staticmethod
    def __update_status(parse_setting, status):
        if status is None:
            return
        if isinstance(status, str):
            status = parse_setting_pb.ParseSettingMessage.ParseStatus.Value(status)
        if status == parse_setting_pb.ParseSettingMessage.ParseStatus.DELETED:
            parse_setting.delete_time = int(time.time())
        parse_setting.status = status






