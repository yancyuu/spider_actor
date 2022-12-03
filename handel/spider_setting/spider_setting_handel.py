# -*- coding: utf-8 -*-
from common_sdk.logging.logger import logger
from common_sdk.data_transform import protobuf_transformer
from spider_sdk.client.spider_setting_client import SpiderSettingClient
from spider_sdk.builder.spider_setting_builder import SpiderSettingBuilder
from manager.spider_setting.parse_setting_manager import ParseSettingManager
import proto.spider.parse_setting_pb2 as parse_setting_pb
from handel import error_codes, errors

'''
    用于爬取网页的handel
'''


class SpiderSettingHandel:

    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.spider_id = actor_id.id
        self.__setting_builder = SpiderSettingBuilder()
        self.__parse_setting_manager = ParseSettingManager()

    '''
        开始爬取（主要逻辑）
    '''

    async def start_crawling(self, data):
        # 查找状态为None的url
        logger.info("开始爬取 {}".format(data))
        # 搜索查询页
        client = SpiderSettingClient(self.__setting_builder, data.get("id"))
        response = client.get_search()
        # 修改数据库状态和返回值
        if response:
            # 根据解析配置开始解析
            parse_settings = protobuf_transformer.dict_to_protobuf(data.get("parseSettings"), parse_setting_pb.ParseSettingMessage)

    '''
        创建一条解析规则
    '''
    async def generate_parse_setting(self, data: dict):
        parse_setting = protobuf_transformer.dict_to_protobuf(data, parse_setting_pb.ParseSettingMessage)
        self.__parse_setting_manager.create_parse_setting(parse_setting)
        await self.__parse_setting_manager.add_or_update_parse_setting(parse_setting)
        return protobuf_transformer.protobuf_to_dict(parse_setting)

    '''
            更新一条解析规则
    '''
    async def update_parse_setting(self, data: dict):
        id = data.get("id")
        if not id:
            raise errors.Error(error_codes.MISSING_ID)
        parse_setting = await self.__parse_setting_manager.get_parse_setting(id)
        if not parse_setting:
            raise errors.Error(error_codes.ENTITY_NOT_EXISTS, action="更新解析规则")
        self.__parse_setting_manager.update_parse_setting(
            parse_setting=parse_setting,
            parse_rules=data.get("parseRules"),
            next_spider_rules=data.get("nextSpiderRules"),
            status=data.get("status"))
        await self.__parse_setting_manager.add_or_update_parse_setting(parse_setting)
        return protobuf_transformer.protobuf_to_dict(parse_setting)

    '''
        删除一条解析规则
    '''
    async def delete_parse_setting(self, data: dict):
        id = data.get("id")
        if not id:
            raise errors.Error(error_codes.MISSING_ID)
        parse_setting = await self.__parse_setting_manager.get_parse_setting(id)
        if not parse_setting:
            raise errors.Error(error_codes.ENTITY_NOT_EXISTS, action="删除解析规则")
        self.__parse_setting_manager.delete_parse_setting(parse_setting)
        await self.__parse_setting_manager.add_or_update_parse_setting(parse_setting)
        return protobuf_transformer.protobuf_to_dict(parse_setting)

    '''
        解析内容（从当前的html中解析a标签的href链接）
    '''

    async def parse_spider(self, response):
        pass


