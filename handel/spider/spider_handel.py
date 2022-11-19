# -*- coding: utf-8 -*-
from common_sdk.logging.logger import logger
from common_sdk.data_transform.protobuf_transformer import protobuf_to_dict
from client.spider_client import SpiderClient
from builder.spider_builder import SpiderBuilder
from manager.spider.spider_manager import SpiderManager
import spider_common.proto.spider.spider_pb2 as spider_pb

'''
    用于爬取网页的handel
'''


class SpiderHandel:

    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.spider_id = actor_id.id
        self.__load_time = None
        self.__manager = SpiderManager()

    async def list_spiders(self):
        """查询所有的爬虫"""
        return await self.__manager.list_spiders()

    '''
        开始爬取（主要逻辑）
    '''

    async def start_crawling(self, id):
        # 查找状态为None的url
        builder = SpiderBuilder()
        spider_obj = await self.__manager.get_spider(id)
        builder.url = spider_obj.target_url
        logger.info("开始爬取 {}".format(protobuf_to_dict(spider_obj)))
        # 搜索查询页
        client = SpiderClient(builder)
        response = client.get_search()
        # 修改数据库状态和返回值
        if response:
            # 根据解析配置开始解析
            parse_setting_id = spider_obj.parse_setting_id

        else:
            spider_obj.status = spider_pb.SpiderMessage.SpiderStatus.RETRY
        await self.__manager.add_or_update_spider(spider_obj)

    '''
        创建一条普通spider
    '''

    async def generate_spider(self, spider_url):
        message = spider_pb.SpiderMessage()
        self.__manager.create_spider(message)
        message.url = spider_url
        await self.__manager.add_or_update_spider(message)
        return message

    '''
        解析内容（从当前的html中解析a标签的href链接）
    '''

    async def parse_spider(self, response):
        pass


