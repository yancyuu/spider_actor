# -*- coding: utf-8 -*-

import time
from common_sdk.util.id_generator import generate_common_id
from dao.spider_da_helper import SpiderDAHelper
from manager.manager_base import ManagerBase
import proto.spider.spider_pb2 as spider_pb


class SpiderManager(ManagerBase):
    def __init__(self):
        super().__init__()
        self._da_helper = None

    @property
    def da_helper(self):
        if not self._da_helper:
            self._da_helper = SpiderDAHelper()
        return self._da_helper

    @staticmethod
    def create_spider(spider, target_url=None):
        if target_url is None:
            return
        spider.id = generate_common_id()
        spider.target_url = target_url
        spider.status = spider_pb.SpiderMessage.spiderStatus.NONE
        spider.create_time = int(time.time())
        return spider

    async def get_spider(self, id=None):
        return await self.da_helper.get_spider(id=id)

    def update_spider(self, spider, status=None):
        self.__update_status(spider, status)

    async def list_spiders(self, status=None):
        proxies = await self.da_helper.list_spiders(
            status=status
        )
        return proxies

    async def add_or_update_spider(self, spider):
        await self.da_helper.add_or_update_spider(spider)

    @staticmethod
    def __update_status(spider, status):
        if status is None:
            return
        if isinstance(status, str):
            status = spider_pb.SpiderMessage.SpiderStatus.Value(status)
        if status == spider_pb.SpiderMessage.SpiderStatus.RETRY:
            spider.retry_time = int(time.time())
        if status == spider_pb.SpiderMessage.SpiderStatus.FINISH:
            spider.finish_time = int(time.time())
        spider.status = status






