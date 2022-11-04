# -*- coding: utf-8 -*-
import json
import time
from common_sdk.logging.logger import logger
from common_sdk.data_transform import protobuf_transformer
from dapr.ext.fastapi import DaprApp
from manager.cookie_manager import CookieManager
import proto.cookie_pb2 as cookie_pb


'''
    用于生成cookie的handel
'''


class CookieHandel:

    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.cookie_id = actor_id.id
        self.__manager = CookieManager()

    async def get_cookie(self):
        # 根据创建一个将要发送的消息
        # 查询所有的cookie
        cookie_list = await self.__manager.list_cookies()
        # 随机返回一个

    '''
        创建一条普通cookie
    '''

    async def generate_cookie(self, cookie_map):
        cookie = cookie_pb.CookieMessage()
        self.__manager.create_cookie(cookie, cookie_map)
        await self.__manager.add_or_update_cookie(cookie)
        return cookie
