# -*- coding: utf-8 -*-
from common_sdk.data_transform.protobuf_transformer import protobuf_to_dict
from manager.cookie.cookie_manager import CookieManager
import proto.cookie.cookie_pb2 as cookie_pb


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
        for key in cookie_map:
            cookie.cookie_map[key] = cookie_map[key]
        self.__manager.create_cookie(cookie)
        await self.__manager.add_or_update_cookie(cookie)
        return protobuf_to_dict(cookie)
