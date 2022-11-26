# -*- coding: utf-8 -*-
from common_sdk.data_transform.protobuf_transformer import protobuf_to_dict
import random
from manager.proxy.proxy_manager import ProxyManager
import proto.proxy.proxy_pb2 as proxy_pb

'''
    用于生成代理的handel
'''


class ProxyHandel:

    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.proxy_id = actor_id.id
        self.__load_time = None
        self.__manager = ProxyManager()
        # 初始化

    async def get_proxy(self):
        message_list = await self.__manager.list_proxies(proxy_pb.ProxyMessage.INIT)
        if message_list:
            return
        # 随机返回一个
        index = random.randint(0, len(message_list))
        return message_list[index]

    '''
        创建一条普通proxy
    '''

    async def generate_proxy(self, proxy_url):
        message = proxy_pb.ProxyMessage()
        self.__manager.create_proxy(message)
        message.url = proxy_url
        await self.__manager.add_or_update_proxy(message)
        return protobuf_to_dict(message)
