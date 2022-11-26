# -*- coding: utf-8 -*-

import time
from common_sdk.util.id_generator import generate_common_id
from dao.proxy_da_helper import ProxyDAHelper
from manager.manager_base import ManagerBase
import proto.proxy.proxy_pb2 as proxy_pb


class ProxyManager(ManagerBase):
    def __init__(self):
        super().__init__()
        self._da_helper = None

    @property
    def da_helper(self):
        if not self._da_helper:
            self._da_helper = ProxyDAHelper()
        return self._da_helper

    @staticmethod
    def create_proxy(proxy, ip=None):
        if ip is None:
            return
        proxy.id = generate_common_id()
        proxy.ip = ip
        proxy.status = proxy_pb.ProxyMessage.ProxyStatus.INIT
        proxy.create_time = int(time.time())
        return proxy

    async def get_proxy(self, id=None):
        return await self.da_helper.get_proxy(id=id)

    def update_proxy(self, proxy, status=None):
        self.__update_status(proxy, status)

    async def list_proxies(self, status=None):
        proxies = await self.da_helper.list_proxies(
            status=status
        )
        return proxies

    async def add_or_update_proxy(self, proxy):
        await self.da_helper.add_or_update_proxy(proxy)

    @staticmethod
    def __update_status(proxy, status):
        if status is None:
            return
        if isinstance(status, str):
            status = proxy_pb.ProxyMessage.proxyStatus.Value(status)
        if status == proxy_pb.ProxyMessage.proxyStatus.USED:
            proxy.use_time = int(time.time())
        proxy.status = status






