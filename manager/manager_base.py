# -*- coding: utf-8 -*-

from common_sdk.system.sys_env import get_env


def ignore_none_param(fn):
    """ 当参数里有None时,被装饰的函数将不会被调用
    仅用于更新实体字段时使用
    """

    def inner(*args, **kwargs):
        has_none_param = False
        for arg in args:
            if arg is None:
                has_none_param = True
                break
        if has_none_param:
            return
        return fn(*args, **kwargs)

    return inner


class ManagerBase:
    """所有业务逻辑处理相关的ManagerBase基类。"""

    @property
    def version(self):
        return get_env("VERSION", "v2.1")

    @property
    def cookie(self):
        return self._cookie

    @property
    def proxy(self):
        return self._proxy

    @property
    def url(self):
        return self._url

    @property
    def request(self):
        return self._request

    @property
    def response(self):
        return self._response

    def __init__(self, url=None):
        self._url = url
        self._response = None
        self._request = None
        self._proxy = None
        self._cookie = None
