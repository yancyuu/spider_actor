# -*- coding: utf-8 -*-

import stringcase
from flask import request


class BaseBuilder:

    __param_name = "__param_name"

    @property
    def params(self):
        return self.__dict__[self.__param_name]

    @property
    def headers(self):
        return self._headers

    def __init__(self, params=None, headers=None):
        self.__dict__[self._BaseBuilder__param_name] = dict()
        if headers is None:
            self._headers = request.headers
        else:
            self._headers = headers

    def __getattr__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]

    def __setattr__(self, key, value):
        if key == "_headers":
            self.__dict__[key] = value
        else:
            key = stringcase.camelcase(key)
            self.__dict__[self._BaseBuilder__param_name].update({key: value})

    def reset_params(self):
        self.__dict__[self._BaseBuilder__param_name] = dict()

    def set_params(self, params):
        self.__dict__[self._BaseBuilder__param_name].update(params)
