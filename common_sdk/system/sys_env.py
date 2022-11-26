# -*- coding: utf-8 -*-

import os


def get_env(key, default=None):
    value = os.getenv(key)
    if value is None:
        return default
    return value
