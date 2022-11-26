# -*- coding: utf-8 -*-

import logging
from logging.handlers import SysLogHandler

from common_sdk.base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from ..system import sys_env
from ..util import file_utils, context

APPNAME_ENV_NAME = 'APPNAME'
# logger
LOGGER_CATEGORY_ENV_NAME = "LOGGER_CATEGORY"
# console
LOGGER_ENABLE_CONSOLE_ENV_NAME = "LOGGER_ENABLE_CONSOLE"
# syslog
LOGGER_ENABLE_SYSLOG_ENV_NAME = "LOGGER_ENABLE_SYSLOG"
LOGGER_SYSLOG_HOST_ENV_NAME = "LOGGER_SYSLOG_HOST"
LOGGER_SYSLOG_PORT_ENV_NAME = "LOGGER_SYSLOG_PORT"
LOGGER_SYSLOG_FACILITY_ENV_NAME = "LOGGER_SYSLOG_FACILITY"
# FILE
LOGGER_ENABLE_FILE_ENV_NAME = "LOGGER_ENABLE_FILE"
LOGGER_FILE_DIRECTORY_ENV_NAME = "LOGGER_FILE_DIRECTORY"


class Logger(metaclass=SingletonMetaclass):

    def __init__(self):
        self._formatter = None
        self._logger = logging.getLogger(self.name)
        self._logger.setLevel(logging.INFO)
        self.__init_syslog_handler()
        self.__init_console_handler()
        self.__init_file_handler()
        self.logger.info(f"######## {self.name}日志类初始化#######")

    @property
    def name(self):
        name = sys_env.get_env(APPNAME_ENV_NAME, default="未知应用名称")
        return name

    @property
    def formatter(self):
        if self._formatter is not None:
            return self._formatter
        formatter = f'{self.name}: %(asctime)s - %(name)s - %(lineno)s - %(levelname)s - %(message)s'
        self._formatter = logging.Formatter(formatter)
        return self._formatter

    @property
    def message_uuid(self):
        try:
            message_uuid = context.get_message_uuid()
            return message_uuid
        except:
            return None

    @property
    def logger(self):
        return self._logger

    def debug(self, message):
        message = self.__wrap_message_with_uuid(message)
        self.logger.debug(message)

    def info(self, message):
        message = self.__wrap_message_with_uuid(message)
        self.logger.info(message)

    def exception(self, message):
        message = self.__wrap_message_with_uuid(message)
        self.logger.exception(message)

    def error(self, message):
        message = self.__wrap_message_with_uuid(message)
        self.logger.error(message)

    def warning(self, message):
        message = self.__wrap_message_with_uuid(message)
        self.logger.warning(message)

    def fatal(self, message):
        message = self.__wrap_message_with_uuid(message)
        self.logger.fatal(message)

    def __wrap_message_with_uuid(self, message):
        if self.message_uuid:
            message = f"{self.message_uuid} - {message}"
        return message

    def __init_syslog_handler(self):
        """ 设置syslog日志
        """
        enable = sys_env.get_env(LOGGER_ENABLE_SYSLOG_ENV_NAME)
        if enable == "false":
            return
        host = sys_env.get_env(LOGGER_SYSLOG_HOST_ENV_NAME)
        port = int(sys_env.get_env(LOGGER_SYSLOG_PORT_ENV_NAME))
        facility = sys_env.get_env(LOGGER_SYSLOG_FACILITY_ENV_NAME)
        category = sys_env.get_env(LOGGER_CATEGORY_ENV_NAME)
        if not category:
            self.__create_syslog_handler(host, port, facility, logging.INFO)
        else:
            cs = category.split(",")
            for c in cs:
                level = self.__name_to_level(c)
                self.__create_syslog_handler(host, port, facility, level)

    def __name_to_level(self, name):
        return logging._nameToLevel[name]

    def __create_syslog_handler(self, host, port, facility, level):
        handler = SysLogHandler(address=(host, port), facility=SysLogHandler.facility_names[facility])
        handler.setFormatter(self.formatter)
        handler.setLevel(level)
        self._logger.addHandler(handler)

    def __init_console_handler(self):
        """ 设置终端日志
        """
        enable = sys_env.get_env(LOGGER_ENABLE_CONSOLE_ENV_NAME)
        if enable == "false":
            return
        handler = logging.StreamHandler()
        handler.setFormatter(self.formatter)
        self._logger.addHandler(handler)

    def __init_file_handler(self):
        """ 设置文件日志
        """
        enable = sys_env.get_env(LOGGER_ENABLE_FILE_ENV_NAME)
        if enable == "false":
            return
        directory = sys_env.get_env(LOGGER_FILE_DIRECTORY_ENV_NAME)
        file_utils.create_dir_if_not_exists(directory)
        category = sys_env.get_env(LOGGER_CATEGORY_ENV_NAME)
        cs = category.split(",")
        for c in cs:
            level = self.__name_to_level(c)
            filepath = file_utils.join_path_filename(directory, c.lower())
            self.__create_file_handler(filepath, level)

    def __create_file_handler(self, filepath, level):
        handler = logging.FileHandler(filepath)
        handler.setLevel(level)
        handler.setFormatter(self.formatter)
        self._logger.addHandler(handler)


# 初始化单例日志实例
logger = Logger()