# -*- coding: utf-8 -*-
from common_sdk.logging.logger import logger
from common_sdk.data_transform import protobuf_transformer
from manager.spider_setting.parse_setting_manager import ParseSettingManager
import proto.spider.parse_setting_pb2 as parse_setting_pb
from handel import errors, error_codes
from handel.base_responses import jsonify_response

'''
    用于爬取网页的handel
'''


class SpiderSettingHandel:

    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.spider_id = actor_id.id
        self.__parse_setting_manager = ParseSettingManager()

    '''
        创建一条解析规则
    '''

    async def generate_parse_setting(self, data: dict):
        parse_setting = protobuf_transformer.dict_to_protobuf(data, parse_setting_pb.ParseSettingMessage)
        self.__parse_setting_manager.create_parse_setting(parse_setting)
        await self.__parse_setting_manager.add_or_update_parse_setting(parse_setting)
        return protobuf_transformer.protobuf_to_dict(parse_setting)

    '''
            更新一条解析规则
    '''

    async def update_parse_setting(self, data: dict):
        logger.info("需要更新的数据{}".format(data))
        id = data.get("id")
        if not id:
            error = errors.Error(error_codes.MISSING_ID)
            return jsonify_response(status_response=(error.errcode, error.errmsg))
        parse_setting = await self.__parse_setting_manager.get_parse_setting(id)
        if not parse_setting:
            error = errors.Error(error_codes.ENTITY_NOT_EXISTS, action="更新解析规则")
            return jsonify_response(status_response=(error.errcode, error.errmsg))
        self.__parse_setting_manager.update_parse_setting(
            parse_setting=parse_setting,
            parse_type=data.get("parseType"),
            next_spider_rules=data.get("nextSpiderRules"),
            parse_rules=data.get("parseRules"),
            status=data.get("status"),
            request_method=data.get("requestMethod"),
            enable_next_spider_repeated=data.get("enableNextSpiderRepeated"))
        await self.__parse_setting_manager.add_or_update_parse_setting(parse_setting)
        data = protobuf_transformer.protobuf_to_dict(parse_setting)
        return jsonify_response(data=data)

    '''
           查询规则列表
    '''
    async def list_parse_settings(self, data: dict):
        ids = data.get("ids")
        if not ids:
            error = errors.Error(error_codes.MISSING_ID)
            return jsonify_response(status_response=(error.errcode, error.errmsg))
        parse_settings = await self.__parse_setting_manager.list_parse_settings(ids=ids)
        if not parse_settings:
            error = errors.Error(error_codes.ENTITY_NOT_EXISTS, action="获取解析规则")
            return jsonify_response(status_response=(error.errcode, error.errmsg))
        return jsonify_response(protobuf_transformer.batch_protobuf_to_dict(parse_settings))

    '''
        删除一条解析规则
    '''

    async def delete_parse_setting(self, data: dict):
        id = data.get("id")
        if not id:
            raise errors.Error(error_codes.MISSING_ID)
        parse_setting = await self.__parse_setting_manager.get_parse_setting(id)
        if not parse_setting:
            raise errors.Error(error_codes.ENTITY_NOT_EXISTS, action="删除解析规则")
        self.__parse_setting_manager.delete_parse_setting(parse_setting)
        await self.__parse_setting_manager.add_or_update_parse_setting(parse_setting)
        return protobuf_transformer.protobuf_to_dict(parse_setting)

