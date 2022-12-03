# -*- coding: utf-8 -*-

from starlette.responses import JSONResponse

from service import error_codes


def jsonify_response(data=None, status_response=None):
    if data is None:
        data = {}
    if status_response is None:
        status_response = {
            "errcode": error_codes.SUCCESS[0],
            "errmsg": error_codes.SUCCESS[1]
        }
    else:
        status_response = {
            "errcode": status_response[0],
            "errmsg": status_response[1]
        }
    ret = {}
    if data:
        ret = {
            "data": data
        }
    ret.update(**status_response)
    return ret


def protobuf_response(obj=None):
    if obj is None:
        return "", 200
    ret = obj.SerializeToString()
    return ret, 200


def get_from_request(request, key, default=None):
    val = default
    if request.json:
        val = request.json.get(key)
    if request.args:
        val = request.args.get(key)
    return val
