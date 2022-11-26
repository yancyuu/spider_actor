# -*- coding: utf-8 -*-

from service import error_codes


class Error(Exception):

    def __init__(self, err=None, **kargs):
        if err is not None:
            self.errcode = err[0]
            self.errmsg = err[1]
            if kargs:
                self.errmsg = self.errmsg.format(**kargs)
            self.err = err
        else:
            self.err = error_codes.SERVER_ERROR
            self.errcode = error_codes.SERVER_ERROR[0]
            self.errmsg = error_codes.SERVER_ERROR[1]
        super(Error, self).__init__(self.errmsg, None)

    def __str__(self):
        return "错误码: {}, 错误内容: {}".format(self.errcode, self.errmsg)


class CustomMessageError(Error):
    def __init__(self):
        super().__init__(err=error_codes.CUSTOM_MESSAGE_ERROR)


class PageNotFound(Error):
    def __init__(self):
        super().__init__(err=error_codes.PAGE_NOT_FOUND)
