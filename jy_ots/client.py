# -*- coding: utf-8 -*-
# !/usr/bin/python
# Create Date 2016/3/14 0014
__author__ = 'wubo'
import urlparse,logging,time
from jy_ots.connection import ConnectionPool
from jy_ots.protocol import OTSProtocol
from jy_ots.error import *
from jy_ots.retry import DefaultRetryPolicy
class OTSClient(object):
    """
    ``OTSClient``实现了OTS服务的所有接口。用户可以通过创建``OTSClient``的实例，并调用它的
    方法来访问OTS服务的所有功能。用户可以在初始化方法``__init__()``中设置各种权限、连接等参数。

    除非另外说明，``OTSClient``的所有接口都以抛异常的方式处理错误(请参考模块``ots.error``
    )，即如果某个函数有返回值，则会在描述中说明；否则返回None。
    """


    DEFAULT_ENCODING = 'utf8'
    DEFAULT_SOCKET_TIMEOUT = 50
    DEFAULT_MAX_CONNECTION = 50
    DEFAULT_LOGGER_NAME = 'jy_ots-client'

    connection_pool_class = ConnectionPool
    protocol_class = OTSProtocol

    def __init__(self, end_point, accessid, accesskey, instance_name, **kwargs):
        """
        初始化``OTSClient``实例。

        ``end_point``是OTS服务的地址（例如 'http://instance.cn-hangzhou.ots.aliyun.com:80'），必须以'http://'开头。

        ``accessid``是访问OTS服务的accessid，通过官方网站申请或通过管理员获取。

        ``accesskey``是访问OTS服务的accesskey，通过官方网站申请或通过管理员获取。

        ``instance_name``是要访问的实例名，通过官方网站控制台创建或通过管理员获取。

        ``encoding``请求参数的字符串编码类型，默认是utf8。

        ``socket_timeout``是连接池中每个连接的Socket超时，单位为秒，可以为int或float。默认值为50。

        ``max_connection``是连接池的最大连接数。默认为50，

        ``logger_name``用来在请求中打DEBUG日志，或者在出错时打ERROR日志。

        ``retry_policy``定义了重试策略，默认的重试策略为 DefaultRetryPolicy。你可以继承 RetryPolicy 来实现自己的重试策略，请参考 DefaultRetryPolicy 的代码。


        示例：创建一个OTSClient实例

            from ots2.client import OTSClient

            ots_client = OTSClient('your_instance_endpoint', 'your_user_id', 'your_user_key', 'your_instance_name')
        """
        self.encoding = kwargs.get('encoding')
        if self.encoding is None:
            self.encoding = OTSClient.DEFAULT_ENCODING

        self.socket_timeout = kwargs.get('socket_timeout')
        if self.socket_timeout is None:
            self.socket_timeout = OTSClient.DEFAULT_SOCKET_TIMEOUT

        self.max_connection = kwargs.get('max_connection')
        if self.max_connection is None:
            self.max_connection = OTSClient.DEFAULT_MAX_CONNECTION

        # initialize logger
        logger_name = kwargs.get('logger_name')
        if logger_name is None:
            self.logger = logging.getLogger(OTSClient.DEFAULT_LOGGER_NAME)
            nullHandler = logging.NullHandler()
            self.logger.addHandler(nullHandler)
        else:
            self.logger = logging.getLogger(logger_name)

        # parse end point
        scheme, netloc, path = urlparse.urlparse(end_point)[:3]
        host = scheme + "://" + netloc

        if scheme != 'http' and scheme != 'https':
            raise OTSClientError(
                "protocol of end_point must be 'http' or 'https', e.g. http://ots.aliyuncs.com:80."
            )
        if host == '':
            raise OTSClientError(
                "host of end_point should be specified, e.g. http://ots.aliyuncs.com:80."
            )

        # intialize protocol instance via user configuration
        self.protocol = self.protocol_class(
            accessid, accesskey, instance_name, self.encoding, self.logger
        )

        # initialize connection via user configuration
        self.connection = self.connection_pool_class(
            host, path, timeout=self.socket_timeout, maxsize=self.max_connection,
        )

        # initialize the retry policy
        retry_policy = kwargs.get('retry_policy')
        if retry_policy is None:
            retry_policy = DefaultRetryPolicy()
        self.retry_policy = retry_policy

    def batch_write_row(self, body):
        query, reqheaders = self.protocol.make_request('BatchWriteRow', body)
        retry_times = 0
        reqbody = body
        while True:
            try:
                status, reason, resheaders, resbody = self.connection.send_receive(
                    query, reqheaders, reqbody
                )
                self.protocol.handle_error('BatchWriteRow', query, status, reason, resheaders, resbody)
                return query,status

            except OTSServiceError as e:

                if self.retry_policy.should_retry(retry_times, e, 'BatchWriteRow'):
                    retry_delay = self.retry_policy.get_retry_delay(retry_times, e, 'BatchWriteRow')
                    time.sleep(retry_delay)
                    retry_times += 1
                else:
                    raise e

    def get_range(self, table_name, direction,
                  inclusive_start_primary_key,
                  exclusive_end_primary_key,
                  columns_to_get=None, limit=None):
        api_name = 'GetRange'
        self.protocol_class.encoder.encoder_request()
        proto = self.encoder.encode_request(api_name, *args, **kwargs)
        body = proto.SerializeToString()

        query = '/' + api_name
        query, reqheaders, reqbody = self.protocol.make_request(
            api_name, *args, **kwargs
        )

        retry_times = 0

        while True:

            try:
                status, reason, resheaders, resbody = self.connection.send_receive(
                    query, reqheaders, reqbody
                )
                self.protocol.handle_error(api_name, query, status, reason, resheaders, resbody)
                break

            except OTSServiceError as e:

                if self.retry_policy.should_retry(retry_times, e, api_name):
                    retry_delay = self.retry_policy.get_retry_delay(retry_times, e, api_name)
                    time.sleep(retry_delay)
                    retry_times += 1
                else:
                    raise e

        ret = self.protocol.parse_response(api_name, status, resheaders, resbody)

        return ret
        (consumed, next_start_primary_key, row_list) = self._request_helper(
                    'GetRange', table_name, direction,
                    inclusive_start_primary_key, exclusive_end_primary_key,
                    columns_to_get, limit
        )
        return consumed, next_start_primary_key, row_list

if __name__ == "__main__":
    pass
    

