# -*- coding: utf-8 -*-

# 多进程
import multiprocessing
"""gunicorn+gevent 的配置文件"""

# 预加载资源
# preload_app = True
# 绑定 ip + 端口
bind = "0.0.0.0:3000"
# 进程数 = cpu数量
# workers = multiprocessing.cpu_count()
# 测试先改为1
workers = 1

# 线程数 = cpu数量
# threads = multiprocessing.cpu_count()
# 测试先改为1
threads = 1

# 等待队列最大长度,超过这个长度的链接将被拒绝连接
backlog = 2048

# 工作模式--协程
worker_class = "gevent"

# 最大客户客户端并发数量,对使用线程和协程的worker的工作有影响
# 服务器配置设置的值  1200：中小型项目  上万并发： 中大型
# 服务器硬件：宽带+数据库+内存
# 服务器的架构：集群 主从
worker_connections = 2048