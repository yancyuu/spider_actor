# Spiker



[![](https://img.shields.io/badge/python-3-brightgreen.svg)](https://www.python.org/downloads/)
<img src="https://img.shields.io/badge/license-GPL--3.0-brightgreen">

*仅限学习交流使用，禁止商用。未经授权禁止转载*


##### 本项目遵守GPL-3.0开源协议

##### 您可以使用本项目for yourself，如果您使用本项目获利（包括但不限于商用、程序代做以及其他私活），则不被允许；

##### 如果您未经允许使用本项目获利，本人保留因侵权连带的一切追责行为；同时造成的法律纠纷与本人无关。



### 求求大家给个star吧！！这个对我真的很重要！！

本程序可以爬取自定义平台搜索页，并将结果写入数据库中。支持多cookie、ip代理以及多数据源爬取。

目前支持的写入类型如下：
- **MongoDB数据库**


如果您需要其他数据库支持，联系我们或者您添加后提PR。

***

**将cookie池、ip代理等诸多手段和爬虫利用actor架构解耦，赋予了使用者很高的操作自由度。**

**使用到了dapr分布式运行时，因此需要启动边车服务才可以运行，并且可以部署在k8s中。**

**如果您是发现了bug或者有什么更好的提议，欢迎给我发邮件提issues、或PR，但是跟程序运行有关的所有问题请自行解决或查看[这里](https://github.com/yancyuu/spider_actor/issues)。**

**不接受小白提问，自行研究。文档比较完善，阅读完完整文档后如还有有疑问，提问时请展示你思考验证的过程。**

***

## 开发计划

### 已支持

- csnd的搜索页链接

- 自定义cookie池

- 自定义ip代理

### 计划支持

- 百度的搜索页链接

- Google的搜索页链接

- 作为一个通用框架，并不局限某一种业务场景

- cookie动态更新

- 支持proxy代理池

## 环境配置
语言：python3

系统：Windows/Linux/MacOS

运行依赖：docker, dapr

容器环境配置：

查看 requirements.txt 一键配置：

    pip install -r requirements.txt

## 使用方法：


### 配置配置文件
首先在根目录创建.env文件，参数意义如下,将mongo和redis相关修改为自己的配置。
vim .env
     
    # 业务相关的配置
    # 是否使用cookie池
    USE_COOKIE_POOL=true
    # 是否使用代理
    USE_PROXY_POOL=true
    # 是否使用随机agent
    USE_RANDOM_AGENT=true
    
    # 日志相关配置
    LOGGER_CATEGORY=INFO,DEBUG,ERROR
    LOGGER_ENABLE_CONSOLE=true
    LOGGER_ENABLE_SYSLOG=false
    LOGGER_SYSLOG_HOST=logger.server
    LOGGER_SYSLOG_PORT=514
    LOGGER_SYSLOG_FACILITY=local7
    LOGGER_ENABLE_FILE=false
    LOGGER_FILE_DIRECTORY=$HOME/logs/$APPNAME/$VERSION
    
    # MongoDB数据库配置
    MONGODB_ADDRESS=''
    MONGODB_PORT=
    MONGODB_USER_NAME=''
    MONGODB_ROOT_PASSWORD=''
    MONGODB_REPLICA_SET=''
    
    # Redis配置
    REDIS_HOST=''
    REDIS_PORT=
    REDIS_PASSWORD=''
    


### 运行程序

**为了保证安全，最好将环境变量在初始化项目的时候打入镜像。我这使用docker构建初始化镜像，步骤如下**
1. 编写初始化构建文件，将初始化环境变量和依赖项打入镜像：vim init

       FROM python:3.10-slim AS base
       RUN mkdir /app
       WORKDIR /app
       COPY requirements.txt .
       COPY .env .
       COPY config.py .
       RUN pip install --no-cache-dir --upgrade pip -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

2. 执行初始构建并生成镜像
  
       docker build -f init -t spider_actor:v1.0 .
3. 执行部署项目的构建文件（见项目中的dockerfile）

       docker build -t spider_actor_service:v1.0 .

### 配置dapr环境（使其支持分布式微服务架构，方便横向扩展）
首先安装[dapr](https://dapr.io/)并初始化dapr

    dapr init

其次运行边车服务作为服务网格 

    dapr run --app-id spider-actor --app-port 3000 --dapr-http-port 3500

然后运行docker容器（记得修改挂载地址）

    docker run -it --name spider -p 3000:3000 spider_actor_service:v1.0 
