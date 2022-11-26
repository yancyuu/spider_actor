from dotenv import load_dotenv

load_dotenv(verbose=True)
from dapr.ext.fastapi import DaprActor
from dapr.conf import settings
from fastapi import FastAPI
from handel.proxy.proxy_actor import ProxyActor
from handel.cookie.cookie_actor import CookieActor
from handel.spider.spider_actor import SpiderActor
import json
import uvicorn
import ujson


class UJSONEncoder(json.JSONEncoder):

    def default(self, obj):
        try:
            return ujson.dumps(obj)
        except TypeError:
            return json.JSONEncoder.default(self, obj)

    def encode(self, obj):
        try:
            return ujson.dumps(obj)
        except TypeError:
            return json.dumps(obj, ensure_ascii=False)


json._default_encoder = UJSONEncoder(
    skipkeys=False,
    ensure_ascii=False,
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=None,
    default=None,
)

app = FastAPI(title=f'spider-actor-service')

app.debug = True
app.json_encoder = UJSONEncoder
settings.DAPR_RUNTIME_HOST = "192.168.1.10"
# Add Dapr Actor Extension
actor = DaprActor(app)


@app.on_event("startup")
async def startup_event():
    # Register Actor
    await actor.register_actor(ProxyActor)
    # 生成cookie的actor
    await actor.register_actor(CookieActor)
    # 生成基础爬虫数据结构的actor
    await actor.register_actor(SpiderActor)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3000)
