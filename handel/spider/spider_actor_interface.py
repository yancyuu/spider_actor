from dapr.actor import ActorInterface, actormethod


class SpiderActorInterface(ActorInterface):
    @actormethod(name="listSpider")
    async def list_spider(self) -> object:
        ...

    @actormethod(name="generateSpider")
    async def generate_spider(self, data: dict) -> object:
        ...

    @actormethod(name="generateParseSettings")
    async def generate_parse_settings(self, data: dict) -> object:
        ...

    @actormethod(name="startCrawling")
    async def start_crawling(self) -> object:
        ...

    @actormethod(name="GetMyData")
    async def get_my_data(self) -> object:
        ...

    @actormethod(name="SetMyData")
    async def set_my_data(self, data: object) -> None:
        ...

    @actormethod(name="ClearMyData")
    async def clear_my_data(self) -> None:
        ...

    @actormethod(name="SetReminder")
    async def set_reminder(self, enabled: bool) -> None:
        ...

    @actormethod(name="SetTimer")
    async def set_timer(self, enabled: bool) -> None:
        ...

    @actormethod(name="GetReentrancyStatus")
    async def get_reentrancy_status(self) -> bool:
        ...