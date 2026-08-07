import os

import nonebot
from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter
import pytest

os.environ["ENVIRONMENT"] = "test"


@pytest.fixture(scope="session", autouse=True)
def after_nonebot_init(after_nonebot_init: None) -> None:
    driver = nonebot.get_driver()
    driver.register_adapter(OneBotV11Adapter)
    assert nonebot.load_plugin("nonebot_plugin_limiter") is not None
