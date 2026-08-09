import os
from pathlib import Path
import tempfile

os.environ["ENVIRONMENT"] = "test"
_TEST_DATABASE = Path(tempfile.mkdtemp(prefix="nonebot-plugin-limiter-tests-")) / "db.sqlite3"
os.environ["SQLALCHEMY_DATABASE_URL"] = f"sqlite+aiosqlite:///{_TEST_DATABASE.as_posix()}"
os.environ["ALEMBIC_STARTUP_CHECK"] = "false"

import nonebot
from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter
import pytest


@pytest.fixture(scope="session", autouse=True)
def after_nonebot_init(after_nonebot_init: None) -> None:
    driver = nonebot.get_driver()
    driver.register_adapter(OneBotV11Adapter)
    assert nonebot.load_plugin("nonebot_plugin_limiter") is not None
