from collections.abc import Callable
from typing import Annotated

from nonebot.params import Depends
from nonebot.typing import T_State


class _FuncWrapper:
    def __init__(self, funcs: list[Callable]) -> None:
        self._funcs = funcs

    def execute(self):
        for func in self._funcs:
            func()


def get_increaser(state: T_State):
    ret = state.get("plugin_limiter:increaser")
    if ret is None:
        raise KeyError("Cannot get increaser, make sure you have enabled `set_increaser` in cooldown policy.")
    return _FuncWrapper(ret)


Increaser = Annotated[_FuncWrapper, Depends(get_increaser)]
