"""Shared pytest setup — keeps tests runnable on Python 3.12+.

1. Default asyncio event loop: Pyrogram's sync wrapper calls
   ``asyncio.get_event_loop()`` at import time; Python 3.12+ no longer
   auto-creates a loop and raises RuntimeError, which breaks test collection
   on newer interpreters (e.g. 3.14) even though production runs on 3.11.
   On 3.11 this is a harmless no-op.

2. Removed AST node classes: Werkzeug 2.2.x (pinned by requirements.txt)
   uses the ``ast.Str`` / ``ast.Num`` / ``ast.Bytes`` / ``ast.NameConstant``
   node classes that were removed in Python 3.12. Re-add them as thin
   subclasses of ``ast.Constant`` (backed by the ``value`` field, with the
   legacy ``s`` / ``n`` attribute accessors) so the venv can validate the
   exact production dependency set on a newer interpreter.
"""
import ast
import asyncio

import pytest

# --- 1. default event loop -------------------------------------------------
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# --- 2. legacy ast node shims ---------------------------------------------
if not hasattr(ast, "Str"):

    class _Str(ast.Constant):
        def __init__(self, s=""):
            super().__init__(value=s)

        @property
        def s(self):
            return self.value

        @s.setter
        def s(self, value):
            self.value = value

    class _Num(ast.Constant):
        def __init__(self, n=0):
            super().__init__(value=n)

        @property
        def n(self):
            return self.value

        @n.setter
        def n(self, value):
            self.value = value

    class _Bytes(ast.Constant):
        def __init__(self, s=b""):
            super().__init__(value=s)

        @property
        def s(self):
            return self.value

        @s.setter
        def s(self, value):
            self.value = value

    class _NameConstant(ast.Constant):
        def __init__(self, value=None):
            super().__init__(value=value)

    for _name, _cls in (
        ("Str", _Str),
        ("Num", _Num),
        ("Bytes", _Bytes),
        ("NameConstant", _NameConstant),
    ):
        if not hasattr(ast, _name):
            setattr(ast, _name, _cls)


@pytest.fixture(autouse=True)
def _reset_app_runtime_flags():
    """Reset shared app runtime flags before every test.

    ``media_downloader.main()`` leaves ``app.is_running=False`` and
    ``app.force_exit=True`` in its finally block; without a reset, monitor /
    worker tests that run afterwards see the exit signal and skip their loop
    bodies. Worker/monitor tasks check these flags, so tests must start from a
    clean "running" state.
    """
    from core.context import app

    app.is_running = True
    app.force_exit = False
    yield

