# providers/__init__.py
from __future__ import annotations

from .base import BatchProvider
from .openai import OpenAIBatchProvider

_PROVIDERS = {"openai": OpenAIBatchProvider}


def get_provider(name: str = "openai") -> BatchProvider:
    try:
        return _PROVIDERS[name]()
    except KeyError:
        raise ValueError(
            f"Unknown provider {name!r}. Available: {', '.join(_PROVIDERS)}"
        ) from None


__all__ = ["BatchProvider", "OpenAIBatchProvider", "get_provider"]
