# providers/__init__.py
from __future__ import annotations

from .anthropic import AnthropicBatchProvider
from .base import BatchProvider
from .openai import OpenAIBatchProvider

_PROVIDERS = {
    "openai": OpenAIBatchProvider,
    "anthropic": AnthropicBatchProvider,
}


def get_provider(name: str = "openai") -> BatchProvider:
    try:
        return _PROVIDERS[name]()
    except KeyError:
        raise ValueError(
            f"Unknown provider {name!r}. Available: {', '.join(available_providers())}"
        ) from None


def available_providers() -> tuple[str, ...]:
    return tuple(sorted(_PROVIDERS))


__all__ = [
    "AnthropicBatchProvider",
    "BatchProvider",
    "OpenAIBatchProvider",
    "available_providers",
    "get_provider",
]
