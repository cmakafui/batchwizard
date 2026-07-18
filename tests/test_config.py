from __future__ import annotations

import stat

import typer

from batchwizard.config import BatchWizardSettings, Config


def test_provider_keys_load_from_environment(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "openai-env")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "anthropic-env")

    settings = BatchWizardSettings(_env_file=None)

    assert settings.openai_api_key == "openai-env"
    assert settings.anthropic_api_key == "anthropic-env"


def test_legacy_api_key_migrates_to_openai():
    settings = BatchWizardSettings.model_validate({"api_key": "legacy-openai"})

    assert settings.openai_api_key == "legacy-openai"
    assert settings.anthropic_api_key is None


def test_key_dispatch_and_atomic_private_config_file(tmp_path, monkeypatch):
    monkeypatch.setattr(typer, "get_app_dir", lambda name: str(tmp_path))
    config = Config(settings=BatchWizardSettings(_env_file=None))

    config.set_api_key("openai-file", "openai")
    config.set_api_key("anthropic-file", "anthropic")

    assert config.get_api_key("openai") == "openai-file"
    assert config.get_api_key("anthropic") == "anthropic-file"
    loaded = BatchWizardSettings.model_validate_json(config.config_file.read_text())
    assert loaded.openai_api_key == "openai-file"
    assert loaded.anthropic_api_key == "anthropic-file"
    assert stat.S_IMODE(config.config_file.stat().st_mode) == 0o600
    assert list(tmp_path.glob(".config.*.tmp")) == []
