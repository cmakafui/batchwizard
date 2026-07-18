# config.py
import os
import tempfile
from pathlib import Path

import typer
from pydantic import AliasChoices, BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BatchWizardSettings(BaseSettings):
    openai_api_key: str | None = Field(
        None,
        validation_alias=AliasChoices("openai_api_key", "api_key", "OPENAI_API_KEY"),
    )
    anthropic_api_key: str | None = Field(
        None,
        validation_alias=AliasChoices("anthropic_api_key", "ANTHROPIC_API_KEY"),
    )
    max_concurrent_jobs: int = Field(default=5, ge=1)
    check_interval: int = Field(default=5, ge=0)
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


class Config(BaseModel):
    settings: BatchWizardSettings = BatchWizardSettings()

    @property
    def config_dir(self) -> Path:
        return Path(typer.get_app_dir("BatchWizard"))

    @property
    def config_file(self) -> Path:
        return self.config_dir / "config.json"

    @property
    def db_file(self) -> Path:
        return self.config_dir / "jobs.db"

    def load(self) -> None:
        if self.config_file.exists():
            self.settings = BatchWizardSettings.model_validate_json(
                self.config_file.read_text()
            )

    def save(self) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self.config_dir, prefix=".config.", suffix=".json.tmp"
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w") as handle:
                handle.write(self.settings.model_dump_json())
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, self.config_file)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def get_api_key(self, provider: str = "openai") -> str | None:
        fields = {
            "openai": ("openai_api_key", "OPENAI_API_KEY"),
            "anthropic": ("anthropic_api_key", "ANTHROPIC_API_KEY"),
        }
        try:
            field, environment = fields[provider]
        except KeyError:
            raise ValueError(f"Unknown provider {provider!r}") from None
        return getattr(self.settings, field) or os.getenv(environment)

    def set_api_key(self, api_key: str, provider: str = "openai") -> None:
        fields = {
            "openai": "openai_api_key",
            "anthropic": "anthropic_api_key",
        }
        try:
            field = fields[provider]
        except KeyError:
            raise ValueError(f"Unknown provider {provider!r}") from None
        setattr(self.settings, field, api_key)
        self.save()


config = Config()
config.load()
