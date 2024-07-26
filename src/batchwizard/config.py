# config.py
import os
from pathlib import Path
from typing import Optional

import typer
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BatchWizardSettings(BaseSettings):
    api_key: Optional[str] = Field(None, env="OPENAI_API_KEY")
    max_concurrent_jobs: int = 5
    check_interval: int = 5
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


class Config(BaseModel):
    settings: BatchWizardSettings = BatchWizardSettings()

    @property
    def config_dir(self) -> Path:
        return Path(typer.get_app_dir("BatchWizard"))

    @property
    def config_file(self) -> Path:
        return self.config_dir / "config.json"

    def load(self) -> None:
        if self.config_file.exists():
            self.settings = BatchWizardSettings.model_validate_json(
                self.config_file.read_text()
            )

    def save(self) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_file.write_text(self.settings.model_dump_json())

    def get_api_key(self) -> Optional[str]:
        return self.settings.api_key or os.getenv("OPENAI_API_KEY")

    def set_api_key(self, api_key: str) -> None:
        self.settings.api_key = api_key
        self.save()


config = Config()
config.load()
