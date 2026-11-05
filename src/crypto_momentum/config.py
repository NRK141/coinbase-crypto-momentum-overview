from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import os
import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    raw: dict[str, Any]
    run_mode: str
    api_key: str
    api_secret: str
    live_confirmation: str
    project_root: Path

    @property
    def strategy(self) -> dict[str, Any]:
        return self.raw["strategy"]

    @property
    def universe(self) -> dict[str, Any]:
        return self.raw["universe"]

    @property
    def signal(self) -> dict[str, Any]:
        return self.raw["signal"]

    @property
    def shadow_universe(self) -> dict[str, Any]:
        return self.raw.get("shadow_universe", self.raw["universe"])

    @property
    def regime_universe(self) -> dict[str, Any]:
        return self.raw["regime_universe"]

    @property
    def regime(self) -> dict[str, Any]:
        return self.raw["regime"]

    @property
    def execution(self) -> dict[str, Any]:
        return self.raw["execution"]

    @property
    def risk(self) -> dict[str, Any]:
        return self.raw["risk"]

    @property
    def rotation(self) -> dict[str, Any]:
        return self.raw["rotation"]

    @property
    def shadow_15m(self) -> dict[str, Any]:
        return self.raw.get("shadow_15m", {})

    @property
    def universe_research(self) -> dict[str, Any]:
        return self.raw.get("universe_research", {})

    @property
    def remote_monitor(self) -> dict[str, Any]:
        return self.raw.get("remote_monitor", {})

    @property
    def trade_ledger(self) -> dict[str, Any]:
        return self.raw.get("trade_ledger", {})

    @property
    def runtime(self) -> dict[str, Any]:
        return self.raw["runtime"]

    @property
    def is_live(self) -> bool:
        return self.run_mode == "live"

    @property
    def is_dry_run(self) -> bool:
        return self.run_mode == "dry_run"

    def require_live_confirmation(self) -> None:
        if self.is_live and self.live_confirmation != "I_UNDERSTAND_LIVE_ORDERS":
            raise RuntimeError(
                "RUN_MODE=live requires LIVE_CONFIRMATION=I_UNDERSTAND_LIVE_ORDERS. "
                "This guard prevents accidental real-money orders."
            )


def _normalize_secret(value: str) -> str:
    # .env files commonly store PEM newlines as literal \\n.
    return value.replace("\\n", "\n").strip()


def load_settings() -> Settings:
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    config_path = os.getenv("CONFIG_PATH", "config/strategy.yaml")
    path = Path(config_path)
    if not path.is_absolute():
        path = project_root / path

    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    run_mode = os.getenv("RUN_MODE", "observe").strip().lower()
    if run_mode not in {"observe", "dry_run", "live"}:
        raise ValueError("RUN_MODE must be one of: observe, dry_run, live")

    api_key = os.getenv("COINBASE_API_KEY", "").strip()
    api_secret = _normalize_secret(os.getenv("COINBASE_API_SECRET", ""))
    confirmation = os.getenv("LIVE_CONFIRMATION", "").strip()

    return Settings(
        raw=raw,
        run_mode=run_mode,
        api_key=api_key,
        api_secret=api_secret,
        live_confirmation=confirmation,
        project_root=project_root,
    )
