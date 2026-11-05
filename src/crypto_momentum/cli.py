from __future__ import annotations

import argparse
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from .config import load_settings
from .runner import BotRunner


def _persist_cli_fatal(exc: BaseException) -> None:
    """Best-effort crash record that survives rotating bot.log and lost terminals."""
    try:
        project_root = Path(__file__).resolve().parents[2]
        path = project_root / "logs" / "runtime_fatal.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write("\n" + "=" * 100 + "\n")
            fh.write(f"utc={datetime.now(timezone.utc).isoformat()}\n")
            fh.write(f"pid={os.getpid()} context=cli_unhandled\n")
            fh.write(f"exception={type(exc).__name__}: {exc}\n")
            fh.write("traceback:\n")
            fh.write("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Coinbase Crypto Momentum V1.4.12")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("doctor", help="Test authentication, balances, products, and fee tier; never places orders")
    sub.add_parser("scan-once", help="Run one strategy scan")
    sub.add_parser("run", help="Run continuously; behavior depends on RUN_MODE in .env")
    sub.add_parser("status", help="Show local state, Coinbase balances, equity, and recent bot events")
    sub.add_parser("halt", help="Set manual halt flag; blocks new entries")
    sub.add_parser("resume", help="Clear manual halt flag")
    sub.add_parser("notify-test", help="Send one Pushover test notification; no order is placed")
    sub.add_parser(
        "recover-orphan",
        help="Recover one recent bot-created live holding after a lost local entry state; places protection but no new buy",
    )
    args = parser.parse_args()

    settings = load_settings()
    runner = BotRunner(settings)

    if args.command == "doctor":
        runner.doctor()
    elif args.command == "scan-once":
        runner.scan_once()
    elif args.command == "run":
        runner.run()
    elif args.command == "status":
        runner.status()
    elif args.command == "halt":
        runner.db.set_json("manual_halt", True)
        print("Manual halt set. New entries are blocked; existing exchange protection is unchanged.")
    elif args.command == "resume":
        runner.db.set_json("manual_halt", False)
        print("Manual halt cleared.")
    elif args.command == "notify-test":
        runner.test_notification()
    elif args.command == "recover-orphan":
        runner.recover_orphan()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        _persist_cli_fatal(exc)
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1)
