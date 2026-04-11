from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal
import json
import subprocess
import sys
import threading
import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent

RunMode = Literal["single", "batch", "all"]


class StrategyRegistryItem(BaseModel):
  strategy_id: str
  module: str
  description: str
  enabled: bool = True
  allow_run_all: bool = True
  timeout_seconds: int = 1800


STRATEGY_REGISTRY: dict[str, StrategyRegistryItem] = {
  "v7_bi": StrategyRegistryItem(
    strategy_id="v7_bi",
    module="user_strategy_v7_bi.run_v7_bi",
    description="V7 BI 结构回测策略",
    enabled=True,
    allow_run_all=True,
    timeout_seconds=1800,
  ),
  # 未来可继续扩展，例如：
  # "v8_macd": StrategyRegistryItem(
  #     strategy_id="v8_macd",
  #     module="user_strategy_v8_macd.run_v8_macd",
  #     description="V8 MACD 过滤策略",
  #     enabled=False,
  #     allow_run_all=True,
  #     timeout_seconds=1800,
  # ),
  # "v9_rsi": StrategyRegistryItem(
  #     strategy_id="v9_rsi",
  #     module="user_strategy_v9_rsi.run_v9_rsi",
  #     description="V9 RSI 过滤策略",
  #     enabled=False,
  #     allow_run_all=True,
  #     timeout_seconds=1800,
  # ),
}


class BacktestRunRequest(BaseModel):
  run_mode: RunMode = Field(
    default="single",
    description="single=单策略, batch=多策略, all=全部启用且允许批量运行的策略",
  )
  strategy_id: str | None = Field(
    default="v7_bi",
    description="单策略运行时使用，例如 v7_bi",
  )
  strategy_ids: list[str] = Field(
    default_factory=list,
    description="批量运行时使用，例如 ['v7_bi', 'v8_macd']",
  )
  note: str | None = Field(
    default=None,
    description="可选备注，方便前端或定时任务传入标记",
  )
  timeout_seconds: int | None = Field(
    default=None,
    description="可选，覆盖单次运行超时时间；不传则使用策略默认值",
  )


class StrategyRunResult(BaseModel):
  strategy_id: str
  module: str
  success: bool
  return_code: int
  command: list[str]
  started_at: str
  finished_at: str
  duration_seconds: float
  stdout_tail: str
  stderr_tail: str
  output_dir: str | None = None
  summary_files: list[str] = Field(default_factory=list)
  error: str | None = None


class BacktestRunData(BaseModel):
  run_mode: str
  requested_strategies: list[str]
  note: str | None = None
  results: list[StrategyRunResult]


class BacktestRunResponse(BaseModel):
  code: int
  message: str
  data: BacktestRunData


class StrategyMeta(BaseModel):
  strategy_id: str
  module: str
  description: str
  enabled: bool
  allow_run_all: bool
  timeout_seconds: int


class StrategyListResponse(BaseModel):
  code: int
  message: str
  data: list[StrategyMeta]


_RUN_LOCK = threading.Lock()
_IS_RUNNING = False


def _now_str() -> str:
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _tail_text(text: str, max_chars: int = 4000) -> str:
  if not text:
    return ""
  return text[-max_chars:]


def _resolve_strategy_ids(req: BacktestRunRequest) -> list[str]:
  if req.run_mode == "single":
    if not req.strategy_id:
      raise ValueError("run_mode=single 时必须提供 strategy_id")
    return [req.strategy_id]

  if req.run_mode == "batch":
    if not req.strategy_ids:
      raise ValueError("run_mode=batch 时必须提供 strategy_ids")
    return req.strategy_ids

  if req.run_mode == "all":
    return [
      item.strategy_id
      for item in STRATEGY_REGISTRY.values()
      if item.enabled and item.allow_run_all
    ]

  raise ValueError(f"unsupported run_mode: {req.run_mode}")


def _validate_strategy_ids(strategy_ids: list[str]) -> list[StrategyRegistryItem]:
  items: list[StrategyRegistryItem] = []
  for sid in strategy_ids:
    item = STRATEGY_REGISTRY.get(sid)
    if item is None:
      raise ValueError(f"未知策略: {sid}")
    if not item.enabled:
      raise ValueError(f"策略未启用: {sid}")
    items.append(item)
  return items


def _guess_output_dir_for_module(module_name: str) -> Path | None:
  parts = module_name.split(".")
  if len(parts) < 2:
    return None
  strategy_dir = BASE_DIR / parts[0]
  if not strategy_dir.exists():
    return None
  return strategy_dir / "results"


def _find_summary_files(output_dir: Path | None) -> list[str]:
  if output_dir is None or not output_dir.exists():
    return []
  return sorted(str(p) for p in output_dir.glob("*summary*.csv"))


def _run_one_strategy(
  item: StrategyRegistryItem,
  override_timeout: int | None = None,
) -> StrategyRunResult:
  python_exec = sys.executable
  command = [python_exec, "-m", item.module]
  timeout_seconds = override_timeout or item.timeout_seconds

  started_dt = datetime.now()
  started_at = started_dt.strftime("%Y-%m-%d %H:%M:%S")
  t0 = time.time()

  output_dir = _guess_output_dir_for_module(item.module)

  try:
    proc = subprocess.run(
      command,
      cwd=str(BASE_DIR),
      capture_output=True,
      text=True,
      timeout=timeout_seconds,
      encoding="utf-8",
      errors="replace",
    )
    finished_dt = datetime.now()
    finished_at = finished_dt.strftime("%Y-%m-%d %H:%M:%S")
    duration_seconds = round(time.time() - t0, 3)

    success = proc.returncode == 0

    return StrategyRunResult(
      strategy_id=item.strategy_id,
      module=item.module,
      success=success,
      return_code=proc.returncode,
      command=command,
      started_at=started_at,
      finished_at=finished_at,
      duration_seconds=duration_seconds,
      stdout_tail=_tail_text(proc.stdout),
      stderr_tail=_tail_text(proc.stderr),
      output_dir=str(output_dir) if output_dir else None,
      summary_files=_find_summary_files(output_dir),
      error=None if success else f"strategy exited with code {proc.returncode}",
    )

  except subprocess.TimeoutExpired as e:
    finished_dt = datetime.now()
    finished_at = finished_dt.strftime("%Y-%m-%d %H:%M:%S")
    duration_seconds = round(time.time() - t0, 3)

    return StrategyRunResult(
      strategy_id=item.strategy_id,
      module=item.module,
      success=False,
      return_code=-1,
      command=command,
      started_at=started_at,
      finished_at=finished_at,
      duration_seconds=duration_seconds,
      stdout_tail=_tail_text((e.stdout or "")),
      stderr_tail=_tail_text((e.stderr or "")),
      output_dir=str(output_dir) if output_dir else None,
      summary_files=_find_summary_files(output_dir),
      error=f"timeout after {timeout_seconds}s",
    )
  except Exception as e:
    finished_dt = datetime.now()
    finished_at = finished_dt.strftime("%Y-%m-%d %H:%M:%S")
    duration_seconds = round(time.time() - t0, 3)

    return StrategyRunResult(
      strategy_id=item.strategy_id,
      module=item.module,
      success=False,
      return_code=-2,
      command=command,
      started_at=started_at,
      finished_at=finished_at,
      duration_seconds=duration_seconds,
      stdout_tail="",
      stderr_tail="",
      output_dir=str(output_dir) if output_dir else None,
      summary_files=_find_summary_files(output_dir),
      error=str(e),
    )


@router.get("/api/chan/backtest/health")
def backtest_health():
  return {"code": 0, "message": "ok"}


@router.get("/api/chan/backtest/strategies", response_model=StrategyListResponse)
def list_backtest_strategies():
  data = [
    StrategyMeta(
      strategy_id=item.strategy_id,
      module=item.module,
      description=item.description,
      enabled=item.enabled,
      allow_run_all=item.allow_run_all,
      timeout_seconds=item.timeout_seconds,
    )
    for item in STRATEGY_REGISTRY.values()
  ]
  return StrategyListResponse(code=0, message="ok", data=data)


@router.post("/api/chan/backtest", response_model=BacktestRunResponse)
def run_backtest(req: BacktestRunRequest):
  global _IS_RUNNING

  if _IS_RUNNING:
    raise HTTPException(status_code=409, detail="已有回测任务正在运行，请稍后再试")

  with _RUN_LOCK:
    if _IS_RUNNING:
      raise HTTPException(status_code=409, detail="已有回测任务正在运行，请稍后再试")
    _IS_RUNNING = True

  try:
    strategy_ids = _resolve_strategy_ids(req)
    strategy_items = _validate_strategy_ids(strategy_ids)

    results: list[StrategyRunResult] = []
    for item in strategy_items:
      result = _run_one_strategy(item, override_timeout=req.timeout_seconds)
      results.append(result)

    requested = [item.strategy_id for item in strategy_items]
    success_count = sum(1 for x in results if x.success)
    message = f"backtest finished: {success_count}/{len(results)} success"

    return BacktestRunResponse(
      code=0,
      message=message,
      data=BacktestRunData(
        run_mode=req.run_mode,
        requested_strategies=requested,
        note=req.note,
        results=results,
      ),
    )

  except ValueError as e:
    raise HTTPException(status_code=400, detail=str(e))
  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
  finally:
    with _RUN_LOCK:
      _IS_RUNNING = False
