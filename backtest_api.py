from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal
import subprocess
import sys
import threading
import time

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
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
  output_dir: str | None = None
  market_summary_file: str | None = None


STRATEGY_REGISTRY: dict[str, StrategyRegistryItem] = {
  "v6_bspzs": StrategyRegistryItem(
    strategy_id="v6_bspzs",
    module="user_strategy_v6_bspzs.run_v6_bspzs",
    description="V6 buy sell zs 结构回测策略",
    enabled=True,
    allow_run_all=True,
    timeout_seconds=1800,
    output_dir="user_strategy_v6_bspzs/results",
    market_summary_file="market_all_summary_v6_bspzs.csv",
  ),
  "v7_bi": StrategyRegistryItem(
    strategy_id="v7_bi",
    module="user_strategy_v7_bi.run_v7_bi",
    description="V7 BI 结构回测策略",
    enabled=True,
    allow_run_all=True,
    timeout_seconds=1800,
    output_dir="user_strategy_v7_bi/results",
    market_summary_file="market_all_summary_v7_bi.csv",
  ),
  "v8_byma": StrategyRegistryItem(
    strategy_id="v8_byma",
    module="user_strategy_v8_byma.run_v8_byma",
    description="V8 blue yellow with ma策略",
    enabled=True,
    allow_run_all=True,
    timeout_seconds=1800,
    output_dir="user_strategy_v8_byma/results",
    market_summary_file="market_all_summary_v8_byma.csv",
  ),
  "v9_mr": StrategyRegistryItem(
    strategy_id="v9_mr",
    module="user_strategy_v9_mr.run_v9_mr",
    description="V9 MACD and RSI 策略",
    enabled=True,
    allow_run_all=True,
    timeout_seconds=1800,
    output_dir="user_strategy_v9_mr/results",
    market_summary_file="market_all_summary_v9_mr.csv",
  ),
}


class BacktestRunRequest(BaseModel):
  run_mode: RunMode = Field(
    default="single",
    description="single=单策略, batch=多策略, all=全部启用策略",
  )
  strategy_id: str | None = Field(
    default="v7_bi",
    description="单策略模式使用",
  )
  strategy_ids: list[str] = Field(
    default_factory=list,
    description="批量模式使用",
  )
  note: str | None = Field(default=None, description="备注")
  timeout_seconds: int | None = Field(
    default=None,
    description="覆盖策略默认超时秒数",
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
  market_summary_file: str | None = None
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
  output_dir: str | None = None
  market_summary_file: str | None = None


class StrategyListResponse(BaseModel):
  code: int
  message: str
  data: list[StrategyMeta]


class ResultFileItem(BaseModel):
  name: str
  path: str
  size_bytes: int
  modified_at: str


class BacktestResultsData(BaseModel):
  strategy_id: str
  output_dir: str
  exists: bool
  market_summary_file: str | None = None
  market_summary_exists: bool = False
  summary_files: list[str] = Field(default_factory=list)
  result_files: list[ResultFileItem] = Field(default_factory=list)


class BacktestResultsResponse(BaseModel):
  code: int
  message: str
  data: BacktestResultsData


class MarketSummaryRow(BaseModel):
  row: dict


class MarketSummaryData(BaseModel):
  strategy_id: str
  csv_file: str
  row_count: int
  columns: list[str]
  rows: list[dict]


class MarketSummaryResponse(BaseModel):
  code: int
  message: str
  data: MarketSummaryData


_RUN_LOCK = threading.Lock()
_IS_RUNNING = False


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


def _strategy_output_dir(item: StrategyRegistryItem) -> Path | None:
  if not item.output_dir:
    return None
  return BASE_DIR / item.output_dir


def _market_summary_path(item: StrategyRegistryItem) -> Path | None:
  out_dir = _strategy_output_dir(item)
  if out_dir is None or not item.market_summary_file:
    return None
  return out_dir / item.market_summary_file


def _find_summary_files(output_dir: Path | None) -> list[str]:
  if output_dir is None or not output_dir.exists():
    return []
  return sorted(str(p) for p in output_dir.glob("*summary*.csv"))


def _list_result_files(output_dir: Path | None) -> list[ResultFileItem]:
  if output_dir is None or not output_dir.exists():
    return []

  files: list[ResultFileItem] = []
  for p in sorted(output_dir.glob("*.csv")):
    stat = p.stat()
    files.append(
      ResultFileItem(
        name=p.name,
        path=str(p),
        size_bytes=stat.st_size,
        modified_at=datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
      )
    )
  return files


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

  output_dir = _strategy_output_dir(item)
  market_summary_path = _market_summary_path(item)

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
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
      market_summary_file=str(market_summary_path)
      if market_summary_path and market_summary_path.exists()
      else None,
      error=None if success else f"strategy exited with code {proc.returncode}",
    )

  except subprocess.TimeoutExpired as e:
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
      stdout_tail=_tail_text(e.stdout or ""),
      stderr_tail=_tail_text(e.stderr or ""),
      output_dir=str(output_dir) if output_dir else None,
      summary_files=_find_summary_files(output_dir),
      market_summary_file=str(market_summary_path)
      if market_summary_path and market_summary_path.exists()
      else None,
      error=f"timeout after {timeout_seconds}s",
    )

  except Exception as e:
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
      market_summary_file=str(market_summary_path)
      if market_summary_path and market_summary_path.exists()
      else None,
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
      output_dir=item.output_dir,
      market_summary_file=item.market_summary_file,
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
      results.append(_run_one_strategy(item, override_timeout=req.timeout_seconds))

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


@router.get("/api/chan/backtest/results", response_model=BacktestResultsResponse)
def get_backtest_results(
  strategy_id: str = Query(..., description="策略ID，例如 v7_bi"),
):
  item = STRATEGY_REGISTRY.get(strategy_id)
  if item is None:
    raise HTTPException(status_code=404, detail=f"未知策略: {strategy_id}")

  output_dir = _strategy_output_dir(item)
  market_summary_path = _market_summary_path(item)

  if output_dir is None:
    raise HTTPException(status_code=500, detail=f"策略未配置 output_dir: {strategy_id}")

  data = BacktestResultsData(
    strategy_id=item.strategy_id,
    output_dir=str(output_dir),
    exists=output_dir.exists(),
    market_summary_file=str(market_summary_path) if market_summary_path else None,
    market_summary_exists=bool(market_summary_path and market_summary_path.exists()),
    summary_files=_find_summary_files(output_dir),
    result_files=_list_result_files(output_dir),
  )

  return BacktestResultsResponse(code=0, message="ok", data=data)


@router.get("/api/chan/backtest/summary/market", response_model=MarketSummaryResponse)
def get_market_summary(
  strategy_id: str = Query(..., description="策略ID，例如 v7_bi"),
  limit: int = Query(500, ge=1, le=5000, description="最多返回多少行"),
):
  item = STRATEGY_REGISTRY.get(strategy_id)
  if item is None:
    raise HTTPException(status_code=404, detail=f"未知策略: {strategy_id}")

  csv_path = _market_summary_path(item)
  if csv_path is None:
    raise HTTPException(
      status_code=500, detail=f"策略未配置 market_summary_file: {strategy_id}"
    )

  if not csv_path.exists():
    raise HTTPException(status_code=404, detail=f"市场汇总文件不存在: {csv_path}")

  try:
    df = pd.read_csv(csv_path)
    df = df.fillna("")
    if len(df) > limit:
      df = df.head(limit)

    rows = df.to_dict(orient="records")

    return MarketSummaryResponse(
      code=0,
      message="ok",
      data=MarketSummaryData(
        strategy_id=item.strategy_id,
        csv_file=str(csv_path),
        row_count=len(rows),
        columns=[str(c) for c in df.columns.tolist()],
        rows=rows,
      ),
    )
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
