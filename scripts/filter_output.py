from __future__ import annotations

import argparse
import re
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple

DATE_FORMAT = "%Y-%m-%d"
DEFAULT_INPUT = Path("output") / "合并交易量价数据.xlsx"
DEFAULT_OUTPUT = Path("output") / "筛选交易量价数据.xlsx"
UNIT_NUMBER_ALIASES = {
    3: 1,  # 3号机组视为1号机组
    4: 2,  # 4号机组视为2号机组
}
_PANDAS = None  # Lazy loaded pandas module


def parse_date(value: str) -> date:
    """Parse YYYY-MM-DD strings for argparse."""
    try:
        return datetime.strptime(value, DATE_FORMAT).date()
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise argparse.ArgumentTypeError(
            f"日期 {value!r} 不符合 {DATE_FORMAT} 格式"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="根据日期、机组和状态过滤 output/ 合并后的交易数据。"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="需要处理的 Excel 文件路径 (默认: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="筛选后的结果文件 (默认: %(default)s)",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_date,
        help="筛选区间起始日期，格式 YYYY-MM-DD (示例: 2026-01-10)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_date,
        help="筛选区间结束日期，格式 YYYY-MM-DD",
    )
    parser.add_argument(
        "--units",
        nargs="+",
        help="需要保留的机组编号或名称，可以同时传多个值 (示例: --units 3号机组 4号机组)",
    )
    parser.add_argument(
        "--s1",
        help="Excel 宏中的 s1 机组编号，将自动加入筛选列表。",
    )
    parser.add_argument(
        "--s2",
        help="Excel 宏中的 s2 机组编号，将自动加入筛选列表。",
    )
    parser.add_argument(
        "--status",
        default="运行",
        help="期望的机组状态 (默认: %(default)s)",
    )
    parser.add_argument(
        "--date-column",
        default="日期",
        help="日期列列名 (默认: %(default)s)",
    )
    parser.add_argument(
        "--unit-column",
        default="机组名称",
        help="机组编号/名称所在列 (默认: %(default)s)",
    )
    parser.add_argument(
        "--status-column",
        default="机组状态",
        help="状态列列名 (默认: %(default)s)",
    )
    return parser


def _collect_unit_ids(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> Tuple[List[str], List[Optional[str]]]:
    raw_units: List[str] = []
    if args.units:
        raw_units.extend(args.units)
    for attr in ("s1", "s2"):
        value = getattr(args, attr)
        if value:
            raw_units.append(value)
    cleaned = [value.strip() for value in raw_units if value and value.strip()]
    if not cleaned:
        parser.error("必须至少提供一个机组编号，可使用 --units 或 --s1/--s2。")
    ordered: List[str] = []
    seen = set()
    for unit in cleaned:
        if unit not in seen:
            ordered.append(unit)
            seen.add(unit)

    unit_keys = [_build_unit_key(value) for value in ordered]
    return ordered, unit_keys


def _get_pandas():
    global _PANDAS
    if _PANDAS is None:
        try:
            import pandas as pd  # type: ignore
        except ImportError as exc:  # pragma: no cover - import guard
            raise SystemExit(
                "未安装 pandas/openpyxl，先运行 `pip install pandas openpyxl`."
            ) from exc
        _PANDAS = pd
    return _PANDAS


def _normalize_text(series):
    pd = _get_pandas()
    original = series.copy()
    normalized = original.astype(str).str.strip()
    return normalized.where(original.notna(), None)


_UNIT_PATTERN_STRICT = re.compile(r"(?P<num>\d+)\s*(?:号)?\s*机组")
_UNIT_PATTERN_GENERIC = re.compile(r"\d+")


def _canonical_unit_number(number: int) -> int:
    return UNIT_NUMBER_ALIASES.get(number, number)


def _extract_unit_number(value: str) -> Optional[int]:
    match = _UNIT_PATTERN_STRICT.search(value)
    if match:
        return int(match.group("num"))
    fallback = _UNIT_PATTERN_GENERIC.findall(value)
    if fallback:
        return int(fallback[-1])
    return None


def _build_unit_key(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    number = _extract_unit_number(text)
    if number is None:
        return text
    canonical_number = _canonical_unit_number(number)
    return f"UNIT-{canonical_number}"


def load_dataframe(path: Path, date_column: str):
    pd = _get_pandas()
    if not path.exists():
        raise SystemExit(f"找不到输入文件: {path}")
    try:
        df = pd.read_excel(path)
    except ImportError as exc:  # pragma: no cover - delegated to pandas
        raise SystemExit("读取 Excel 需要 openpyxl，请先安装该依赖。") from exc
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    if date_column not in df.columns:
        raise SystemExit(f"输入文件缺少日期列: {date_column}")
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce").dt.date
    if df[date_column].isna().all():
        raise SystemExit("日期列解析失败，请检查列名或日期格式。")
    return df


def filter_dataframe(df, *, date_column: str, unit_column: str, status_column: str,
                      unit_ids: List[str], unit_keys: List[Optional[str]],
                      start_date: date, end_date: date, status: str):
    pd = _get_pandas()
    missing = [col for col in (unit_column, status_column) if col not in df.columns]
    if missing:
        joined = ", ".join(missing)
        raise SystemExit(f"输入文件缺少以下列: {joined}")

    unit_series = _normalize_text(df[unit_column])
    status_series = _normalize_text(df[status_column])
    date_series = df[date_column]
    unit_key_series = unit_series.apply(_build_unit_key)

    status_value = status.strip()
    date_mask = (date_series >= start_date) & (date_series <= end_date)
    unit_key_set = {key for key in unit_keys if key}
    unit_mask = unit_key_series.isin(unit_key_set)
    if status_value:
        status_mask = status_series == status_value
    else:
        status_mask = pd.Series(True, index=df.index)

    mask = date_mask & unit_mask & status_mask
    filtered = df.loc[mask].copy()

    sort_columns = [date_column]
    if "时间" in filtered.columns:
        sort_columns.append("时间")
    filtered = filtered.sort_values(sort_columns).reset_index(drop=True)
    return filtered


def save_results(df, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False, sheet_name="筛选结果")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("开始日期不能晚于结束日期。")

    args.unit_ids, args.unit_keys = _collect_unit_ids(args, parser)
    args.status = args.status.strip()

    df = load_dataframe(args.input, args.date_column)
    filtered = filter_dataframe(
        df,
        date_column=args.date_column,
        unit_column=args.unit_column,
        status_column=args.status_column,
        unit_ids=args.unit_ids,
        unit_keys=args.unit_keys,
        start_date=args.start_date,
        end_date=args.end_date,
        status=args.status,
    )

    save_results(filtered, args.output)

    print("=" * 60)
    print("数据筛选完成")
    print(f"输入文件: {args.input}")
    print(f"总行数: {len(df)}")
    print(
        f"筛选条件: 日期 {args.start_date:%Y-%m-%d} 至 {args.end_date:%Y-%m-%d}; "
        f"机组 {', '.join(args.unit_ids)}; 状态 {args.status or '全部'}"
    )
    print(f"结果行数: {len(filtered)}")
    print(f"输出文件: {args.output}")
    if filtered.empty:
        print("未找到符合条件的数据，已输出空文件方便后续流程。")
    else:
        preview = filtered.head(10)
        print("示例数据(前10行):")
        print(preview.to_string(index=False))
    print("=" * 60)


if __name__ == "__main__":
    main()
