#!/usr/bin/env python3
"""Reproduce review.ipynb calculations and export results to Excel."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

HOURS_PER_RECORD = 4  # 96 点制到 24 小时
COEFFICIENT = 660
SUMMARY_KEY = "output"
SOURCE_KEY = "交易量价数据信息"
INFO_KEY = "基础信息"
STATUS_COLUMN = "机组状态"
RUNNING_STATUS = "运行"
DEEP_START_DATE = "2026-03-16"
DEEP_END_DATE = "2026-03-16"
HIGH_START_DATE = "2026-03-16"
HIGH_END_DATE = "2026-03-16"
DEEP_RESULT_COLUMNS = [
    "单位",
    "日前低价时长（小时）",
    "现货价格",
    "深调平均负荷",
    "中长期平均持仓",
    "深调套利（元）",
]
HIGH_RESULT_COLUMNS = [
    "单位",
    "日前高价时长",
    "实时高价时长",
    "日前现货高价均价",
    "实时现货高价均价",
    "中长期平均持仓",
    "高价日前平均中标负荷",
    "高价实时平均负荷",
]


def format_pct(value: float) -> str:
    return "" if pd.isna(value) else f"{value:.2%}"


def analyze_spot_prices(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(f"找不到现货出清电价文件: {path}")

    df = pd.read_excel(path)
    df = df[df["序号"] != "均价"].copy()
    df["日期"] = pd.to_datetime(df["日期"])
    data_date = df["日期"].dropna().iloc[0]

    day_ahead_avg = df["日前出清价格(元/MWh)"].mean()
    real_time_avg = df["实时出清价格(元/MWh)"].mean()

    day_ahead_low_count = ((df["日前出清价格(元/MWh)"] >= 0) &
                           (df["日前出清价格(元/MWh)"] <= 200)).sum()
    real_time_low_count = ((df["实时出清价格(元/MWh)"] >= 0) &
                           (df["实时出清价格(元/MWh)"] <= 200)).sum()

    day_ahead_high_count = ((df["日前出清价格(元/MWh)"] >= 566) &
                            (df["日前出清价格(元/MWh)"] <= 1500)).sum()
    real_time_high_count = ((df["实时出清价格(元/MWh)"] >= 566) &
                            (df["实时出清价格(元/MWh)"] <= 1500)).sum()

    detail_df = pd.DataFrame([
        {
            "指标": "日前",
            "均价": day_ahead_avg,
            "0-200区间点数": day_ahead_low_count,
            "0-200区间小时": day_ahead_low_count * 15 / 60,
            "300-1500区间点数": day_ahead_high_count,
            "300-1500区间小时": day_ahead_high_count * 15 / 60,
        },
        {
            "指标": "实时",
            "均价": real_time_avg,
            "0-200区间点数": real_time_low_count,
            "0-200区间小时": real_time_low_count * 15 / 60,
            "300-1500区间点数": real_time_high_count,
            "300-1500区间小时": real_time_high_count * 15 / 60,
        },
    ])

    summary_text = (
        f"{data_date.strftime('%m月%d日').lstrip('0').replace('月0', '月')}"
        f"现货日前均价{day_ahead_avg:.1f}元/兆瓦时，实时均价{real_time_avg:.2f}元/兆瓦时。"
        f"现货日前0价约{detail_df.loc[0, '0-200区间小时']:.2f}小时，实时0价约{detail_df.loc[1, '0-200区间小时']:.2f}小时；"
        f"现货日前高价约{detail_df.loc[0, '300-1500区间小时']:.2f}小时，实时高价约{detail_df.loc[1, '300-1500区间小时']:.2f}小时。"
        "火电机组核心收益点在于："
    )
    summary_df = pd.DataFrame([{"摘要": summary_text}])
    return summary_df, detail_df


def load_output_workbook(path: Path) -> Dict[str, pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(f"找不到合并后的输出文件: {path}")
    data = pd.read_excel(path, sheet_name=None)
    if SUMMARY_KEY not in data:
        if SOURCE_KEY not in data:
            raise KeyError("在工作簿中找不到 `交易量价数据信息` 表，无法创建合并数据。")
        data[SUMMARY_KEY] = data[SOURCE_KEY].copy()
    return data


def ensure_columns(summary_df: pd.DataFrame, info_df: pd.DataFrame) -> None:
    required_columns = [
        "日前中标出力",
        "省内中长期上网电量",
        "日前出清节点价格",
        "省内中长期均价",
        "日期",
        "公司名称",
        "日内实际出力",
    ]
    missing = [col for col in required_columns if col not in summary_df.columns]
    if missing:
        raise ValueError(f"{SUMMARY_KEY} 缺少以下列: {missing}")
    if STATUS_COLUMN not in summary_df.columns:
        raise ValueError(f"{SUMMARY_KEY} 缺少列: {STATUS_COLUMN}")
    info_required = ["机组容量", "机组名称", "公司名称"]
    missing_info = [col for col in info_required if col not in info_df.columns]
    if missing_info:
        raise ValueError(f"基础信息 缺少必需列: {missing_info}")


def compute_deep_adjustment(
    data_out: Dict[str, pd.DataFrame], start_date: pd.Timestamp, end_date: pd.Timestamp
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    summary_df = data_out[SUMMARY_KEY].copy()
    info_df = data_out[INFO_KEY]
    ensure_columns(summary_df, info_df)

    summary_df["日期"] = pd.to_datetime(summary_df["日期"])
    summary_df = summary_df[
        (summary_df["日期"] >= start_date) &
        (summary_df["日期"] <= end_date) &
        (summary_df["日前出清节点价格"] >= 0) &
        (summary_df["日前出清节点价格"] <= 200)
    ]
    if summary_df.empty:
        return pd.DataFrame(columns=DEEP_RESULT_COLUMNS), summary_df

    numeric_cols = [
        "日前中标出力",
        "省内中长期上网电量",
        "省内中长期均价",
        "日前出清节点价格",
        "日内实际出力",
    ]
    has_inter = "省间中长期上网电量" in summary_df.columns and "省间中长期均价" in summary_df.columns
    if has_inter:
        numeric_cols.extend(["省间中长期上网电量", "省间中长期均价"])
    for col in numeric_cols:
        if col in summary_df.columns:
            summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce")

    summary_df["匹配键"] = summary_df["公司名称"] + summary_df["机组名称"]
    capacity_mapping = pd.DataFrame({
        "匹配键": info_df["公司名称"] + info_df["机组名称"],
        "机组容量": pd.to_numeric(info_df["机组容量"], errors="coerce"),
    })
    summary_df = summary_df.merge(capacity_mapping, on="匹配键", how="left")
    summary_df["机组容量"] = pd.to_numeric(summary_df["机组容量"], errors="coerce").replace(0, pd.NA)
    status_mask = summary_df[STATUS_COLUMN].fillna("").astype(str).str.strip() == RUNNING_STATUS
    summary_df = summary_df[status_mask].copy()
    if summary_df.empty:
        return pd.DataFrame(columns=DEEP_RESULT_COLUMNS), summary_df

    if has_inter:
        summary_df["省间中长期上网电量"] = summary_df["省间中长期上网电量"].fillna(0)
        summary_df["省间中长期均价"] = summary_df["省间中长期均价"].fillna(0)
        contract_power = summary_df["省内中长期上网电量"] + summary_df["省间中长期上网电量"]
        intra_value = summary_df["省内中长期上网电量"] * summary_df["省内中长期均价"]
        inter_value = summary_df["省间中长期上网电量"] * summary_df["省间中长期均价"]
        total_contract_value = intra_value + inter_value
    else:
        contract_power = summary_df["省内中长期上网电量"]
        total_contract_value = summary_df["省内中长期上网电量"] * summary_df["省内中长期均价"]

    contract_price = total_contract_value / contract_power
    contract_price = contract_price.replace([float("inf"), float("-inf")], 0).fillna(0)

    condition = (
        summary_df["日前中标出力"] < contract_power * HOURS_PER_RECORD
    ) & (summary_df["日前出清节点价格"] < contract_price)
    summary_df["深调套利收入"] = (
        (contract_power * HOURS_PER_RECORD - summary_df["日前中标出力"]) *
        (contract_price - summary_df["日前出清节点价格"]) *
        summary_df["机组容量"] / COEFFICIENT
    ).where(condition, 0).fillna(0)

    capacity_series = summary_df["机组容量"]
    holding_ratio = (contract_power / capacity_series).where(capacity_series > 0, 0)
    summary_df["中长期平均持仓"] = (holding_ratio * HOURS_PER_RECORD).fillna(0)
    summary_df["单台深调平均负荷"] = summary_df["日内实际出力"] / summary_df["机组容量"]
    filtered_output = summary_df.copy()

    unit_df = summary_df.groupby("匹配键", as_index=False).agg(
        公司名称=("公司名称", "first"),
        日前低价时长_小时_=("日前出清节点价格", "count"),
        现货价格_=("日前出清节点价格", "mean"),
        深调平均负荷_=("单台深调平均负荷", "mean"),
        中长期平均持仓_=("中长期平均持仓", "mean"),
        深调套利_元_=("深调套利收入", "sum"),
    )

    result_df = unit_df.groupby("公司名称", as_index=False).agg(
        日前低价时长_小时_=("日前低价时长_小时_", "mean"),
        现货价格_=("现货价格_", "mean"),
        深调平均负荷_=("深调平均负荷_", "mean"),
        中长期平均持仓_=("中长期平均持仓_", "mean"),
        深调套利_元_=("深调套利_元_", "sum"),
    )
    result_df["日前低价时长_小时_"] /= HOURS_PER_RECORD
    result_df.columns = DEEP_RESULT_COLUMNS
    return result_df, filtered_output


def compute_high_price_stats(
    data_out: Dict[str, pd.DataFrame], start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    summary_df = data_out[SUMMARY_KEY].copy()
    info_df = data_out[INFO_KEY].copy()

    summary_df["日期"] = pd.to_datetime(summary_df["日期"])
    summary_df = summary_df[
        (summary_df["日期"] >= start_date) &
        (summary_df["日期"] <= end_date)
    ]
    if summary_df.empty:
        return pd.DataFrame(columns=HIGH_RESULT_COLUMNS)

    numeric_cols = [
        "日前中标出力",
        "省内中长期上网电量",
        "日前出清节点价格",
        "日内实际出力",
        "日内出清节点价格",
    ]
    has_inter = "省间中长期上网电量" in summary_df.columns and "省间中长期均价" in summary_df.columns
    if has_inter:
        numeric_cols.append("省间中长期上网电量")
    for col in numeric_cols:
        if col in summary_df.columns:
            summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce")

    summary_df["匹配键"] = summary_df["公司名称"] + summary_df["机组名称"]
    capacity_mapping = pd.DataFrame({
        "匹配键": info_df["公司名称"] + info_df["机组名称"],
        "机组容量": pd.to_numeric(info_df["机组容量"], errors="coerce"),
    })
    summary_df = summary_df.merge(capacity_mapping, on="匹配键", how="left")
    summary_df["机组容量"] = pd.to_numeric(summary_df["机组容量"], errors="coerce").replace(0, pd.NA)
    status_mask = summary_df[STATUS_COLUMN].fillna("").astype(str).str.strip() == RUNNING_STATUS
    summary_df = summary_df[status_mask].copy()
    if summary_df.empty:
        return pd.DataFrame(columns=HIGH_RESULT_COLUMNS)

    day_ahead_high_mask = (summary_df["日前出清节点价格"] >= 300) & (summary_df["日前出清节点价格"] <= 1500)
    real_time_high_mask = (summary_df["日内出清节点价格"] >= 300) & (summary_df["日内出清节点价格"] <= 1500)

    unit_stats = []
    for unit_key in summary_df["匹配键"].dropna().unique():
        unit_data = summary_df[summary_df["匹配键"] == unit_key]
        day_ahead_high_data = unit_data[day_ahead_high_mask[unit_data.index]]
        real_time_high_data = unit_data[real_time_high_mask[unit_data.index]]

        day_ahead_high_hours = len(day_ahead_high_data) / HOURS_PER_RECORD
        real_time_high_hours = len(real_time_high_data) / HOURS_PER_RECORD
        day_ahead_high_avg_price = day_ahead_high_data["日前出清节点价格"].mean() if len(day_ahead_high_data) > 0 else 0
        real_time_high_avg_price = real_time_high_data["日内出清节点价格"].mean() if len(real_time_high_data) > 0 else 0

        if has_inter:
            contract_power_da = (
                day_ahead_high_data["省内中长期上网电量"].fillna(0) +
                day_ahead_high_data["省间中长期上网电量"].fillna(0)
            )
        else:
            contract_power_da = day_ahead_high_data["省内中长期上网电量"].fillna(0)
        capacity = unit_data["机组容量"].iloc[0]
        if len(day_ahead_high_data) > 0 and capacity and capacity > 0:
            mid_long_position = (contract_power_da / capacity).mean() * HOURS_PER_RECORD
        else:
            mid_long_position = 0

        day_ahead_avg_output = (
            day_ahead_high_data["日前中标出力"].mean() / capacity
            if len(day_ahead_high_data) > 0 and capacity and capacity > 0 else 0
        )
        real_time_avg_output = (
            real_time_high_data["日内实际出力"].mean() / capacity
            if len(real_time_high_data) > 0 and capacity and capacity > 0 else 0
        )

        unit_stats.append(
            {
                "匹配键": unit_key,
                "公司名称": unit_data["公司名称"].iloc[0],
                "日前高价时长": day_ahead_high_hours,
                "实时高价时长": real_time_high_hours,
                "日前现货高价均价": day_ahead_high_avg_price,
                "实时现货高价均价": real_time_high_avg_price,
                "中长期平均持仓": mid_long_position,
                "高价日前平均中标负荷": day_ahead_avg_output,
                "高价实时平均负荷": real_time_avg_output,
            }
        )

    result_df = pd.DataFrame(unit_stats)
    if result_df.empty:
        return pd.DataFrame(columns=HIGH_RESULT_COLUMNS)
    final_df = result_df.groupby("公司名称", as_index=False).agg(
        {
            "日前高价时长": "mean",
            "实时高价时长": "mean",
            "日前现货高价均价": "mean",
            "实时现货高价均价": "mean",
            "中长期平均持仓": "mean",
            "高价日前平均中标负荷": "mean",
            "高价实时平均负荷": "mean",
        }
    )
    final_df["中长期平均持仓"] = final_df["中长期平均持仓"].apply(format_pct)
    final_df["高价日前平均中标负荷"] = final_df["高价日前平均中标负荷"].apply(format_pct)
    final_df["高价实时平均负荷"] = final_df["高价实时平均负荷"].apply(format_pct)
    final_df.columns = HIGH_RESULT_COLUMNS
    return final_df


def write_results(output_path: Path, dfs: Dict[str, pd.DataFrame]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path) as writer:
        for sheet_name, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run review analysis pipeline.")
    parser.add_argument("--spot-path", type=Path, default=Path("data_input/现货出清电价_REPORT0.xlsx"))
    parser.add_argument("--output-workbook", type=Path, default=Path("data_output/output.xlsx"))
    parser.add_argument("--result-path", type=Path, default=Path("data_output/review_results.xlsx"))
    parser.add_argument("--deep-start-date", default=DEEP_START_DATE)
    parser.add_argument("--deep-end-date", default=DEEP_END_DATE)
    parser.add_argument("--high-start-date", default=HIGH_START_DATE)
    parser.add_argument("--high-end-date", default=HIGH_END_DATE)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spot_summary_df, spot_detail_df = analyze_spot_prices(args.spot_path)
    data_out = load_output_workbook(args.output_workbook)

    deep_start = pd.to_datetime(args.deep_start_date)
    deep_end = pd.to_datetime(args.deep_end_date)
    high_start = pd.to_datetime(args.high_start_date)
    high_end = pd.to_datetime(args.high_end_date)

    deep_adjust_df, deep_filtered_df = compute_deep_adjustment(data_out, deep_start, deep_end)
    high_price_df = compute_high_price_stats(data_out, high_start, high_end)

    result_frames = {
        "现货摘录": spot_summary_df,
        "现货区间统计": spot_detail_df,
        "深调收益": deep_adjust_df,
        "深调区间明细": deep_filtered_df,
        "高价区间": high_price_df,
    }
    write_results(args.result_path, result_frames)
    print(f"分析完成，结果已保存到 {args.result_path}")


if __name__ == "__main__":
    main()
