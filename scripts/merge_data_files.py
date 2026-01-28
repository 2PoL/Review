import pandas as pd
from pathlib import Path
import re


def extract_company_name(filename):
    """从文件名中提取公司名称"""
    # 匹配文件名中的公司名称，如 "同承-电力营销信息统计1.10-202601.12(1).xlsx"
    match = re.match(r'([^-]+)-', filename)
    if match:
        return match.group(1)
    return filename.split('-')[0]


def merge_data_files():
    """合并 data_input 目录中的所有 Excel 文件"""
    data_dir = Path("data_input")

    if not data_dir.exists():
        print(f"错误：目录 {data_dir} 不存在")
        return

    # 获取所有 Excel 文件
    excel_files = sorted(data_dir.glob("*.xlsx")) + sorted(data_dir.glob("*.xls"))

    if not excel_files:
        print(f"错误：目录 {data_dir} 中没有找到 Excel 文件")
        return

    print(f"找到 {len(excel_files)} 个 Excel 文件")
    print("=" * 80)

    # 存储所有公司的基础信息、日前申报信息和交易量价数据信息
    all_basic_info = []
    all_day_ahead_info = []
    all_trade_price_info = []

    for file_path in excel_files:
        company_name = extract_company_name(file_path.name)
        print(f"处理文件: {file_path.name} -> 公司: {company_name}")

        try:
            # 读取基础信息
            df_basic = pd.read_excel(file_path, sheet_name="1.基础信息", header=1)
            # 删除可能存在的 "Unnamed: 0" 列
            if "Unnamed: 0" in df_basic.columns:
                df_basic = df_basic.drop(columns=["Unnamed: 0"])
            df_basic['公司名称'] = company_name
            all_basic_info.append(df_basic)

            # 读取日前申报信息
            df_day_ahead = pd.read_excel(file_path, sheet_name="1.日前申报-信息", header=1)
            # 删除可能存在的 "Unnamed: 0" 列
            if "Unnamed: 0" in df_day_ahead.columns:
                df_day_ahead = df_day_ahead.drop(columns=["Unnamed: 0"])
            df_day_ahead['公司名称'] = company_name
            all_day_ahead_info.append(df_day_ahead)

            # 读取交易量价数据信息
            df_trade_price = pd.read_excel(file_path, sheet_name="1.交易量价数据信息", header=1)
            # 删除可能存在的 "Unnamed: 0" 列
            if "Unnamed: 0" in df_trade_price.columns:
                df_trade_price = df_trade_price.drop(columns=["Unnamed: 0"])
            df_trade_price['公司名称'] = company_name
            all_trade_price_info.append(df_trade_price)

            print(f"  基础信息: {len(df_basic)} 行")
            print(f"  日前申报信息: {len(df_day_ahead)} 行")
            print(f"  交易量价数据信息: {len(df_trade_price)} 行")

        except Exception as e:
            print(f"  错误: {e}")

        print("-" * 80)

    # 合并所有基础信息
    if all_basic_info:
        print("\n合并基础信息...")
        merged_basic_info = pd.concat(all_basic_info, ignore_index=True)
        print(f"基础信息总行数: {len(merged_basic_info)}")

    # 合并所有日前申报信息
    if all_day_ahead_info:
        print("合并日前申报信息...")
        merged_day_ahead = pd.concat(all_day_ahead_info, ignore_index=True)
        print(f"日前申报信息总行数: {len(merged_day_ahead)}")

    # 合并所有交易量价数据信息
    if all_trade_price_info:
        print("合并交易量价数据信息...")
        merged_trade_price = pd.concat(all_trade_price_info, ignore_index=True)
        print(f"交易量价数据信息总行数: {len(merged_trade_price)}")

    # 保存到 Excel 文件
    output_path = "data_output/合并数据_汇总.xlsx"
    output_dir = Path(output_path).parent
    output_dir.mkdir(exist_ok=True)

    print(f"\n保存到: {output_path}")

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        if all_basic_info:
            merged_basic_info.to_excel(writer, sheet_name="基础信息", index=False)
        if all_day_ahead_info:
            merged_day_ahead.to_excel(writer, sheet_name="日前申报信息", index=False)
        if all_trade_price_info:
            merged_trade_price.to_excel(writer, sheet_name="交易量价数据信息", index=False)

    print("完成！")
    print("\n" + "=" * 80)
    print("合并统计:")
    print(f"  处理文件数: {len(excel_files)}")
    if all_basic_info:
        print(f"  基础信息总行数: {len(merged_basic_info)}")
        print(f"  基础信息列: {list(merged_basic_info.columns)}")
    if all_day_ahead_info:
        print(f"  日前申报信息总行数: {len(merged_day_ahead)}")
        print(f"  日前申报信息列: {list(merged_day_ahead.columns)}")
    if all_trade_price_info:
        print(f"  交易量价数据信息总行数: {len(merged_trade_price)}")
        print(f"  交易量价数据信息列: {list(merged_trade_price.columns)}")
    print("=" * 80)

    # 显示数据预览
    if all_basic_info:
        print("\n基础信息预览:")
        print(merged_basic_info.head(10).to_string())
    if all_day_ahead_info:
        print("\n日前申报信息预览:")
        print(merged_day_ahead.head(10).to_string())
    if all_trade_price_info:
        print("\n交易量价数据信息预览:")
        print(merged_trade_price.head(10).to_string())


if __name__ == "__main__":
    merge_data_files()
