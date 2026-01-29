import pandas as pd
from pathlib import Path
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple
import warnings
import time
from datetime import timedelta

warnings.filterwarnings('ignore', category=UserWarning)


def extract_company_name(filename: str) -> str:
    """从文件名中提取公司名称"""
    match = re.match(r'([^-]+)-', filename)
    if match:
        return match.group(1)
    return filename.split('-')[0]


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """清理数据框：移除空列和空行"""
    if df.empty:
        return df
    
    # 移除 Unnamed 列
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
    
    # 移除完全为空的列
    df = df.dropna(axis=1, how='all')
    
    # 移除完全为空的行
    df = df.dropna(axis=0, how='all')
    
    return df


def read_sheet_optimized(file_path: Path, sheet_name: str, company_name: str) -> pd.DataFrame:
    """优化的读取单个工作表的函数"""
    try:
        # 读取数据，从第2行开始（header=1）
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)
        
        # 清理空列和空行
        df = clean_dataframe(df)
        
        # 如果数据为空，直接返回
        if df.empty:
            return pd.DataFrame()
        
        # 更新公司名称列的值（已经确认所有文件都有这个列）
        if '公司名称' in df.columns:
            # 使用从文件名提取的公司名称覆盖原有值
            df['公司名称'] = company_name
        else:
            # 理论上不会走到这里，但保险起见还是加上
            df.insert(0, '公司名称', company_name)
        
        return df
        
    except Exception as e:
        print(f"  ✗ 读取 {sheet_name} 失败: {e}")
        return pd.DataFrame()


def process_single_file(file_path: Path) -> Tuple[str, Dict[str, pd.DataFrame]]:
    """处理单个文件，返回公司名称和三个数据表"""
    company_name = extract_company_name(file_path.name)
    
    result = {
        'basic': pd.DataFrame(),
        'day_ahead': pd.DataFrame(),
        'trade_price': pd.DataFrame()
    }
    
    try:
        # 读取三个工作表
        result['basic'] = read_sheet_optimized(file_path, "1.基础信息", company_name)
        result['day_ahead'] = read_sheet_optimized(file_path, "1.日前申报-信息", company_name)
        result['trade_price'] = read_sheet_optimized(file_path, "1.交易量价数据信息", company_name)
        
        return company_name, result
        
    except Exception as e:
        print(f"  ✗ 处理文件失败: {e}")
        return company_name, result


def merge_data_files(max_workers: int = 4):
    """
    合并 data_input 目录中的所有 Excel 文件
    
    Args:
        max_workers: 并行处理的最大线程数，默认为4
    """
    # 开始计时
    start_time = time.time()
    
    data_dir = Path("data_input")

    if not data_dir.exists():
        print(f"❌ 错误：目录 {data_dir} 不存在")
        return

    # 获取所有 Excel 文件
    excel_files = sorted(data_dir.glob("*.xlsx")) + sorted(data_dir.glob("*.xls"))

    if not excel_files:
        print(f"❌ 错误：目录 {data_dir} 中没有找到 Excel 文件")
        return

    print(f"📁 找到 {len(excel_files)} 个 Excel 文件")
    print("=" * 100)

    # 存储所有数据
    all_basic_info = []
    all_day_ahead_info = []
    all_trade_price_info = []

    # 统计信息
    success_count = 0
    fail_count = 0

    # 使用线程池并行处理文件
    print("🚀 开始并行处理文件...\n")
    
    # 文件处理阶段计时
    file_processing_start = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_file = {executor.submit(process_single_file, file_path): file_path 
                          for file_path in excel_files}
        
        # 处理完成的任务
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                company_name, result = future.result()
                
                # 统计各表的行数
                basic_rows = len(result['basic']) if not result['basic'].empty else 0
                day_ahead_rows = len(result['day_ahead']) if not result['day_ahead'].empty else 0
                trade_price_rows = len(result['trade_price']) if not result['trade_price'].empty else 0
                
                if basic_rows > 0 or day_ahead_rows > 0 or trade_price_rows > 0:
                    print(f"✅ {file_path.name}")
                    print(f"   公司: {company_name}")
                    print(f"   基础信息: {basic_rows} 行 | 日前申报: {day_ahead_rows} 行 | 交易量价: {trade_price_rows} 行")
                    success_count += 1
                else:
                    print(f"⚠️  {file_path.name} - 没有读取到有效数据")
                    fail_count += 1
                
                # 添加到列表（只添加非空数据）
                if not result['basic'].empty:
                    all_basic_info.append(result['basic'])
                if not result['day_ahead'].empty:
                    all_day_ahead_info.append(result['day_ahead'])
                if not result['trade_price'].empty:
                    all_trade_price_info.append(result['trade_price'])
                
            except Exception as e:
                print(f"❌ {file_path.name} 处理失败: {e}")
                fail_count += 1
            
            print("-" * 100)

    # 文件处理完成，显示用时
    file_processing_time = time.time() - file_processing_start
    print(f"\n⏱️  文件处理完成，用时: {timedelta(seconds=int(file_processing_time))}")
    
    # 检查是否有数据
    if not all_basic_info and not all_day_ahead_info and not all_trade_price_info:
        print("\n❌ 错误：所有文件都没有读取到有效数据")
        return

    # 合并数据
    print(f"\n📊 开始合并数据...")
    print("=" * 100)
    
    # 数据合并阶段计时
    merge_start = time.time()
    
    merged_data = {}
    
    if all_basic_info:
        print("🔄 合并基础信息...")
        merged_data['basic'] = pd.concat(all_basic_info, ignore_index=True)
        # 再次清理（确保合并后没有重复的空列）
        merged_data['basic'] = clean_dataframe(merged_data['basic'])
        print(f"   ✓ 完成: {len(merged_data['basic'])} 行, {len(merged_data['basic'].columns)} 列")
    else:
        print("⚠️  基础信息: 没有有效数据")

    if all_day_ahead_info:
        print("🔄 合并日前申报信息...")
        merged_data['day_ahead'] = pd.concat(all_day_ahead_info, ignore_index=True)
        merged_data['day_ahead'] = clean_dataframe(merged_data['day_ahead'])
        print(f"   ✓ 完成: {len(merged_data['day_ahead'])} 行, {len(merged_data['day_ahead'].columns)} 列")
    else:
        print("⚠️  日前申报: 没有有效数据")

    if all_trade_price_info:
        print("🔄 合并交易量价数据信息...")
        merged_data['trade_price'] = pd.concat(all_trade_price_info, ignore_index=True)
        merged_data['trade_price'] = clean_dataframe(merged_data['trade_price'])
        print(f"   ✓ 完成: {len(merged_data['trade_price'])} 行, {len(merged_data['trade_price'].columns)} 列")
    else:
        print("⚠️  交易量价: 没有有效数据")

    # 数据合并完成，显示用时
    merge_time = time.time() - merge_start
    print(f"\n⏱️  数据合并完成，用时: {timedelta(seconds=int(merge_time))}")
    
    # 保存到 Excel 文件
    output_path = "data_output/合并数据_汇总.xlsx"
    output_dir = Path(output_path).parent
    output_dir.mkdir(exist_ok=True)

    print(f"\n💾 保存到: {output_path}")
    print("=" * 100)
    
    # 文件保存阶段计时
    save_start = time.time()
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            sheets_written = 0
            
            if 'basic' in merged_data and not merged_data['basic'].empty:
                merged_data['basic'].to_excel(writer, sheet_name="基础信息", index=False)
                sheets_written += 1
                print(f"   ✓ 写入工作表: 基础信息 ({len(merged_data['basic'])} 行)")
            
            if 'day_ahead' in merged_data and not merged_data['day_ahead'].empty:
                merged_data['day_ahead'].to_excel(writer, sheet_name="日前申报信息", index=False)
                sheets_written += 1
                print(f"   ✓ 写入工作表: 日前申报信息 ({len(merged_data['day_ahead'])} 行)")
            
            if 'trade_price' in merged_data and not merged_data['trade_price'].empty:
                merged_data['trade_price'].to_excel(writer, sheet_name="交易量价数据信息", index=False)
                sheets_written += 1
                print(f"   ✓ 写入工作表: 交易量价数据信息 ({len(merged_data['trade_price'])} 行)")
            
            if sheets_written == 0:
                # 如果没有任何数据，创建一个提示工作表
                pd.DataFrame({'提示': ['所有工作表都没有有效数据']}).to_excel(
                    writer, sheet_name="提示", index=False
                )
                print(f"   ⚠️  创建提示工作表（无有效数据）")
        
        # 文件保存完成，显示用时
        save_time = time.time() - save_start
        print(f"\n⏱️  文件保存完成，用时: {timedelta(seconds=int(save_time))}")
        print(f"\n✅ 保存完成！")
        
    except Exception as e:
        print(f"\n❌ 保存文件时出错: {e}")
        return
    
    # 打印最终统计信息
    print("\n" + "=" * 100)
    print("📈 合并统计报告")
    print("=" * 100)
    print(f"处理文件总数: {len(excel_files)}")
    print(f"  ✅ 成功: {success_count} 个")
    print(f"  ❌ 失败: {fail_count} 个")
    
    if 'basic' in merged_data and not merged_data['basic'].empty:
        print(f"\n【基础信息】")
        print(f"  总行数: {len(merged_data['basic']):,}")
        print(f"  总列数: {len(merged_data['basic'].columns)}")
        print(f"  列名: {', '.join(merged_data['basic'].columns.tolist())}")
        print(f"  公司数: {merged_data['basic']['公司名称'].nunique()}")
        print(f"  公司列表: {', '.join(merged_data['basic']['公司名称'].unique().tolist())}")
    
    if 'day_ahead' in merged_data and not merged_data['day_ahead'].empty:
        print(f"\n【日前申报信息】")
        print(f"  总行数: {len(merged_data['day_ahead']):,}")
        print(f"  总列数: {len(merged_data['day_ahead'].columns)}")
        print(f"  列名: {', '.join(merged_data['day_ahead'].columns.tolist())}")
        print(f"  公司数: {merged_data['day_ahead']['公司名称'].nunique()}")
    
    if 'trade_price' in merged_data and not merged_data['trade_price'].empty:
        print(f"\n【交易量价数据信息】")
        print(f"  总行数: {len(merged_data['trade_price']):,}")
        print(f"  总列数: {len(merged_data['trade_price'].columns)}")
        print(f"  列名: {', '.join(merged_data['trade_price'].columns.tolist())}")
        print(f"  公司数: {merged_data['trade_price']['公司名称'].nunique()}")
    
    print("=" * 100)

    # 显示数据预览
    print("\n" + "=" * 100)
    print("📋 数据预览")
    print("=" * 100)
    
    if 'basic' in merged_data and not merged_data['basic'].empty:
        print("\n【基础信息】前 3 行:")
        print(merged_data['basic'].head(3).to_string(index=False))
    
    if 'day_ahead' in merged_data and not merged_data['day_ahead'].empty:
        print("\n【日前申报信息】前 3 行:")
        print(merged_data['day_ahead'].head(3).to_string(index=False))
    
    if 'trade_price' in merged_data and not merged_data['trade_price'].empty:
        print("\n【交易量价数据信息】前 3 行:")
        print(merged_data['trade_price'].head(3).to_string(index=False))
    
    print("\n" + "=" * 100)
    print("🎉 处理完成！")
    print("=" * 100)
    
    # 计算并显示总用时
    total_time = time.time() - start_time
    print(f"\n⏱️  总用时: {timedelta(seconds=int(total_time))}")
    print(f"   - 文件处理: {timedelta(seconds=int(file_processing_time))}")
    print(f"   - 数据合并: {timedelta(seconds=int(merge_time))}")
    print(f"   - 文件保存: {timedelta(seconds=int(save_time))}")
    print("=" * 100)


if __name__ == "__main__":
    # 可以调整 max_workers 参数来控制并行处理的线程数
    # 根据你的 CPU 核心数调整，建议值：2-8
    merge_data_files(max_workers=4)