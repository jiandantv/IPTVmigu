import re
import argparse
import os
#
# --- 1. 辅助函数：提取归一化 Key ---
def get_norm_key(name):
    """去掉横杠和后缀'台'，转大写，用于判断是否为同名频道"""
    if not name: return ""
    temp = name.replace('-', '')
    if temp.endswith('台'):
        temp = temp[:-1]
    return temp.strip().upper()

# --- 2. 辅助函数：判断显示优先级 ---
def is_preferred(name):
    """判断名字是否含有横杠或'台'"""
    return '-' in name or name.endswith('台')

# --- 3. 辅助函数：提取 CCTV 数字 ---
def extract_cctv_num(name):
    """提取 CCTV 后的数字，用于排序。如果没有数字则排在最后。"""
    match = re.search(r'(?i)CCTV-?(\d+)', name)
    return int(match.group(1)) if match else 999

# --- 4. 辅助函数：解析 M3U ---
def parse_m3u(file_path):
    if not os.path.exists(file_path):
        return None, []
        
    channels = {} # key: norm_key, value: data_dict
    order = []    # 记录第一次发现该频道的顺序
    header = "#EXTM3U"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_info = None
    current_name = None
    
    for line in lines:
        line = line.strip()
        if line.startswith('#EXTM3U'):
            header = line
            continue
            
        if line.startswith('#EXTINF:'):
            current_info = line
            # 提取逗号后的频道名
            name_match = re.search(r',([^,]+)$', line)
            current_name = name_match.group(1).strip() if name_match else None
            
        elif line.startswith(('http://', 'https://')) and current_name:
            norm_key = get_norm_key(current_name)
            
            # 提取原有的 group-title
            group_match = re.search(r'group-title="([^"]*)"', current_info)
            original_group = group_match.group(1) if group_match else "其他"
            
            if norm_key not in channels:
                channels[norm_key] = {
                    "info": current_info,
                    "name": current_name,
                    "urls": {line},
                    "original_group": original_group,
                    "order_idx": len(order)
                }
                order.append(norm_key)
            else:
                # 合并 URL
                channels[norm_key]["urls"].add(line)
                # 检查显示名称优先级：如果新名字更符合偏好，更新 info
                old_name = channels[norm_key]["name"]
                if is_preferred(current_name) and not is_preferred(old_name):
                    channels[norm_key]["info"] = current_info
                    channels[norm_key]["name"] = current_name
                    
    return header, channels

# --- 5. 主逻辑 ---
def main():
    parser = argparse.ArgumentParser(description="单文件M3U频道合并排序脚本")
    parser.add_argument('-i', '--input', required=True, help="输入M3U文件")
    parser.add_argument('-o', '--output', required=True, help="输出M3U文件")
    args = parser.parse_args()

    header, channels = parse_m3u(args.input)
    if not channels:
        print("未发现有效频道数据。")
        return

    # 分类桶
    cctv_bucket = []
    weishee_bucket = []
    other_bucket = []

    for key, data in channels.items():
        name = data["name"]
        if "CCTV" in name.upper():
            data["final_group"] = "央视"
            cctv_bucket.append(data)
        elif "卫视" in name:
            data["final_group"] = "卫视"
            weishee_bucket.append(data)
        else:
            data["final_group"] = data["original_group"]
            other_bucket.append(data)

    # 排序：
    # 央视：按数字排
    cctv_bucket.sort(key=lambda x: extract_cctv_num(x["name"]))
    # 卫视：按原顺序排
    weishee_bucket.sort(key=lambda x: x["order_idx"])
    # 其他：按原频道组名，组内按原顺序
    other_bucket.sort(key=lambda x: (x["original_group"], x["order_idx"]))

    # 生成最终列表
    final_list = cctv_bucket + weishee_bucket + other_bucket

    # 写入文件
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(header + '\n')
        for item in final_list:
            # 替换或更新 info 行中的 group-title
            info = item["info"]
            new_group = item["final_group"]
            if 'group-title="' in info:
                info = re.sub(r'group-title="[^"]*"', f'group-title="{new_group}"', info)
            else:
                info = info.replace('#EXTINF:', f'#EXTINF: group-title="{new_group}",')
            
            f.write(info + '\n')
            # URL 排序输出保持稳定
            for url in sorted(list(item["urls"])):
                f.write(url + '\n')

    print(f"处理完成！\n- 央视：{len(cctv_bucket)} 个\n- 卫视：{len(weishee_bucket)} 个\n- 其他：{len(other_bucket)} 个")

if __name__ == "__main__":
    main()
