import re
import argparse
import sys
import os
import tempfile
import shutil

# --- 辅助函数：提取 Group-Title ---
def extract_group_title(info_line):
    """从 #EXTINF 行中提取 group-title 的值。"""
    match = re.search(r'group-title="([^"]*)"', info_line)
    if match:
        return match.group(1).strip()
    return ""

# --- 辅助函数：解析单个 M3U 内容 (使用复合键 (频道名, Group) 保证独立性) ---
def parse_single_m3u(m3u_content):
    if not m3u_content:
        return [], {}, ""
        
    lines = [line.strip() for line in m3u_content.strip().split('\n') if line.strip()]
    
    # channels_map 结构: { ("频道名称", "Group-Title"): {"info": "#EXTINF...", "urls": set()} }
    channels_map = {}
    order_list = [] # 包含 ("频道名称", "Group-Title") 复合键
    header = ""
    
    current_info_line = None
    current_channel_name = None
    current_group_title = None
    
    for line in lines:
        if line.startswith('#EXTM3U'):
            if not header:
                header = line
            continue

        if line.startswith('#EXTINF:'):
            current_info_line = line
            name_match = re.search(r',(.+)$', line)
            current_channel_name = name_match.group(1).strip() if name_match else None
            current_group_title = extract_group_title(current_info_line)
            
            if current_channel_name:
                # 使用复合键 (Name, Group) 确保独立性
                channel_key = (current_channel_name, current_group_title)
                
                if channel_key not in channels_map:
                    channels_map[channel_key] = {
                        "info": current_info_line, 
                        "urls": set(),
                    }
                    order_list.append(channel_key)
                else:
                    # 频道实体已存在（名称和分组都相同），更新信息
                    channels_map[channel_key]["info"] = current_info_line
            
        elif (line.startswith('http://') or line.startswith('https://')):
            # URL 属于最近解析成功的频道实体
            if current_channel_name and current_group_title is not None:
                channel_key = (current_channel_name, current_group_title)
                if channel_key in channels_map:
                    channels_map[channel_key]["urls"].add(line)
        
        else:
            # 遇到无关行，重置解析状态
            current_channel_name = None
            current_group_title = None

    return order_list, channels_map, header


# --- 安全文件写入函数 ---
def safe_write_output(content, input_files, output_path):
    """
    安全地写入输出文件，支持输入文件包含输出文件的情况
    
    :param content: 要写入的内容字符串
    :param input_files: 输入文件路径列表
    :param output_path: 输出文件路径
    :return: (success, temp_path) 成功返回(True, None)，失败返回(False, temp_path)
    """
    # 检查输出文件是否在输入文件中
    output_abs = os.path.abspath(output_path)
    input_abs_list = [os.path.abspath(f) for f in input_files if os.path.exists(f)]
    
    is_output_in_inputs = output_abs in input_abs_list
    temp_path = None
    
    try:
        # 如果输出文件在输入文件中，先写到临时文件
        if is_output_in_inputs:
            # 在与输出文件相同目录创建临时文件
            output_dir = os.path.dirname(output_path) or '.'
            fd, temp_path = tempfile.mkstemp(
                dir=output_dir,
                suffix='.m3u',
                prefix='.tmp_',
                text=True
            )
            
            # 使用文件描述符打开文件
            out_f = os.fdopen(fd, 'w', encoding='utf-8')
        else:
            # 直接打开输出文件
            out_f = open(output_path, 'w', encoding='utf-8')
        
        # 写入数据
        with out_f:
            out_f.write(content)
        
        # 如果是输出文件在输入文件中，进行原子替换
        if is_output_in_inputs:
            try:
                # Python 3.3+ 推荐使用 os.replace 实现原子替换
                os.replace(temp_path, output_path)
                temp_path = None  # 替换成功，清除临时文件引用
            except Exception as e:
                # 如果 os.replace 失败，使用 shutil.move 作为备选
                print(f"警告：原子替换失败，使用备选方案: {e}")
                shutil.move(temp_path, output_path)
                temp_path = None  # 移动成功，清除临时文件引用
        
        return True, None
        
    except Exception as e:
        print(f"写入文件失败: {e}")
        return False, temp_path


# --- 验证参数函数 ---
def validate_arguments(input_files, output_path):
    """
    验证命令行参数的合理性
    
    :param input_files: 输入文件路径列表
    :param output_path: 输出文件路径
    :return: 验证成功返回True，失败返回False
    """
    # 检查至少有一个有效的输入文件
    valid_inputs = []
    for input_file in input_files:
        if not os.path.exists(input_file):
            print(f"警告: 输入文件 '{input_file}' 不存在。", file=sys.stderr)
            continue
        
        if not os.access(input_file, os.R_OK):
            print(f"错误: 输入文件 '{input_file}' 不可读", file=sys.stderr)
            return False
        
        if not os.path.isfile(input_file):
            print(f"错误: '{input_file}' 不是文件", file=sys.stderr)
            return False
        
        # 检查文件扩展名（可选警告）
        if not input_file.lower().endswith('.m3u'):
            print(f"警告: 输入文件 '{input_file}' 可能不是标准M3U文件", file=sys.stderr)
        
        valid_inputs.append(input_file)
    
    if not valid_inputs:
        print("错误: 没有有效的输入文件", file=sys.stderr)
        return False
    
    # 检查输出目录是否可写
    output_dir = os.path.dirname(os.path.abspath(output_path)) or '.'
    if not os.access(output_dir, os.W_OK):
        print(f"错误: 输出目录 '{output_dir}' 不可写", file=sys.stderr)
        return False
    
    # 检查输出文件是否已存在（不是输入文件之一）
    output_abs = os.path.abspath(output_path)
    if os.path.exists(output_path) and output_abs not in [os.path.abspath(f) for f in valid_inputs]:
        print(f"警告: 输出文件 '{output_path}' 已存在且不是输入文件", file=sys.stderr)
        print("      输出文件将被覆盖", file=sys.stderr)
    
    # 检查输出文件是否在输入文件中（提供信息性提示）
    if output_abs in [os.path.abspath(f) for f in valid_inputs]:
        print(f"信息: 输出文件 '{output_path}' 是输入文件之一，将安全覆盖", file=sys.stderr)
    
    return True


# --- 清理临时文件函数 ---
def cleanup_temp_file(temp_path):
    """
    清理临时文件
    """
    if temp_path and os.path.exists(temp_path):
        try:
            os.unlink(temp_path)
            print(f"已清理临时文件: {temp_path}", file=sys.stderr)
        except Exception as e:
            print(f"警告: 无法删除临时文件 {temp_path}: {e}", file=sys.stderr)


# --- 主函数：实现 Group-Title 优先的相对插入排序逻辑 ---
def main():
    parser = argparse.ArgumentParser(
        description="合并M3U文件，使用 (频道名, Group-Title) 复合键保证独立性，并进行 Group-Title 优先的相对插入排序。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('-i', '--input', type=str, nargs='+', required=True, 
                       help="一个或多个输入M3U文件的路径")
    parser.add_argument('-o', '--output', type=str, required=True, 
                       help="输出M3U文件的路径")
    parser.add_argument('--force', action='store_true',
                       help="强制操作，即使输出文件已存在且不是输入文件")
    
    args = parser.parse_args()
    
    if not args.input:
        print("错误: 请提供至少一个输入文件。", file=sys.stderr)
        sys.exit(1)
    
    # 验证参数
    if not validate_arguments(args.input, args.output):
        sys.exit(1)
    
    # 检查输出文件是否已存在且不是输入文件
    output_abs = os.path.abspath(args.output)
    input_abs_list = [os.path.abspath(f) for f in args.input if os.path.exists(f)]
    
    if os.path.exists(args.output) and output_abs not in input_abs_list:
        if not args.force:
            print(f"错误: 输出文件 '{args.output}' 已存在且不是输入文件", file=sys.stderr)
            print("      使用 --force 参数强制覆盖，或指定不同的输出文件", file=sys.stderr)
            sys.exit(1)
    
    # 主数据结构：
    # final_channels_data = {
    #     "group_name": {
    #         "channels": { "频道名": {"info": "#EXTINF...", "urls": set()} }, # 注意：这里的键是频道名字符串
    #         "order_list": ["频道名1", "频道名2", ...] # 该分组的内部顺序列表
    #     }
    # }
    final_channels_data = {}
    # 记录 Group-Title 首次出现的顺序（用于最终 Group 排序）
    group_global_order = [] 
    final_header = ""
    
    # 1. 遍历所有输入文件并合并数据
    valid_input_files = []
    for input_file in args.input:
        if not os.path.exists(input_file):
            print(f"警告: 输入文件 '{input_file}' 不存在。跳过。", file=sys.stderr)
            continue
            
        valid_input_files.append(input_file)
        
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # current_order_list 包含 (Name, Group) 复合键
            # current_map 包含 { (Name, Group): {"info":..., "urls":...} }
            current_order_list, current_map, header = parse_single_m3u(content)
            
            if not final_header and header:
                final_header = header
            
            # A. 将当前文件解析出的频道按 Group-Title 再次分组
            current_groups = {}
            for channel_key in current_order_list:
                _, group = channel_key # 仅提取 Group-Title
                data = current_map[channel_key]
                
                if group not in current_groups:
                    current_groups[group] = []
                # 存储复合键和数据，以便后续处理
                current_groups[group].append((channel_key, data)) 

            # B. 遍历当前文件中的 Group-Title，执行合并和相对插入
            for group_title, current_group_items in current_groups.items():
                
                # 1. 初始化 Group 数据
                if group_title not in final_channels_data:
                    final_channels_data[group_title] = {"channels": {}, "order_list": []}
                    group_global_order.append(group_title) # 记录新的 Group 顺序
                
                final_group_data = final_channels_data[group_title]
                final_group_channels = final_group_data["channels"]
                final_group_order = final_group_data["order_list"]
                
                # 2. 执行组内相对插入排序
                last_known_channel_index = -1

                for channel_key, current_channel_data in current_group_items:
                    channel_name, _ = channel_key
                    
                    if channel_name in final_group_channels:
                        # 频道已存在于这个 Group 中: A. 合并 URL 并更新属性
                        
                        final_group_channels[channel_name]["info"] = current_channel_data["info"]
                        final_group_channels[channel_name]["urls"].update(current_channel_data["urls"])
                        
                        # 更新 last_known_channel_index
                        try:
                            last_known_channel_index = final_group_order.index(channel_name)
                        except ValueError:
                            pass
                            
                    else:
                        # 频道是新的 Group 实体: B. 相对插入
                        
                        # 1. 将新频道添加到 Group Map (使用 channel_name 作为键)
                        final_group_channels[channel_name] = {
                            "info": current_channel_data["info"], 
                            "urls": current_channel_data["urls"]
                        }
                        
                        # 2. 插入到 order_list 中 (使用 channel_name)
                        insert_index = last_known_channel_index + 1
                        final_group_order.insert(insert_index, channel_name)
                        
                        # 3. 更新 last_known_channel_index
                        last_known_channel_index = insert_index
                        
        except Exception as e:
            print(f"处理文件 '{input_file}' 时发生错误: {e}", file=sys.stderr)
            sys.exit(1)

    # 2. 生成最终结果：按 Group Global Order 和 Group Order List 生成内容
    output_lines = [final_header] if final_header else []
    
    for group_title in group_global_order:
        if group_title in final_channels_data:
            group_data = final_channels_data[group_title]
            
            for name in group_data["order_list"]:
                if name in group_data["channels"]:
                    data = group_data["channels"][name]
                    
                    # 写入 EXTINF 行
                    output_lines.append(data["info"])
                    
                    # 写入 URL 行 (排序后，保持稳定)
                    for url in sorted(list(data["urls"])):
                        output_lines.append(url)
                
    modified_m3u = '\n'.join(output_lines)

    # 3. 安全写入输出文件
    success, temp_path = safe_write_output(modified_m3u, valid_input_files, args.output)
    
    # 如果失败，清理临时文件
    if not success:
        cleanup_temp_file(temp_path)
        print("处理失败！", file=sys.stderr)
        sys.exit(1)
    
    # 计算统计信息
    total_channels = 0
    total_groups = len(group_global_order)
    
    for group_title in group_global_order:
        if group_title in final_channels_data:
            group_data = final_channels_data[group_title]
            total_channels += len(group_data["order_list"])
    
    print(f"成功: {len(valid_input_files)} 个 M3U 文件已合并", file=sys.stderr)
    print(f"      共 {total_channels} 个频道，{total_groups} 个分组", file=sys.stderr)
    print(f"      结果已写入 '{args.output}'", file=sys.stderr)
    
    # 检查是否使用了安全覆盖
    output_abs = os.path.abspath(args.output)
    if output_abs in [os.path.abspath(f) for f in valid_input_files]:
        print(f"注意: 已安全覆盖输入文件 '{args.output}'", file=sys.stderr)


if __name__ == "__main__":
    main()
