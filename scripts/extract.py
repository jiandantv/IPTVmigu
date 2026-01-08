import argparse
import sys
import os
import tempfile
import shutil

def _check_match(text, keyword_str):
    """
    辅助函数：检查文本是否包含指定关键字，支持 && 和 || 逻辑。
    """
    if not keyword_str or not keyword_str.strip():
        return False

    # 处理关键词中的引号，去除首尾可能存在的双引号并清理空格
    processed_keyword = keyword_str.strip().strip('"')

    if "&&" in processed_keyword:
        sub_keywords = [k.strip() for k in processed_keyword.split("&&") if k.strip()]
        return all(k in text for k in sub_keywords)
    elif "||" in processed_keyword:
        sub_keywords = [k.strip() for k in processed_keyword.split("||") if k.strip()]
        return any(k in text for k in sub_keywords)
    else:
        return processed_keyword in text

def extract_keyword_lines(filepath, extinf_and_url_keywords=None, extinf_or_url_keywords=None, 
                          no_config=False, remove_mode=False):
    """
    高级 M3U 解析器：支持多行配置、URL 容错及去重。
    :param no_config: 如果为 True，则丢弃 #EXTVLCOPT 等中间配置行。
    :param remove_mode: 如果为 True，则删除匹配的记录，保留不匹配的记录。
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            # 过滤掉纯空行，并去除每行末尾换行符
            lines = [line.strip() for line in file if line.strip()]
    except Exception as e:
        print(f"错误：无法读取文件 {filepath}。原因：{e}")
        return []

    ordered_record_pairs = []
    seen_record_pairs = set()

    # 解析关键字逻辑
    kw1_and_kw2 = None
    if extinf_and_url_keywords:
        parts = [k.strip() for k in extinf_and_url_keywords.split(',')]
        if len(parts) == 2:
            if not parts[0] or not parts[1]:
                print("错误：--eandu 参数的两个关键字不能为空。")
                return []
            kw1_and_kw2 = (parts[0], parts[1])
        else:
            print("错误：--eandu 需要格式 'Keyword1,Keyword2'。")
            return []

    kw1_or_kw2 = None
    if extinf_or_url_keywords:
        parts = [k.strip() for k in extinf_or_url_keywords.split(',')]
        if len(parts) == 2:
            kw1_or_kw2 = (parts[0], parts[1])
        else:
            print("错误：--eoru 需要格式 'Keyword1,Keyword2'。")
            return []

    i = 0
    while i < len(lines):
        # 寻找记录起始点
        if lines[i].startswith('#EXTINF'):
            current_extinf = lines[i]
            current_sub_configs = []
            current_url = None
            
            # 向下探测，寻找 URL
            j = i + 1
            while j < len(lines):
                next_line = lines[j]
                if next_line.startswith('#EXTINF'):
                    # 异常情况：在找到 URL 前遇见了下一个标签，说明当前频道 URL 丢失
                    break
                elif next_line.startswith('#'):
                    # 收集配置行
                    current_sub_configs.append(next_line)
                    j += 1
                else:
                    # 找到第一个非 '#' 开头的行，判定为 URL
                    current_url = next_line
                    break
            
            # 如果成功锁定了一组完整的 (EXTINF + URL)
            if current_url:
                matched = False
                if kw1_and_kw2:
                    matched = _check_match(current_extinf, kw1_and_kw2[0]) and \
                              _check_match(current_url, kw1_and_kw2[1])
                elif kw1_or_kw2:
                    matched = _check_match(current_extinf, kw1_or_kw2[0]) or \
                              _check_match(current_url, kw1_or_kw2[1])

                # 根据 remove_mode 决定处理逻辑
                if remove_mode:
                    # 删除模式：只保留不匹配的记录
                    if not matched:
                        # 根据 no_config 参数决定是否包含中间行
                        if no_config:
                            record_block = [current_extinf, current_url]
                        else:
                            record_block = [current_extinf] + current_sub_configs + [current_url]
                        
                        # 去重逻辑
                        record_key = (current_extinf, current_url)
                        if record_key not in seen_record_pairs:
                            ordered_record_pairs.append(record_block)
                            seen_record_pairs.add(record_key)
                else:
                    # 原始模式：只保留匹配的记录
                    if matched:
                        # 根据 no_config 参数决定是否包含中间行
                        if no_config:
                            record_block = [current_extinf, current_url]
                        else:
                            record_block = [current_extinf] + current_sub_configs + [current_url]
                        
                        # 去重逻辑
                        record_key = (current_extinf, current_url)
                        if record_key not in seen_record_pairs:
                            ordered_record_pairs.append(record_block)
                            seen_record_pairs.add(record_key)
                
                i = j + 1  # 移动到 URL 之后的一行
            else:
                # 丢失 URL 的频道，直接跳到下一个起始点
                i = j
        else:
            # 处理文件开头的非EXTINF行（如#EXTM3U等头部信息）
            # 在删除模式下，我们保留这些行
            if remove_mode:
                ordered_record_pairs.append([lines[i]])
            i += 1

    # 展开结果，并在每个记录块后添加空行
    result = []
    for block in ordered_record_pairs:
        result.extend(block)
        result.append("") 

    # 移除最后一个空行（如果有）
    if result and result[-1] == "":
        result.pop()
    
    return result

def safe_write_output(data, input_path, output_path):
    """
    安全地写入输出文件，支持同文件覆盖
    
    :param data: 要写入的数据列表
    :param input_path: 输入文件路径
    :param output_path: 输出文件路径
    :return: (success, temp_path) 成功返回(True, None)，失败返回(False, temp_path)
    """
    # 获取绝对路径以判断是否为同一个文件
    input_abs = os.path.abspath(input_path)
    output_abs = os.path.abspath(output_path)
    is_same_file = input_abs == output_abs
    
    temp_path = None
    
    try:
        # 如果是同一个文件，先写到临时文件
        if is_same_file:
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
            for line in data:
                out_f.write(line + '\n')
        
        # 如果是同一个文件，进行原子替换
        if is_same_file:
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

def validate_arguments(args):
    """
    验证命令行参数的合理性
    """
    # 检查输入文件是否存在
    if not os.path.exists(args.input):
        print(f"错误：输入文件 '{args.input}' 不存在")
        return False
    
    # 检查输入文件是否可读
    if not os.access(args.input, os.R_OK):
        print(f"错误：输入文件 '{args.input}' 不可读")
        return False
    
    # 检查是否为文件
    if not os.path.isfile(args.input):
        print(f"错误：'{args.input}' 不是文件")
        return False
    
    # 检查输入文件扩展名（可选警告）
    if not args.input.lower().endswith('.m3u'):
        print(f"警告：输入文件 '{args.input}' 可能不是标准M3U文件")
    
    # 检查输出目录是否可写
    output_dir = os.path.dirname(os.path.abspath(args.output)) or '.'
    if not os.access(output_dir, os.W_OK):
        print(f"错误：输出目录 '{output_dir}' 不可写")
        return False
    
    # 检查输入输出是否为同一文件（提供信息性提示）
    input_abs = os.path.abspath(args.input)
    output_abs = os.path.abspath(args.output)
    
    if input_abs == output_abs:
        print("信息：输入和输出为同一文件，将安全覆盖原文件")
    
    return True

def parse_arguments():
    parser = argparse.ArgumentParser(description='从M3U文件中提取或删除包含指定关键字的记录')
    parser.add_argument('--input', required=True, help='输入M3U文件路径')
    parser.add_argument('--output', required=True, help='输出文件路径')
    parser.add_argument('-n', action='store_true', dest='no_config', 
                       help='只保留EXTINF和URL行，丢弃中间配置行')
    parser.add_argument('-r', action='store_true', dest='remove_mode', 
                       help='删除模式：删除匹配的记录，保留不匹配的记录')
    parser.add_argument('--force', action='store_true',
                       help='强制覆盖输出文件（如果已存在且与输入不同）')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--eandu', dest='extinf_and_url_keywords', 
                      help='AND模式："EXTINF关键词,URL关键词"')
    group.add_argument('--eoru', dest='extinf_or_url_keywords', 
                      help='OR模式："EXTINF关键词,URL关键词"')

    return parser.parse_args()

def get_original_channel_count(filepath):
    """
    获取原始文件中的频道数量
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            count = sum(1 for line in f if line.strip().startswith('#EXTINF'))
        return count
    except Exception as e:
        print(f"警告：无法计算原始频道数量: {e}")
        return 0

def cleanup_temp_file(temp_path):
    """
    清理临时文件
    """
    if temp_path and os.path.exists(temp_path):
        try:
            os.unlink(temp_path)
            print(f"已清理临时文件: {temp_path}")
        except Exception as e:
            print(f"警告：无法删除临时文件 {temp_path}: {e}")

if __name__ == "__main__":
    args = parse_arguments()
    
    # 验证参数
    if not validate_arguments(args):
        sys.exit(1)
    
    # 检查输出文件是否已存在且与输入不同
    input_abs = os.path.abspath(args.input)
    output_abs = os.path.abspath(args.output)
    
    if os.path.exists(args.output) and input_abs != output_abs:
        if not args.force:
            print(f"错误：输出文件 '{args.output}' 已存在")
            print("使用 --force 参数强制覆盖，或指定不同的输出文件")
            sys.exit(1)
    
    # 根据参数调用函数
    if args.extinf_and_url_keywords:
        extracted_lines = extract_keyword_lines(
            args.input, 
            extinf_and_url_keywords=args.extinf_and_url_keywords,
            no_config=args.no_config,
            remove_mode=args.remove_mode
        )
        if args.remove_mode:
            mode_str = "删除EXTINF和URL均匹配(AND)的记录"
        else:
            mode_str = "提取EXTINF和URL均匹配(AND)的记录"
    else:
        extracted_lines = extract_keyword_lines(
            args.input, 
            extinf_or_url_keywords=args.extinf_or_url_keywords,
            no_config=args.no_config,
            remove_mode=args.remove_mode
        )
        if args.remove_mode:
            mode_str = "删除EXTINF或URL匹配(OR)的记录"
        else:
            mode_str = "提取EXTINF或URL匹配(OR)的记录"
    
    # 安全写入输出文件
    success, temp_path = safe_write_output(extracted_lines, args.input, args.output)
    
    # 如果失败，清理临时文件
    if not success:
        cleanup_temp_file(temp_path)
        print("处理失败！")
        sys.exit(1)
    
    # 计算统计信息
    count = sum(1 for line in extracted_lines if line.startswith('#EXTINF'))
    
    if args.remove_mode:
        print(f"处理完成！成功保留 {count} 条记录。")
        original_count = get_original_channel_count(args.input)
        if original_count > 0:
            deleted_count = original_count - count
            print(f"删除了 {deleted_count} 条匹配的记录。")
    else:
        print(f"处理完成！成功提取 {count} 条记录。")
    
    if args.no_config:
        print("提示：已开启 -n 模式，丢弃了所有中间配置行。")
    
    print(f"模式：{mode_str}")
    print(f"结果保存至：{args.output}")
    
    # 检查是否使用了临时文件（即输入输出相同）
    if os.path.abspath(args.input) == os.path.abspath(args.output):
        print("注意：已安全覆盖原文件")
