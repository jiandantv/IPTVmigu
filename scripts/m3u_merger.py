import re
import argparse
import sys
import os

def merge_m3u_channels(m3u_content):
    """
    合并M3U内容中同名频道下的所有URL。
    
    :param m3u_content: 原始M3U文件的字符串内容。
    :return: 格式化后的M3U字符串内容。
    """
    if not m3u_content:
        return ""

    # 使用换行符分隔内容，去除首尾空白
    lines = [line.strip() for line in m3u_content.strip().split('\n') if line.strip()]
    
    header = ""
    # 存储合并后的频道数据: { "频道名称": {"info": "#EXTINF...", "urls": {"url1", "url2", ...}} }
    channels_map = {}
    
    current_info_line = None
    current_channel_name = None
    
    # --- 第一步：解析和分组数据 ---
    for line in lines:
        
        # 1. 识别 M3U 头部 (只保留第一个文件的头部)
        if line.startswith('#EXTM3U'):
            if not header:
                header = line
            continue

        # 2. 识别频道信息行
        if line.startswith('#EXTINF:'):
            current_info_line = line
            # 使用正则表达式提取频道名称
            match = re.search(r',(.+)$', line)
            if match:
                current_channel_name = match.group(1).strip()
            else:
                current_channel_name = None 
            
            if current_channel_name:
                if current_channel_name not in channels_map:
                    # 首次遇到，初始化
                    channels_map[current_channel_name] = {"info": current_info_line, "urls": set()}
                else:
                    # 非首次遇到，更新属性行（选择最新/最后读取到的）
                    channels_map[current_channel_name]["info"] = current_info_line
            
        # 3. 识别 URL 行
        # 只要当前有激活的频道名称，并且当前行是 URL，就进行收集。
        elif (line.startswith('http://') or line.startswith('https://')):
            if current_channel_name and current_channel_name in channels_map:
                # 将 URL 添加到对应频道的集合中 (集合会自动去重)
                channels_map[current_channel_name]["urls"].add(line)
            
            # --- 关键修改：不再重置 current_channel_name/current_info_line。
            # 这允许连续的多个 URL 都归属于同一个 #EXTINF。
            # 只有遇到下一个 #EXTINF 或非 URL/EXTINF 行时，它才会被覆盖或重置。
        
        # 4. 遇到非 M3U 标签、非 #EXTINF 和 非 URL 的行 (例如注释或空行)，则重置状态
        else:
             current_channel_name = None
             current_info_line = None


    # --- 第二步：重新构建 M3U 内容 ---
    output_lines = [header]
    
    # 遍历已分组的频道
    for name, data in channels_map.items():
        # 添加合并后的 #EXTINF 行
        output_lines.append(data["info"])
        # 添加所有收集到的 URL，并排序以保持一致性
        for url in sorted(list(data["urls"])):
            output_lines.append(url)
            
    return '\n'.join(output_lines)

# (main 函数保持不变，因为它处理文件 I/O 是正确的)
def main():
    parser = argparse.ArgumentParser(
        description="合并多个M3U文件的内容，对同名频道下的所有URL进行去重和分组，并输出到一个文件。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-i', '--input',
        type=str,
        nargs='+',  
        required=True,
        help="一个或多个输入M3U文件的路径，例如: input1.m3u input2.m3u"
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        required=True,
        help="输出M3U文件的路径，例如: combined.m3u"
    )

    args = parser.parse_args()
    all_m3u_content = []
    
    for input_file in args.input:
        if not os.path.exists(input_file):
            print(f"错误: 输入文件 '{input_file}' 不存在。跳过此文件。", file=sys.stderr)
            continue
            
        if input_file == args.output:
            print(f"警告: 输入文件 '{input_file}' 和输出文件不能是同一个文件。跳过此文件。", file=sys.stderr)
            continue

        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                all_m3u_content.append(f.read())
                
        except Exception as e:
            print(f"读取文件 '{input_file}' 时发生错误: {e}", file=sys.stderr)
            sys.exit(1)

    if not all_m3u_content:
        print("没有可供处理的输入文件内容。程序退出。", file=sys.stderr)
        sys.exit(0)
        
    combined_content = '\n'.join(all_m3u_content)

    try:
        modified_m3u = merge_m3u_channels(combined_content)
        
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(modified_m3u)
            
        print(f"成功: {len(args.input)} 个 M3U 文件已合并，并写入到 '{args.output}'")
        
    except Exception as e:
        print(f"处理文件时发生错误: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    # 根据用户要求，检查代码中的拼写和语法，以确保代码的稳定性和准确性。
    # 代码已检查。
    main()
