import argparse
import sys
import re
import os
import tempfile
import shutil
import traceback
from typing import List, Dict, Optional, Tuple, Set

# ==================== 调试和错误处理配置 ====================
DEBUG_MODE = os.environ.get('DEBUG', 'false').lower() == 'true'
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'info').lower()

def debug_log(message: str, level: str = 'debug'):
    """分级日志输出"""
    if DEBUG_MODE or level in ['error', 'warn', 'info']:
        prefix = {
            'debug': '🔍 DEBUG',
            'info': 'ℹ️ INFO',
            'warn': '⚠️ WARN',
            'error': '❌ ERROR'
        }.get(level, 'ℹ️ INFO')
        
        # 限制调试输出的详细程度
        if level == 'debug' and LOG_LEVEL not in ['debug', 'trace']:
            return
        
        print(f"{prefix}: {message}")

def log_exception(e: Exception, context: str = ""):
    """记录异常详细信息"""
    debug_log(f"{context}发生异常: {type(e).__name__}: {e}", 'error')
    if DEBUG_MODE:
        print("异常堆栈跟踪:")
        traceback.print_exc()

# ==================== 参数验证增强 ====================
def validate_arguments_extended(args) -> Tuple[bool, str]:
    """增强的参数验证"""
    errors = []
    
    # 检查输入文件
    if not os.path.exists(args.input):
        errors.append(f"输入文件 '{args.input}' 不存在")
    elif not os.path.isfile(args.input):
        errors.append(f"'{args.input}' 不是文件")
    elif not os.access(args.input, os.R_OK):
        errors.append(f"输入文件 '{args.input}' 不可读")
    
    # 检查输出目录
    output_dir = os.path.dirname(os.path.abspath(args.output)) or '.'
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            errors.append(f"无法创建输出目录 '{output_dir}': {e}")
    elif not os.access(output_dir, os.W_OK):
        errors.append(f"输出目录 '{output_dir}' 不可写")
    
    # 检查参数逻辑
    if args.rename_group and not args.groups:
        errors.append("-rg/--rename-group 参数需要配合 -gr/--groups 使用")
    
    if args.rename and not (args.channels and args.keywords):
        errors.append("-rn/--rename 参数需要同时配合 -ch 和 -k 使用")
    
    # 检查文件扩展名（警告）
    if args.input and not args.input.lower().endswith(('.m3u', '.m3u8')):
        debug_log(f"输入文件 '{args.input}' 可能不是标准M3U文件", 'warn')
    
    if errors:
        return False, "\n".join(errors)
    return True, ""

# ==================== 原有函数（添加调试输出） ====================
def parse_extinf_group(extinf_line: str) -> Optional[str]:
    """从EXTINF行解析group-title属性"""
    debug_log(f"解析EXTINF行: {extinf_line[:100]}...", 'debug')
    
    # 查找 group-title="..." 模式
    group_match = re.search(r'group-title="([^"]*)"', extinf_line)
    if group_match:
        result = group_match.group(1)
        debug_log(f"从group-title属性解析到组名: {result}", 'debug')
        return result
    
    # 也可以尝试查找 group-title='...' 单引号模式
    group_match = re.search(r"group-title='([^']*)'", extinf_line)
    if group_match:
        result = group_match.group(1)
        debug_log(f"从group-title属性(单引号)解析到组名: {result}", 'debug')
        return result
    
    debug_log("EXTINF行中没有找到group-title属性", 'debug')
    return None

def update_extinf_group(extinf_line: str, new_group_name: str) -> str:
    """更新EXTINF行中的group-title属性"""
    debug_log(f"更新组名: '{extinf_line[:50]}...' -> '{new_group_name}'", 'debug')
    
    # 如果已有group-title属性，替换它
    if 'group-title="' in extinf_line:
        updated_line = re.sub(r'group-title="[^"]*"', f'group-title="{new_group_name}"', extinf_line)
    elif "group-title='" in extinf_line:
        updated_line = re.sub(r"group-title='[^']*'", f"group-title='{new_group_name}'", extinf_line)
    else:
        # 如果没有group-title属性，需要添加
        if ',' in extinf_line:
            parts = extinf_line.rsplit(',', 1)
            attributes = parts[0]
            channel_name = parts[1]
            if attributes.endswith('"'):
                updated_line = f'{attributes} group-title="{new_group_name}",{channel_name}'
            else:
                updated_line = f'{attributes} group-title="{new_group_name}",{channel_name}'
        else:
            debug_log(f"无法更新组名，EXTINF格式异常: {extinf_line}", 'warn')
            return extinf_line
    
    debug_log(f"更新后的行: {updated_line[:100]}...", 'debug')
    return updated_line

def parse_m3u_file(lines: List[str]) -> Tuple[List[Dict], List[str]]:
    """解析M3U文件，支持多种格式"""
    debug_log(f"开始解析M3U文件，共 {len(lines)} 行", 'info')
    
    channels_data = []
    header_lines = []
    
    current_inf = None
    current_urls = []
    current_group = None
    current_extgrp = None
    channel_count = 0
    line_num = 0
    
    i = 0
    while i < len(lines):
        line_num += 1
        line = lines[i].strip()
        
        if not line:
            i += 1
            continue
        
        debug_log(f"行 {line_num}: 处理 '{line[:50]}...'", 'debug')
        
        # 处理文件头
        if i == 0 and (line.startswith('#EXTM3U') or line.startswith('#PLAYLIST')):
            header_lines.append(line)
            debug_log(f"行 {line_num}: 识别为文件头", 'debug')
            i += 1
            continue
        
        # 处理其他可能的头部注释
        if i < 3 and line.startswith('#'):
            if not line.startswith('#EXTINF') and not line.startswith('#EXTGRP'):
                header_lines.append(line)
                debug_log(f"行 {line_num}: 识别为头部注释", 'debug')
                i += 1
                continue
        
        # 处理EXTGRP标签
        if line.startswith('#EXTGRP:'):
            current_extgrp = line
            current_group = line.replace('#EXTGRP:', '').strip()
            debug_log(f"行 {line_num}: 识别为EXTGRP标签，组名: {current_group}", 'debug')
            i += 1
            continue
        
        # 处理EXTINF行
        if line.startswith('#EXTINF'):
            # 保存上一个频道
            if current_inf:
                group = current_group
                if group is None:
                    group = parse_extinf_group(current_inf)
                
                channels_data.append({
                    "inf": current_inf, 
                    "urls": current_urls,
                    "group": group,
                    "extgrp_line": current_extgrp
                })
                channel_count += 1
                debug_log(f"完成解析频道 {channel_count}: 组名='{group}', URL数量={len(current_urls)}", 'debug')
            
            # 开始新频道
            current_inf = line
            current_urls = []
            current_group = parse_extinf_group(line)
            current_extgrp = None
            debug_log(f"行 {line_num}: 识别为新频道开始", 'debug')
            i += 1
            continue
        
        # 处理URL行
        if not line.startswith('#'):
            current_urls.append(line)
            debug_log(f"行 {line_num}: 识别为URL ({len(current_urls)})", 'debug')
            i += 1
            continue
        
        # 其他注释行
        debug_log(f"行 {line_num}: 跳过注释行", 'debug')
        i += 1
    
    # 保存最后一个频道
    if current_inf:
        group = current_group
        if group is None:
            group = parse_extinf_group(current_inf)
        
        channels_data.append({
            "inf": current_inf, 
            "urls": current_urls,
            "group": group,
            "extgrp_line": current_extgrp
        })
        channel_count += 1
        debug_log(f"完成解析最后一个频道: 组名='{group}', URL数量={len(current_urls)}", 'debug')
    
    debug_log(f"解析完成: 共 {len(channels_data)} 个频道, {len(header_lines)} 行头部", 'info')
    
    # 调试输出频道统计
    if DEBUG_MODE:
        group_stats = {}
        for ch in channels_data:
            group = ch.get("group", "无组名")
            group_stats[group] = group_stats.get(group, 0) + 1
        
        debug_log("频道组统计:", 'debug')
        for group, count in group_stats.items():
            debug_log(f"  {group}: {count} 个频道", 'debug')
    
    return channels_data, header_lines

def sort_m3u_urls(input_file: str, output_file: str, keywords_str: str, 
                  reverse_mode: bool = False, target_channels_str: Optional[str] = None,
                  new_name: Optional[str] = None, force: bool = False,
                  group_names_str: Optional[str] = None, rename_group: Optional[str] = None,
                  group_sort: bool = False) -> Tuple[List[str], int, int, int, int, int, int]:
    """处理M3U文件，支持URL排序和条件重命名"""
    
    debug_log("=" * 60, 'info')
    debug_log("开始处理M3U文件", 'info')
    debug_log(f"输入文件: {input_file}", 'info')
    debug_log(f"输出文件: {output_file}", 'info')
    debug_log(f"关键字: {keywords_str}", 'info')
    debug_log(f"目标频道: {target_channels_str}", 'info')
    debug_log(f"新频道名: {new_name}", 'info')
    debug_log(f"目标组: {group_names_str}", 'info')
    debug_log(f"新组名: {rename_group}", 'info')
    debug_log(f"反向模式: {reverse_mode}", 'info')
    debug_log(f"组排序: {group_sort}", 'info')
    debug_log(f"强制覆盖: {force}", 'info')
    debug_log("=" * 60, 'info')
    
    # 1. 参数解析与标准化
    keywords = [k.strip() for k in keywords_str.split(',') if k.strip()]
    target_channels = [c.strip() for c in target_channels_str.split(',') if c.strip()] if target_channels_str else None
    group_names = [g.strip() for g in group_names_str.split(',') if g.strip()] if group_names_str else None
    
    debug_log(f"解析后的关键字列表: {keywords}", 'debug')
    debug_log(f"解析后的目标频道列表: {target_channels}", 'debug')
    debug_log(f"解析后的目标组列表: {group_names}", 'debug')
    
    # 检查是否进入重命名模式
    rename_mode = bool(new_name or rename_group)
    debug_log(f"重命名模式: {rename_mode}", 'info')
    
    try:
        debug_log(f"正在读取文件: {input_file}", 'info')
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        debug_log(f"读取成功，共 {len(lines)} 行", 'info')
    except Exception as e:
        log_exception(e, "读取输入文件")
        return None, 0, 0, 0, 0, 0, 0
    
    # 2. 结构化解析
    try:
        channels_data, header_lines = parse_m3u_file([line.rstrip('\n') for line in lines])
        debug_log(f"解析出 {len(channels_data)} 个频道", 'info')
    except Exception as e:
        log_exception(e, "解析M3U文件")
        return None, 0, 0, 0, 0, 0, 0
    
    # 排序得分函数
    def get_url_sort_score(item: str) -> int:
        if "://" not in item: 
            return 9999
        
        for index, kw in enumerate(keywords):
            if kw.lower() in item.lower():
                score = (index + 1) if reverse_mode else (index - len(keywords))
                debug_log(f"URL '{item[:50]}...' 匹配关键字 '{kw}'，得分: {score}", 'debug')
                return score
        return 0

    # 频道组排序得分函数
    def get_group_sort_score(channel_data: Dict) -> int:
        ch_group = channel_data.get("group", "")
        
        if group_names:
            for index, group_kw in enumerate(group_names):
                if group_kw.lower() in ch_group.lower():
                    score = index - len(group_names)
                    debug_log(f"组 '{ch_group}' 匹配关键字 '{group_kw}'，得分: {score}", 'debug')
                    return score
        return 0

    # 重命名频道函数
    def rename_inf(inf_line: str, name: str) -> str:
        debug_log(f"重命名频道: '{inf_line[:50]}...' -> '{name}'", 'debug')
        
        if 'tvg-name="' in inf_line:
            inf_line = re.sub(r'tvg-name="[^"]*"', f'tvg-name="{name}"', inf_line)
        elif "tvg-name='" in inf_line:
            inf_line = re.sub(r"tvg-name='[^']*'", f"tvg-name='{name}'", inf_line)
        
        if ',' in inf_line:
            parts = inf_line.rsplit(',', 1)
            return f"{parts[0]},{name}"
        return f"{inf_line},{name}"

    # 3. 生成输出内容
    output_lines = []
    rename_count = 0
    sort_count = 0
    group_rename_count = 0
    group_sort_count = 0
    group_rename_with_k_count = 0
    
    # 添加文件头
    output_lines.extend(header_lines)
    debug_log(f"添加了 {len(header_lines)} 行头部信息", 'debug')
    
    # 如果需要组间排序
    if group_sort and group_names and not rename_mode:
        debug_log("执行组间排序", 'info')
        channels_data.sort(key=get_group_sort_score)
        group_sort_count = 1
    
    # 处理每个频道
    processed_groups = set()
    last_group = None
    processed_channel_count = 0
    
    debug_log(f"开始处理 {len(channels_data)} 个频道", 'info')
    
    for idx, ch in enumerate(channels_data):
        processed_channel_count += 1
        ch_group = ch.get("group", "")
        extgrp_line = ch.get("extgrp_line")
        
        debug_log(f"处理频道 {idx+1}/{len(channels_data)}: 组='{ch_group}'", 'debug')
        
        # 条件匹配
        name_match = any(tc.lower() in ch["inf"].lower() for tc in target_channels) if target_channels else False
        url_match_for_rename = any(any(kw.lower() in url.lower() for kw in keywords) for url in ch["urls"])
        group_match = any(gn.lower() in ch_group.lower() for gn in group_names) if group_names else True
        
        debug_log(f"  频道名匹配: {name_match}, URL匹配: {url_match_for_rename}, 组匹配: {group_match}", 'debug')
        
        # 判断是否需要处理当前频道
        should_process = True
        if group_names and not group_match:
            should_process = not group_sort or (group_sort and not rename_mode)
        
        # 输出EXTGRP行
        if ch_group and ch_group != last_group:
            debug_log(f"  组变化: '{last_group}' -> '{ch_group}'", 'debug')
            
            if rename_mode and rename_group and group_match:
                should_rename_this_group = False
                
                if not keywords and not target_channels:
                    should_rename_this_group = True
                elif keywords and not target_channels and url_match_for_rename:
                    should_rename_this_group = True
                elif not keywords and target_channels and name_match:
                    should_rename_this_group = True
                elif keywords and target_channels and name_match and url_match_for_rename:
                    should_rename_this_group = True
                
                if should_rename_this_group:
                    output_lines.append(f"#EXTGRP:{rename_group}")
                    if ch_group not in processed_groups:
                        group_rename_count += 1
                        processed_groups.add(ch_group)
                        if keywords:
                            group_rename_with_k_count += 1
                    last_group = ch_group
                    debug_log(f"  重命名EXTGRP行: '{ch_group}' -> '{rename_group}'", 'debug')
                else:
                    if extgrp_line:
                        output_lines.append(extgrp_line)
                    last_group = ch_group
            elif not rename_mode:
                if extgrp_line:
                    output_lines.append(extgrp_line)
                last_group = ch_group
            else:
                if extgrp_line:
                    output_lines.append(extgrp_line)
                last_group = ch_group
        
        if not should_process:
            debug_log(f"  跳过处理（不匹配组条件）", 'debug')
            output_lines.append(ch["inf"])
            output_lines.extend(ch["urls"])
            continue
        
        # 初始化最终INF行
        final_inf = ch["inf"]
        channel_renamed = False
        
        # 重命名模式逻辑
        if rename_mode:
            debug_log("  执行重命名模式逻辑", 'debug')
            
            # 频道重命名
            if new_name and target_channels and keywords:
                if name_match and url_match_for_rename:
                    final_inf = rename_inf(ch["inf"], new_name)
                    rename_count += 1
                    channel_renamed = True
                    debug_log(f"  频道重命名成功，计数: {rename_count}", 'debug')
            
            # 频道组重命名（group-title属性）
            if rename_group and group_match and parse_extinf_group(final_inf):
                should_rename_group_attr = False
                
                if not keywords and not target_channels:
                    should_rename_group_attr = True
                elif keywords and not target_channels and url_match_for_rename:
                    should_rename_group_attr = True
                elif not keywords and target_channels and name_match:
                    should_rename_group_attr = True
                elif keywords and target_channels and name_match and url_match_for_rename:
                    should_rename_group_attr = True
                
                if should_rename_group_attr:
                    final_inf = update_extinf_group(final_inf, rename_group)
                    if ch_group not in processed_groups:
                        group_rename_count += 1
                        processed_groups.add(ch_group)
                        if keywords:
                            group_rename_with_k_count += 1
                    debug_log(f"  组属性重命名成功，计数: {group_rename_count}", 'debug')
            
            # 重命名模式下：先输出EXTINF行，再输出URLs
            output_lines.append(final_inf)
            output_lines.extend(ch["urls"])
            
        # 排序模式逻辑
        else:
            debug_log("  执行排序模式逻辑", 'debug')
            should_sort_urls = False
            
            if group_sort:
                should_sort_urls = group_match and len(ch["urls"]) > 1
            else:
                if target_channels:
                    should_sort_urls = name_match and group_match
                elif group_names:
                    should_sort_urls = group_match
                else:
                    should_sort_urls = True
            
            # 排序模式下：先输出EXTINF行
            output_lines.append(final_inf)
            
            # 然后输出URLs（可能排序）
            if should_sort_urls and len(ch["urls"]) > 1:
                sorted_list = sorted(ch["urls"], key=get_url_sort_score)
                output_lines.extend(sorted_list)
                if sorted_list != ch["urls"]:
                    sort_count += 1
                    debug_log(f"  URL排序成功，排序变化计数: {sort_count}", 'debug')
            else:
                output_lines.extend(ch["urls"])
    
    debug_log(f"处理完成: 重命名 {rename_count} 个频道, 排序 {sort_count} 个频道", 'info')
    debug_log(f"组重命名: {group_rename_count} 个频道组", 'info')
    
    return output_lines, rename_count, sort_count, len(channels_data), group_rename_count, group_sort_count, group_rename_with_k_count

def safe_write_output(lines: List[str], input_path: str, output_path: str) -> Tuple[bool, Optional[str]]:
    """安全地写入输出文件"""
    debug_log(f"安全写入输出文件: {output_path}", 'info')
    debug_log(f"输入路径: {input_path}", 'debug')
    
    input_abs = os.path.abspath(input_path)
    output_abs = os.path.abspath(output_path)
    is_same_file = input_abs == output_abs
    
    debug_log(f"是否为同一文件: {is_same_file}", 'debug')
    
    temp_path = None
    
    try:
        if is_same_file:
            output_dir = os.path.dirname(output_path) or '.'
            fd, temp_path = tempfile.mkstemp(
                dir=output_dir,
                suffix='.m3u',
                prefix='.tmp_',
                text=True
            )
            debug_log(f"创建临时文件: {temp_path}", 'debug')
            
            out_f = os.fdopen(fd, 'w', encoding='utf-8')
        else:
            out_f = open(output_path, 'w', encoding='utf-8')
            debug_log(f"直接打开输出文件: {output_path}", 'debug')
        
        with out_f:
            for line in lines:
                out_f.write(line + '\n')
        
        debug_log(f"写入完成，共 {len(lines)} 行", 'info')
        
        if is_same_file:
            try:
                os.replace(temp_path, output_path)
                temp_path = None
                debug_log("原子替换原文件成功", 'info')
            except Exception as e:
                debug_log(f"原子替换失败，使用备选方案: {e}", 'warn')
                shutil.move(temp_path, output_path)
                temp_path = None
                debug_log("移动临时文件成功", 'info')
        
        return True, None
        
    except Exception as e:
        log_exception(e, "写入输出文件")
        return False, temp_path

def cleanup_temp_file(temp_path: Optional[str]) -> None:
    """清理临时文件"""
    if temp_path and os.path.exists(temp_path):
        try:
            os.unlink(temp_path)
            debug_log(f"已清理临时文件: {temp_path}", 'info')
        except Exception as e:
            debug_log(f"无法删除临时文件 {temp_path}: {e}", 'warn')

def main():
    """主函数，添加详细的错误处理"""
    debug_log("脚本启动", 'info')
    debug_log(f"命令行参数: {sys.argv}", 'debug')
    
    try:
        parser = argparse.ArgumentParser(
            description="M3U URL排序与条件重命名工具",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
🚀 调试选项:
  设置环境变量 DEBUG=true 启用调试模式
  设置环境变量 LOG_LEVEL=debug|info|warn|error 控制日志级别
  
  示例:
    DEBUG=true python script.py -i input.m3u -k "test"
    LOG_LEVEL=debug python script.py -i input.m3u -k "test"

🎯 基本用法:
  重命名模式:
    %(prog)s -i input.m3u -k "keyword" -ch "channel" -rn "new_name"
  
  排序模式:
    %(prog)s -i input.m3u -k "keyword1,keyword2" -r
            """
        )
        
        # 基础参数
        parser.add_argument("-i", "--input", required=True, help="输入M3U文件路径")
        parser.add_argument("-o", "--output", default="sorted_output.m3u", help="输出文件路径")
        parser.add_argument("-k", "--keywords", default="", help="URL关键字，逗号分隔")
        parser.add_argument("-r", "--reverse", action="store_true", help="开启反向模式")
        
        # 频道相关参数
        parser.add_argument("-ch", "--channels", help="目标频道名关键字，逗号分隔")
        parser.add_argument("-rn", "--rename", help="重命名频道名（需同时满足 -ch 和 -k 条件）")
        
        # 频道组相关参数
        parser.add_argument("-gr", "--groups", help="目标频道组名关键字，逗号分隔")
        parser.add_argument("-rg", "--rename-group", help="重命名频道组名")
        parser.add_argument("-gs", "--group-sort", action="store_true", help="对频道组进行排序")
        
        parser.add_argument("--force", action="store_true", help="强制覆盖输出文件")
        
        # 添加调试参数
        parser.add_argument("--debug", action="store_true", help="启用调试模式")
        parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
        
        args = parser.parse_args()
        
        # 处理调试参数
        if args.debug:
            global DEBUG_MODE
            DEBUG_MODE = True
            debug_log("通过 --debug 参数启用调试模式", 'info')
        
        if args.verbose:
            global LOG_LEVEL
            LOG_LEVEL = 'debug'
            debug_log("通过 --verbose 参数启用详细输出", 'info')
        
        debug_log("参数解析完成", 'info')
        
        # 验证参数
        debug_log("开始验证参数", 'info')
        is_valid, error_message = validate_arguments_extended(args)
        if not is_valid:
            print("❌ 参数验证失败:")
            print(error_message)
            sys.exit(1)
        debug_log("参数验证通过", 'info')
        
        # 检查输出文件
        input_abs = os.path.abspath(args.input)
        output_abs = os.path.abspath(args.output)
        
        if os.path.exists(args.output) and input_abs != output_abs:
            if not args.force:
                print(f"❌ 错误：输出文件 '{args.output}' 已存在")
                print("   使用 --force 参数强制覆盖，或指定不同的输出文件")
                sys.exit(1)
            else:
                debug_log(f"将强制覆盖已存在的输出文件: {args.output}", 'warn')
        
        # 处理M3U文件
        debug_log("开始处理M3U文件", 'info')
        
        try:
            output_lines, rename_count, sort_count, total_channels, group_rename_count, group_sort_count, group_rename_with_k_count = sort_m3u_urls(
                args.input, args.output, args.keywords, args.reverse, 
                args.channels, args.rename, args.force,
                args.groups, args.rename_group, args.group_sort
            )
            
            if output_lines is None:
                print("❌ 处理失败：sort_m3u_urls 返回 None")
                sys.exit(1)
            
            debug_log(f"处理完成，生成 {len(output_lines)} 行输出", 'info')
            
        except Exception as e:
            log_exception(e, "处理M3U文件")
            print("❌ 处理M3U文件时发生错误，请检查输入文件格式和参数")
            if DEBUG_MODE:
                print("详细错误信息已记录")
            sys.exit(1)
        
        # 安全写入输出文件
        debug_log("开始写入输出文件", 'info')
        try:
            success, temp_path = safe_write_output(output_lines, args.input, args.output)
            
            if not success:
                cleanup_temp_file(temp_path)
                print("❌ 写入输出文件失败")
                sys.exit(1)
        except Exception as e:
            log_exception(e, "写入输出文件")
            print("❌ 写入输出文件时发生错误")
            sys.exit(1)
        
        # 输出统计信息
        print(f"\n{'='*60}")
        print("✅ 处理成功！")
        print(f"{'='*60}")
        
        if args.rename or args.rename_group:
            print(f"\n📝 重命名模式结果:")
            if args.rename:
                print(f"   频道重命名: {rename_count} 个频道已重命名为 '{args.rename}'")
            if args.rename_group:
                print(f"   频道组重命名: {group_rename_count} 个频道的组名已修改为 '{args.rename_group}'")
        else:
            print(f"\n🔄 排序模式结果:")
            if args.keywords:
                print(f"   URL排序: {sort_count} 个频道的URL已按 '{args.keywords}' 排序")
            if args.group_sort and group_sort_count:
                print(f"   组间排序: 频道组已按照 '{args.groups}' 顺序排列")
        
        print(f"\n📊 统计信息:")
        print(f"   输入文件: {args.input}")
        print(f"   输出文件: {args.output}")
        print(f"   频道总数: {total_channels} 个")
        
        if DEBUG_MODE:
            print(f"\n🔍 调试信息:")
            print(f"   处理的行数: {len(output_lines)}")
            print(f"   临时文件: {'已清理' if temp_path is None else '存在'}")
        
        if input_abs == output_abs:
            print(f"\n⚠️  注意: 已安全覆盖原文件")
        
        debug_log("脚本执行完成", 'info')
        
    except SystemExit as e:
        # 正常退出或参数错误
        debug_log(f"脚本退出，代码: {e.code}", 'info')
        raise
    except KeyboardInterrupt:
        print("\n\n❌ 用户中断执行")
        sys.exit(130)
    except Exception as e:
        log_exception(e, "主函数")
        print("\n❌ 脚本执行过程中发生未预期的错误")
        print("   请使用 --debug 参数运行以获取详细错误信息")
        sys.exit(1)

if __name__ == "__main__":
    main()
