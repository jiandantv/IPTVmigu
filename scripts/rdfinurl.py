import requests
import re
import os
import sys
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from urllib.parse import urljoin
import argparse

def get_final_url(url, max_redirects=10, timeout=5):
    """
    获取 URL 的最终重定向地址，并在获取到响应头后检查 Content-Type。
    如果检测到视频内容（包括HLS播放列表），则中止下载响应体。
    """
    current_url = url
    redirect_count = 0

    try:
        while redirect_count < max_redirects:
            # 初始请求，allow_redirects=False 来手动处理重定向
            response = requests.get(current_url, allow_redirects=False, timeout=timeout, stream=True) # stream=True 关键
            response.raise_for_status() # 检查HTTP状态码，如果不是2xx，则抛出异常

            if response.status_code in (301, 302, 303, 307, 308) and 'Location' in response.headers:
                new_url = response.headers['Location']
                if not new_url.startswith(('http://', 'https://')):
                    new_url = urljoin(current_url, new_url)
                current_url = new_url
                redirect_count += 1
                # 在重定向时关闭当前响应的连接
                response.close()
            else:
                # 到达最终URL，或者不再重定向
                final_url = current_url
                content_type = response.headers.get('Content-Type', '').lower()
                print(f"最终URL: {final_url}")
                print(f"Content-Type: {content_type}")

                # 检查是否为视频内容或HLS播放列表
                is_video_related = False
                if 'video/' in content_type or \
                   'application/octet-stream' in content_type or \
                   'application/vnd.apple.mpegurl' in content_type or \
                   'application/x-mpegurl' in content_type or \
                   final_url.lower().endswith('.m3u8'): # 也可以根据文件扩展名判断

                    is_video_related = True
                    print(f"检测到视频相关内容 ({content_type} 或 .m3u8)，中止响应体下载。")
                    response.close() # 立即关闭连接，中止下载
                    return final_url, True, is_video_related # 返回最终URL，成功，是视频
                else:
                    print(f"检测到非视频相关内容 ({content_type})。")
                    response.close() # 如果不需要响应体内容，也可以直接关闭
                    return final_url, True, is_video_related # 返回最终URL，成功，不是视频

    except requests.exceptions.RequestException as e:
        print(f"⚠️ 请求失败: {current_url} ({type(e).__name__}: {e})")
        # 即使请求失败，也返回三个值，保持一致性
        return current_url, False, False

def resolve_urls_with_retry(urls, max_workers=10, timeout=5, max_retries=3, delay_between_retries=10):
    """
    解析URL，失败后延迟重试，最多尝试 max_retries 次
    """
    # 存储最终解析的URL和其视频相关性状态
    resolved_info = {}
    retries = 0

    while retries <= max_retries:
        print(f"\n🔄 开始第 {retries+1} 轮处理...")
        failed_urls = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务，future_to_url 映射 future 对象到原始 URL
            future_to_url = {executor.submit(get_final_url, url, 10, timeout): url for url in urls}

            for future in as_completed(future_to_url):
                original_url = future_to_url[future]
                try:
                    # 关键修改在这里：解包三个返回值
                    final_url, success, is_video_related = future.result()
                    # 存储解析后的信息
                    resolved_info[original_url] = {
                        "final_url": final_url,
                        "success": success,
                        "is_video_related": is_video_related
                    }

                    if success:
                        status = "✅ 成功"
                        if is_video_related:
                            status += " (视频相关)"
                        print(f"{status}: {final_url}")
                    else:
                        print(f"❌ 失败: {original_url}")
                        failed_urls.append(original_url)
                except Exception as exc:
                    print(f"❌ URL '{original_url}' 生成异常: {exc}")
                    failed_urls.append(original_url)
                    # 存储异常情况下的信息
                    resolved_info[original_url] = {
                        "final_url": original_url, # 失败时，final_url 可以是原始URL
                        "success": False,
                        "is_video_related": False, # 失败时，默认为非视频
                        "error": str(exc)
                    }


        if not failed_urls:
            break  # 全部成功，跳出循环
        if retries == max_retries:
            print("\n❗已达最大重试次数，以下 URL 仍处理失败：")
            for url in failed_urls:
                print(url)
            break

        print(f"\n⏳ 等待 {delay_between_retries} 秒后重新尝试 {len(failed_urls)} 个失败的请求...")
        time.sleep(delay_between_retries)
        urls = failed_urls
        retries += 1

    return resolved_info # 返回包含所有解析结果的字典

def safe_write_output(lines, input_path, output_path):
    """
    安全地写入输出文件，支持同文件覆盖
    
    :param lines: 要写入的行列表
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
            out_f.write('\n'.join(lines))
        
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

def validate_arguments(input_path, output_path):
    """
    验证命令行参数的合理性
    
    :param input_path: 输入文件路径
    :param output_path: 输出文件路径
    :return: 验证成功返回True，失败返回False
    """
    # 检查输入文件是否存在
    if not os.path.exists(input_path):
        print(f"错误：输入文件 '{input_path}' 不存在")
        return False
    
    # 检查输入文件是否可读
    if not os.access(input_path, os.R_OK):
        print(f"错误：输入文件 '{input_path}' 不可读")
        return False
    
    # 检查是否为文件
    if not os.path.isfile(input_path):
        print(f"错误：'{input_path}' 不是文件")
        return False
    
    # 检查输入文件扩展名（可选警告）
    if not input_path.lower().endswith('.m3u'):
        print(f"警告：输入文件 '{input_path}' 可能不是标准M3U文件")
    
    # 检查输出目录是否可写
    output_dir = os.path.dirname(os.path.abspath(output_path)) or '.'
    if not os.access(output_dir, os.W_OK):
        print(f"错误：输出目录 '{output_dir}' 不可写")
        return False
    
    # 检查输入输出是否为同一文件（提供信息性提示）
    input_abs = os.path.abspath(input_path)
    output_abs = os.path.abspath(output_path)
    
    if input_abs == output_abs:
        print("信息：输入和输出为同一文件，将安全覆盖原文件")
    
    return True

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

def process_m3u_file(input_file, output_file, max_workers=10, timeout=5, max_retries=3, force=False):
    """
    处理 M3U 文件，解析所有 URL，自动重试失败项
    """
    start_time = time.time()

    # 检查输出文件是否已存在且与输入不同
    input_abs = os.path.abspath(input_file)
    output_abs = os.path.abspath(output_file)
    
    if os.path.exists(output_file) and input_abs != output_abs:
        if not force:
            print(f"错误：输出文件 '{output_file}' 已存在")
            print("使用 --force 参数强制覆盖，或指定不同的输出文件")
            return False

    with open(input_file, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f.readlines()]

    url_pattern = re.compile(r'^https?://\S+')
    url_to_line_indices = {}
    urls_to_process = [] # 使用更明确的变量名

    for i, line in enumerate(lines):
        if url_pattern.match(line):
            urls_to_process.append(line)
            url_to_line_indices.setdefault(line, []).append(i)

    # 统计URL数量
    url_count = len(urls_to_process)
    if url_count == 0:
        print("未找到需要处理的URL")
        return False

    print(f"找到 {url_count} 个需要处理的URL")

    # resolved_map 现在存储的是包含 'final_url', 'success', 'is_video_related' 的字典
    resolved_map = resolve_urls_with_retry(
        urls_to_process, max_workers=max_workers, timeout=timeout, 
        max_retries=max_retries, delay_between_retries=10
    )

    # 遍历原始行，替换为最终解析的URL
    success_count = 0
    fail_count = 0
    
    for original_url, info in resolved_map.items():
        final_url = info["final_url"]
        success = info["success"]

        if success:
            for i in url_to_line_indices[original_url]:
                lines[i] = final_url
            success_count += 1
        else:
            # 如果解析失败，可以选择保留原始URL或进行其他处理
            print(f"❗ 原始 URL '{original_url}' 解析失败，保留原样。")
            # 也可以选择 lines[i] = f"#FAILED_URL_{original_url}" 来标记失败
            fail_count += 1

    # 安全写入输出文件
    write_success, temp_path = safe_write_output(lines, input_file, output_file)
    
    if not write_success:
        cleanup_temp_file(temp_path)
        return False

    total_time = time.time() - start_time
    
    print(f"\n🎉 所有任务完成，总耗时 {total_time:.2f} 秒")
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print(f"URL处理统计:")
    print(f"  - 成功: {success_count} 个")
    print(f"  - 失败: {fail_count} 个")
    print(f"  - 总计: {url_count} 个")
    
    if success_count > 0:
        print(f"  - 成功率: {success_count/url_count*100:.1f}%")
    
    if input_abs == output_abs:
        print("注意：已安全覆盖原文件")
    
    return True

def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='处理M3U文件中的URL重定向')
    parser.add_argument('--input', required=True, help='输入M3U文件路径')
    parser.add_argument('--output', required=True, help='输出M3U文件路径')
    parser.add_argument('--workers', type=int, default=5, 
                       help='最大工作线程数 (默认: 5)')
    parser.add_argument('--timeout', type=int, default=10, 
                       help='请求超时时间(秒) (默认: 10)')
    parser.add_argument('--retries', type=int, default=5, 
                       help='最大重试次数 (默认: 5)')
    parser.add_argument('--force', action='store_true',
                       help='强制覆盖输出文件（如果已存在且与输入不同）')
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    # 验证参数
    if not validate_arguments(args.input, args.output):
        sys.exit(1)
    
    success = process_m3u_file(
        input_file=args.input,
        output_file=args.output,
        max_workers=args.workers,
        timeout=args.timeout,
        max_retries=args.retries,
        force=args.force
    )
    
    if not success:
        sys.exit(1)
