import subprocess
import os

def convert_silk_to_mp3(input_file, output_file):
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"The input file {input_file} does not exist.")
    
    # 构建ffmpeg命令
    command = [
        'ffmpeg',
        '-i', input_file,  # 输入文件
        '-codec:a', 'libmp3lame',  # 使用MP3编码器
        '-q:a', '4',  # 设置MP3质量（范围0-9，4是默认值，质量较高）
        output_file  # 输出文件
    ]
    
    # 调用ffmpeg命令
    try:
        subprocess.run(command, check=True)
        print(f"Successfully converted {input_file} to {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred during conversion: {e}")

# 示例用法
input_silk_file = r'C:\tt\mumu\test\618ec4995f5c6a7c322083a5c064aa4e.silk'
output_mp3_file = r'C:\tt\mumu\test\numbers.json'
convert_silk_to_mp3(input_silk_file, output_mp3_file)