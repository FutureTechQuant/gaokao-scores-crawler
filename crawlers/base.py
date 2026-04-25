import requests
import json
import time
import random
from datetime import datetime

class BaseCrawler:
    def __init__(self):
        self.base_url = "https://api.zjzw.cn/web/api/"
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9",
            "content-type": "application/json",
            "origin": "https://www.gaokao.cn",
            "referer": "https://www.gaokao.cn/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.rate_limit_sleep = 3  # 增加初始延迟从1秒到3秒
    
    def make_request(self, payload, retry=3, delay=2):
        """统一的请求方法，支持限流处理"""
        for attempt in range(retry):
            try:
                response = self.session.post(
                    self.base_url,
                    json=payload,
                    timeout=15
                )
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        
                        # 检查业务错误码
                        code = result.get('code')
                        
                        # 限流错误处理
                        if code == '1069' or code == 1069:
                            message = result.get('message', '访问太过频繁')
                            print(f"⚠️  限流警告: {message}")
                            
                            # 指数退避：增加延迟时间
                            self.rate_limit_sleep = min(self.rate_limit_sleep * 2, 60)  # 最大60秒
                            print(f"   等待 {self.rate_limit_sleep:.1f} 秒后重试...")
                            time.sleep(self.rate_limit_sleep)
                            
                            # 重试当前请求
                            if attempt < retry - 1:
                                continue
                            return None
                        
                        # 成功请求，逐渐减少延迟（但不低于3秒）
                        if code == '0000' or code == 0:
                            self.rate_limit_sleep = max(self.rate_limit_sleep * 0.9, 3)
                        
                        return result
                        
                    except json.JSONDecodeError as e:
                        print(f"⚠️  JSON解析失败: {str(e)}")
                        print(f"   响应内容类型: {response.headers.get('content-type')}")
                        print(f"   响应前200字符: {response.text[:200]}")
                        return None
                else:
                    print(f"⚠️  请求失败，状态码: {response.status_code}")
                    
            except requests.exceptions.Timeout:
                print(f"⚠️  请求超时 (尝试 {attempt + 1}/{retry})")
            except requests.exceptions.RequestException as e:
                print(f"⚠️  请求出错 (尝试 {attempt + 1}/{retry}): {str(e)}")
            
            if attempt < retry - 1:
                time.sleep(delay * (attempt + 1))
        
        return None
    
    def polite_sleep(self, min_delay=3.0, max_delay=6.0):
        """随机延迟，模拟人类行为，考虑限流因素"""
        base_delay = random.uniform(min_delay, max_delay)
        # 如果有限流警告，使用更长的延迟
        total_delay = base_delay * (self.rate_limit_sleep / 3.0)
        time.sleep(min(total_delay, 20))  # 最多20秒
    
    def save_to_json(self, data, filename):
        """保存数据到JSON文件"""
        filepath = f'data/{filename}'
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'count': len(data),
                'data': data
            }, f, ensure_ascii=False, indent=2)
        print(f"✓ 数据已保存到 {filepath}")
