import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

def scrape_dwts_running_order(season_num):
    """
    抓取指定赛季维基百科页面中的出场顺序数据
    """
    url = f"https://en.wikipedia.org/wiki/Dancing_with_the_Stars_(American_season_{season_num})"
    headers = {
        'User-Agent': 'Mozilla/5.0 (MCM_Research_Bot; mailto:your_email@example.com)'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Season {season_num} 页面请求失败")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 寻找“Weekly scores and songs”或类似的标题
        all_data = []
        
        # 维基百科的周次数据通常存储在具有 'wikitable' 类的表格中
        # 我们寻找包含 "Order" 或 "Running order" 列头的表格
        tables = soup.find_all('table', {'class': 'wikitable'})
        
        current_week = 0
        for table in tables:
            rows = table.find_all('tr')
            header_text = table.find_previous(['h2', 'h3']).text.lower()
            
            # 过滤掉非周次评分表的表格
            if 'week' not in header_text and 'scores' not in header_text:
                continue
            
            # 提取周次（例如从 "Week 1: Spring 2024" 中提取 1）
            week_match = re.search(r'week\s+(\d+)', header_text)
            if week_match:
                current_week = week_match.group(1)
            else:
                # 如果 h3 没写，可能是连续的表格，累加周次
                current_week += 1

            headers = [th.text.strip() for th in rows[0].find_all(['th', 'td'])]
            
            # 寻找出场顺序（Order）和选手名（Couple/Contestant）所在的列索引
            order_idx = -1
            couple_idx = -1
            for idx, h in enumerate(headers):
                if 'order' in h.lower(): order_idx = idx
                if 'couple' in h.lower() or 'pair' in h.lower(): couple_idx = idx
            
            if order_idx != -1 and couple_idx != -1:
                for row in rows[1:]:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) > max(order_idx, couple_idx):
                        order = cols[order_idx].text.strip()
                        couple = cols[couple_idx].text.strip()
                        # 去除 Couple 名字中的引用标志如 [1] 或注释
                        couple = re.sub(r'\[.*\]', '', couple).strip()
                        
                        all_data.append({
                            'Season': season_num,
                            'Week': current_week,
                            'Running_Order': order,
                            'Couple': couple
                        })
        
        return all_data

    except Exception as e:
        print(f"解析 Season {season_num} 时出错: {e}")
        return None

# --- 执行批量抓取 ---
all_seasons_order = []
# 建议先跑一个赛季测试，比如题目重点提到的 Season 27
target_seasons = [2, 4, 11, 27, 28, 31, 32, 33, 34] 

for s in target_seasons:
    print(f"正在抓取 Season {s} 的出场顺序...")
    data = scrape_dwts_running_order(s)
    if data:
        all_seasons_order.extend(data)
    time.sleep(2) # 礼貌抓取

# 转换为 DataFrame 并导出
df_order = pd.DataFrame(all_seasons_order)
df_order.to_csv("dwts_running_order.csv", index=False)
print("✅ 数据已保存至 dwts_running_order.csv")