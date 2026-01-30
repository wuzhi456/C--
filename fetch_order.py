import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from pathlib import Path

def _clean_text(cell):
    text = cell.get_text(" ", strip=True) if cell else ""
    return re.sub(r'\[.*?\]', '', text).strip()


def _parse_couple(cell):
    if cell is None:
        return "", "", ""
    parts = [re.sub(r'\[.*?\]', '', text).strip() for text in cell.stripped_strings]
    parts = [p for p in parts if p]
    if len(parts) >= 2:
        return parts[0], parts[1], " / ".join(parts)
    if parts:
        return parts[0], "", parts[0]
    return "", "", ""


def _parse_bottom_two_status(cell, row):
    status_text = _clean_text(cell).lower() if cell else ""
    if status_text:
        if "eliminated" in status_text or "elimination" in status_text:
            return "Eliminated"
        if "withdrew" in status_text or "withdrawn" in status_text or "quit" in status_text:
            return "Withdrew"
        if "disqualified" in status_text:
            return "Disqualified"
        if "bottom" in status_text or "btm" in status_text:
            return "Bottom Three" if "three" in status_text or "3" in status_text else "Bottom Two"
        if "low" in status_text or "danger" in status_text:
            return "Bottom Two"
        if "safe" in status_text or "saved" in status_text:
            return "Safe"
        return status_text.title()

    classes = set((cell.get("class") or []))
    row_classes = set((row.get("class") or []))
    combined = classes.union(row_classes)
    if any("bottom2" in c or "btm2" in c for c in combined):
        return "Bottom Two"
    if any("bottom3" in c or "btm3" in c for c in combined):
        return "Bottom Three"
    if any("elim" in c or "eliminated" in c for c in combined):
        return "Eliminated"
    return ""


def scrape_dwts_weekly_details(season_num):
    """
    抓取指定赛季维基百科页面中的周次信息：
    Running_Order, Celebrity, Ballroom_Partner, Couple, Dance_Style, Weekly_Bottom_Two_Status
    """
    url = f"https://en.wikipedia.org/wiki/Dancing_with_the_Stars_(American_season_{season_num})"
    headers = {
        'User-Agent': 'Mozilla/5.0 (MCM_Research_Bot; mailto:your_email@example.com)'
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
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
            heading = table.find_previous(['h2', 'h3'])
            header_text = heading.text.lower() if heading else ""

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

            header_cells = rows[0].find_all(['th', 'td']) if rows else []
            headers = [cell.get_text(strip=True) for cell in header_cells]

            # 寻找出场顺序（Order）、选手名、舞蹈种类与结果列索引
            order_idx = -1
            couple_idx = -1
            dance_idx = -1
            result_idx = -1
            for idx, h in enumerate(headers):
                lower_h = h.lower()
                if 'order' in lower_h:
                    order_idx = idx
                if 'couple' in lower_h or 'pair' in lower_h or 'contestant' in lower_h:
                    couple_idx = idx
                if 'dance' in lower_h or 'style' in lower_h:
                    dance_idx = idx
                if 'result' in lower_h or 'status' in lower_h:
                    result_idx = idx

            if order_idx != -1 and couple_idx != -1:
                for row in rows[1:]:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) <= max(order_idx, couple_idx):
                        continue
                    order = _clean_text(cols[order_idx])
                    if not order or order.lower() in {"order", "running order"}:
                        continue
                    celebrity, partner, couple = _parse_couple(cols[couple_idx])
                    dance_style = _clean_text(cols[dance_idx]) if dance_idx != -1 and len(cols) > dance_idx else ""
                    result_cell = cols[result_idx] if result_idx != -1 and len(cols) > result_idx else None
                    bottom_status = _parse_bottom_two_status(result_cell, row)

                    all_data.append({
                        'Season': season_num,
                        'Week': current_week,
                        'Running_Order': order,
                        'Celebrity': celebrity,
                        'Ballroom_Partner': partner,
                        'Couple': couple,
                        'Dance_Style': dance_style,
                        'Weekly_Bottom_Two_Status': bottom_status
                    })

        if not all_data:
            print(f"Season {season_num} 未找到包含出场顺序的周次表格。")
        return all_data

    except Exception as e:
        print(f"解析 Season {season_num} 时出错: {e}")
        return None

def _load_target_seasons(data_path):
    if not data_path.exists():
        return [2, 4, 11, 27, 28, 31, 32, 33, 34]
    try:
        df = pd.read_csv(data_path)
        seasons = pd.to_numeric(df['season'], errors='coerce').dropna().astype(int).unique()
        return sorted(seasons.tolist())
    except Exception as exc:
        print(f"读取赛季列表失败，使用默认赛季：{exc}")
        return [2, 4, 11, 27, 28, 31, 32, 33, 34]


if __name__ == "__main__":
    # --- 执行批量抓取 ---
    repo_root = Path(__file__).resolve().parent
    data_path = repo_root / "2026_MCM_Problem_C_Data.csv"
    all_seasons_order = []
    target_seasons = _load_target_seasons(data_path)

    for s in target_seasons:
        print(f"正在抓取 Season {s} 的周次明细...")
        data = scrape_dwts_weekly_details(s)
        if data:
            all_seasons_order.extend(data)
        time.sleep(2)  # 礼貌抓取

    # 转换为 DataFrame 并导出
    df_order = pd.DataFrame(all_seasons_order)
    df_order.to_csv("dwts_weekly_details.csv", index=False)
    print("✅ 数据已保存至 dwts_weekly_details.csv")
