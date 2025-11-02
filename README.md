# 豆瓣电影 Top 100 爬虫

该项目提供一个简单的命令行脚本，用于抓取豆瓣电影 Top 100 数据，并存储到 SQLite 数据库中。数据库结构围绕可视化需求设计，方便统计评分、年份、地区、类型等多维度信息。

## 快速开始

### 1. 创建并激活虚拟环境（可选）
```bash
python -m venv .venv
source .venv/bin/activate  # Windows 使用 .venv\Scripts\activate
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 运行爬虫
```bash
python src/douban_top100.py --database data/douban_top100.sqlite3
```

参数说明：
- `--database/-d`：SQLite 数据库路径，默认 `data/douban_top100.sqlite3`。
- `--delay`：每次请求之间的延时（秒），默认 `0.5`，以降低被封禁的风险。
- `--log-level`：日志等级，默认 `INFO`。

脚本会自动创建数据库及以下数据表：
- `movies`：基础电影信息（排名、标题、评分、年份等）。
- `movie_regions`：电影与出品地区的多对多关系。
- `movie_genres`：电影与类型的多对多关系。
- `movie_directors`：电影与导演的多对多关系。
- `movie_actors`：电影与主演的多对多关系。

该设计便于后续在可视化服务中构建地区、类型、年份分布以及导演/演员维度的图表。
