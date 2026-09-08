# Journal Classification API

统一期刊分区查询服务

## 功能

- **社会科学**: 本地 `journal_data.json` 数据查询
- **自然科学**: 分众表 API 在线查询
- **REST API**: `GET /journals` 查询接口
- **MCP Tool**: `query_journals_tool` 工具

## 启动

```bash
# 安装依赖
uv sync

# 启动服务
uv run python journal_api.py
```

## REST API

### 服务状态
```bash
GET /status
```

### 期刊查询
```bash
GET /journals?journal_type=social&name=心理学&year=2022&limit=10
GET /journals?journal_type=science&name=Nature&year=2023
```

参数:
- `journal_type` (必填): `science` (自然科学) / `social` (社会科学)
- `name` (可选): 期刊名称关键词（模糊匹配）
- `year` (可选): 年份（精确匹配）
- `limit` (可选): 返回数量限制，默认 100

## MCP Tool

MCP 端点: `/sse/mcp` (SSE 传输)

### 使用 MCP 客户端连接

```json
{
  "mcpServers": {
    "journal-classification": {
      "transport": "sse",
      "url": "http://localhost:8000/sse/mcp"
    }
  }
}
```

### 工具调用
```python
# MCP Tool: query_journals_tool
{
  "journal_type": "social",  # 必填: "science" 或 "social"
  "name": "心理学",           # 可选: 期刊名称关键词
  "year": 2022,              # 可选: 年份
  "limit": 100               # 可选: 返回数量
}
```

## 环境变量

复制 `.env.example` 为 `.env` 并配置:

```env
HOST=0.0.0.0
PORT=8000
FENQUBIAO_API_V2=http://webapi.fenqubiao.com/api/v2/user
FENQUBIAO_API_V1=http://webapi.fenqubiao.com/api/user
FENQUBIAO_USER=your_username
FENQUBIAO_PASSWORD=your_password
```