"""
统一期刊分区查询服务
- 社会科学：本地 journal_data.json
- 自然科学：分众表 API
- REST API + MCP Tool
"""

import json
import os
from pathlib import Path
from typing import Optional, List, Dict, Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastmcp import FastMCP

# 加载环境变量
load_dotenv()

# ============== 配置 ==============

DATA_FILE = Path(__file__).parent / "journal_data.json"

FENQUBIAO_API_V2 = os.getenv("FENQUBIAO_API_V2", "http://webapi.fenqubiao.com/api/v2/user")
FENQUBIAO_API_V1 = os.getenv("FENQUBIAO_API_V1", "http://webapi.fenqubiao.com/api/user")
FENQUBIAO_USER = os.getenv("FENQUBIAO_USER", "")
FENQUBIAO_PASSWORD = os.getenv("FENQUBIAO_PASSWORD", "")

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# ============== 数据加载 ==============

def load_social_journals() -> List[Dict[str, Any]]:
    """加载社会科学期刊数据"""
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    return [
        {
            "name": item[0],
            "year": item[1],
            "source": item[2],
            "classification": item[3]
        }
        for item in raw_data
    ]

_social_journals: Optional[List[Dict[str, Any]]] = None

def get_social_journals() -> List[Dict[str, Any]]:
    global _social_journals
    if _social_journals is None:
        _social_journals = load_social_journals()
    return _social_journals

# ============== 自然科学 API 查询 ==============

async def query_science_api(keyword: str, year: int = 2021) -> List[Dict[str, Any]]:
    """查询自然科学期刊（分众表API）"""
    api_base = FENQUBIAO_API_V2 if year >= 2021 else FENQUBIAO_API_V1
    url = f"{api_base}/search?year={year}&keyword={keyword}&user={FENQUBIAO_USER}&password={FENQUBIAO_PASSWORD}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()

    if not isinstance(data, list):
        return []

    return [{"name": j.get("Title", ""), "abbr": j.get("AbbrTitle", ""), "issn": j.get("ISSN", "")}
            for j in data if j.get("Title")]

async def get_science_detail(journal_name: str, year: int = 2021) -> Optional[Dict[str, Any]]:
    """获取自然科学期刊详细信息"""
    api_base = FENQUBIAO_API_V2 if year >= 2021 else FENQUBIAO_API_V1
    url = f"{api_base}/get?year={year}&keyword={journal_name}&user={FENQUBIAO_USER}&password={FENQUBIAO_PASSWORD}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()

    if not data or not data.get("Title"):
        return None

    return {
        "name": data.get("Title"),
        "abbr": data.get("AbbrTitle"),
        "issn": data.get("ISSN"),
        "year": year,
        "zky": data.get("ZKY", []),
        "jcr": data.get("JCR", [])
    }

# ============== 社会科学本地查询 ==============

def query_social_local(name: Optional[str] = None, year: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """查询社会科学期刊（本地数据）"""
    journals = get_social_journals()

    if year is not None:
        journals = [j for j in journals if j["year"] == year]

    if name:
        name_lower = name.lower()
        journals = [j for j in journals if name_lower in j["name"].lower()]

    return journals[:limit]

# ============== 统一查询接口 ==============

async def query_journals(
    journal_type: str,
    name: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = 100
) -> Dict[str, Any]:
    """
    统一期刊查询

    Args:
        journal_type: "science" (自然科学) / "social" (社会科学)
        name: 期刊名称（模糊匹配）
        year: 年份（精确匹配）
        limit: 返回数量限制

    Returns:
        {"total": int, "results": [...]}
    """
    if journal_type == "social":
        results = query_social_local(name=name, year=year, limit=limit)
        return {"total": len(results), "results": results}

    elif journal_type == "science":
        if not name:
            return {"total": 0, "results": [], "error": "name is required for science journals"}

        name_list = await query_science_api(keyword=name, year=year or 2021)
        if not name_list:
            return {"total": 0, "results": []}

        results = []
        for j in name_list[:limit]:
            detail = await get_science_detail(j["name"], year or 2021)
            if detail:
                results.append(detail)

        return {"total": len(results), "results": results}

    return {"total": 0, "results": [], "error": "Invalid journal_type. Use 'science' or 'social'"}

# ============== MCP Server ==============

mcp = FastMCP("Journal Classification")

@mcp.tool()
async def query_journals_tool(
    journal_type: str,
    name: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = 100
) -> Dict[str, Any]:
    """
    期刊分区查询

    Args:
        journal_type: 期刊类型 - "science" (自然科学) 或 "social" (社会科学)
        name: 期刊名称关键词（模糊匹配）
        year: 年份（精确匹配）
        limit: 返回数量限制 (默认100)
    """
    return await query_journals(journal_type, name, year, limit)

# 创建 MCP HTTP 应用 (使用 stateless streamable-http 传输)
mcp_app = mcp.http_app(path="/mcp", transport="streamable-http", stateless_http=True)

# ============== FastAPI 应用 ==============

app = FastAPI(
    title="Journal Classification API",
    description="统一期刊分区查询 - 自然科学(API) + 社会科学(本地)",
    version="1.0.0",
    lifespan=mcp_app.lifespan
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# 挂载 MCP 应用 (MCP endpoint at /ai/mcp)
app.mount("/ai", mcp_app)

# REST API
class QueryResponse(BaseModel):
    total: int
    results: List[Dict[str, Any]]

@app.get("/status", tags=["Status"])
async def get_status():
    """服务状态"""
    return {
        "status": "healthy",
        "service": "journal-classification-api",
        "version": "1.0.0",
        "host": HOST,
        "port": PORT,
        "data_counts": {
            "social_sciences": len(get_social_journals()),
            "natural_sciences": "via API"
        }
    }

@app.get("/journals", response_model=QueryResponse, tags=["Journals"])
async def query_journals_api(
    journal_type: str = Query(..., pattern="^(science|social)$", description="期刊类型"),
    name: Optional[str] = Query(None, description="期刊名称（模糊匹配）"),
    year: Optional[int] = Query(None, description="年份（精确匹配）"),
    limit: int = Query(100, ge=1, le=500, description="返回数量")
):
    """统一期刊分区查询"""
    result = await query_journals(journal_type, name, year, limit)
    return QueryResponse(total=result["total"], results=result.get("results", []))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)