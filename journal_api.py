"""
期刊分区查询服务 - DMP数据源版本
- 启动时从DMP API获取access_token
- 全量加载自然/社会科学期刊分区数据到内存
- 提供快速模糊匹配查询（期刊名称必填，年份可选）
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
from typing import Optional, List, Dict, Any, Callable
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
from fastmcp import FastMCP

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# ============== 配置 ==============

DMP_BASE_URL = os.getenv("DMP_BASE_URL", "https://dmp.ynu.edu.cn")
DMP_APP_KEY = os.getenv("DMP_APP_KEY", "")
DMP_APP_SECRET = os.getenv("DMP_APP_SECRET", "")

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# 数据库配置
DB_FILE = Path(__file__).parent / "journals.db"

# API端点
TOKEN_ENDPOINT = "/open_api/authentication/get_access_token"
NATURAL_SCIENCE_ENDPOINT = "/open_api/customization/tzrkxqkxx/full"
SOCIAL_SCIENCE_ENDPOINT = "/open_api/customization/tshkxqkxx/full"

# ============== 数据模型 ==============

class NaturalScienceJournal(BaseModel):
    """自然科学期刊"""
    name: str = Field(alias="KM")
    issn: Optional[str] = Field(None, alias="ISSN")
    year: Optional[str] = Field(None, alias="NF")
    category: Optional[str] = Field(None, alias="DLXK")  # 大类学科
    partition: Optional[str] = Field(None, alias="DLFQ")  # 大类分区
    top_journal: Optional[str] = Field(None, alias="TOPQK")  # Top期刊

    class Config:
        populate_by_name = True

class SocialScienceJournal(BaseModel):
    """社会科学期刊"""
    name: str = Field(alias="QKMC")  # 期刊名称
    year: Optional[str] = Field(None, alias="NF")
    source: Optional[str] = Field(None, alias="LY")  # 来源
    partition: Optional[str] = Field(None, alias="FQ")  # 分区

    class Config:
        populate_by_name = True

# ============== DMP API 客户端 ==============

class DMPClient:
    """DMP API客户端"""

    def __init__(self, base_url: str, app_key: str, app_secret: str):
        self.base_url = base_url.rstrip("/")
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=60.0)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def get_access_token(self) -> Optional[str]:
        """获取access_token"""
        if self.access_token and time.time() < self.token_expires_at - 300:  # 提前5分钟刷新
            logger.info("[DMP] 使用缓存的access_token")
            return self.access_token

        url = f"{self.base_url}{TOKEN_ENDPOINT}"
        params = {
            "key": self.app_key,
            "secret": self.app_secret
        }

        logger.info(f"[DMP] 请求access_token - URL: {url}")
        logger.info(f"[DMP] App Key: {self.app_key[:8]}...")  # 只显示前8位

        try:
            client = await self._get_client()
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            logger.info(f"[DMP] Token响应: code={data.get('code')}, message={data.get('message')}")

            if data.get("code") == 10000:
                result = data.get("result", {})
                self.access_token = result.get("access_token")
                expires_in = result.get("expires_in", 7200)
                self.token_expires_at = time.time() + expires_in
                logger.info(f"[DMP] 获取token成功，有效期: {expires_in}秒")
                return self.access_token
            else:
                logger.error(f"[DMP] 获取token失败: {data.get('message')}")
                return None

        except httpx.HTTPError as e:
            logger.error(f"[DMP] HTTP错误: {e}")
            return None
        except Exception as e:
            logger.error(f"[DMP] 错误: {e}")
            return None

    async def fetch_page(self, endpoint: str, page: int = 1, per_page: int = 100) -> Optional[Dict[str, Any]]:
        """获取单页数据"""
        token = await self.get_access_token()
        if not token:
            logger.error("[DMP] 无法获取access_token")
            return None

        url = f"{self.base_url}{endpoint}"
        params = {
            "access_token": token,
            "page": page,
            "per_page": per_page
        }

        try:
            client = await self._get_client()
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("code") == 10000:
                return data.get("result", {})
            else:
                logger.error(f"[DMP] 请求失败: {data.get('message')}")
                # 如果是token过期，尝试刷新
                if "token" in data.get("message", "").lower():
                    logger.info("[DMP] Token可能已过期，清除缓存")
                    self.access_token = None
                    self.token_expires_at = 0
                return None

        except httpx.HTTPError as e:
            logger.error(f"[DMP] HTTP错误: {e}")
            return None
        except Exception as e:
            logger.error(f"[DMP] 错误: {e}")
            return None

    async def fetch_all_data(
        self,
        endpoint: str,
        log_prefix: str = "[DMP]",
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> List[Dict[str, Any]]:
        """全量获取数据（分页）"""
        per_page = 2000  # 每页最多2000条

        # 先请求第一页获取元数据（total）
        logger.info(f"{log_prefix} 获取元数据（第一页）...")
        first_result = await self.fetch_page(endpoint, 1, per_page)

        if first_result is None:
            logger.error(f"{log_prefix} 获取第一页数据失败")
            return []

        # 提取元数据
        total = first_result.get("total", 0)
        # 根据 total 计算总页数
        max_page = (total + per_page - 1) // per_page if total > 0 else 1
        logger.info(f"{log_prefix} 元数据: 总计 {total} 条, 最大页数 {max_page}")

        # 从第一页开始，使用 total 计算的分页数查询
        all_data = []
        for page in range(1, max_page + 1):
            logger.info(f"{log_prefix} 请求第 {page}/{max_page} 页...")

            result = await self.fetch_page(endpoint, page, per_page)

            if result is None:
                logger.error(f"{log_prefix} 获取第 {page} 页失败，尝试重试...")
                await asyncio.sleep(2)
                result = await self.fetch_page(endpoint, page, per_page)
                if result is None:
                    logger.error(f"{log_prefix} 重试失败，停止获取")
                    break

            data_list = result.get("data", [])
            all_data.extend(data_list)

            if progress_callback:
                progress_callback(page, max_page)

            logger.info(f"{log_prefix} 第 {page}/{max_page} 页: 获取 {len(data_list)} 条，总计: {len(all_data)}/{total}")

            # 避免请求过快
            if page < max_page:
                await asyncio.sleep(0.3)

        logger.info(f"{log_prefix} 共获取 {len(all_data)} 条数据")
        return all_data

# ============== 数据库管理 ==============

class JournalDB:
    """期刊数据库管理"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化数据库表"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS natural_science_journals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    issn TEXT,
                    year TEXT,
                    category TEXT,
                    partition TEXT,
                    top_journal TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS social_science_journals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    year TEXT,
                    source TEXT,
                    partition TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 创建索引加速查询
            conn.execute("CREATE INDEX IF NOT EXISTS idx_natural_name ON natural_science_journals(name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_natural_year ON natural_science_journals(year)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_social_name ON social_science_journals(name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_social_year ON social_science_journals(year)")
            conn.commit()

        logger.info(f"[DB] 数据库初始化完成: {self.db_path}")

    def load_natural_science_journals(self) -> List[Dict[str, Any]]:
        """从数据库加载自然科学期刊数据"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT name, issn, year, category, partition, top_journal FROM natural_science_journals")
                rows = cursor.fetchall()
                data = [dict(row) for row in rows]
                logger.info(f"[DB] 从数据库加载 {len(data)} 条自然科学期刊数据")
                return data
        except Exception as e:
            logger.error(f"[DB] 加载自然科学期刊失败: {e}")
            return []

    def load_social_science_journals(self) -> List[Dict[str, Any]]:
        """从数据库加载社会科学期刊数据"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT name, year, source, partition FROM social_science_journals")
                rows = cursor.fetchall()
                data = [dict(row) for row in rows]
                logger.info(f"[DB] 从数据库加载 {len(data)} 条社会科学期刊数据")
                return data
        except Exception as e:
            logger.error(f"[DB] 加载社会科学期刊失败: {e}")
            return []

    def save_natural_science_journals(self, data: List[Dict[str, Any]]):
        """保存自然科学期刊数据到数据库"""
        if not data:
            return
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 清空旧数据
                conn.execute("DELETE FROM natural_science_journals")
                # 批量插入
                for item in data:
                    conn.execute(
                        "INSERT INTO natural_science_journals (name, issn, year, category, partition, top_journal) VALUES (?, ?, ?, ?, ?, ?)",
                        (item.get("name"), item.get("issn"), item.get("year"),
                         item.get("category"), item.get("partition"), item.get("top_journal"))
                    )
                conn.commit()
            logger.info(f"[DB] 保存 {len(data)} 条自然科学期刊数据到数据库")
        except Exception as e:
            logger.error(f"[DB] 保存自然科学期刊失败: {e}")

    def save_social_science_journals(self, data: List[Dict[str, Any]]):
        """保存社会科学期刊数据到数据库"""
        if not data:
            return
        try:
            with sqlite3.connect(self.db_path) as conn:
                # 清空旧数据
                conn.execute("DELETE FROM social_science_journals")
                # 批量插入
                for item in data:
                    conn.execute(
                        "INSERT INTO social_science_journals (name, year, source, partition) VALUES (?, ?, ?, ?)",
                        (item.get("name"), item.get("year"), item.get("source"), item.get("partition"))
                    )
                conn.commit()
            logger.info(f"[DB] 保存 {len(data)} 条社会科学期刊数据到数据库")
        except Exception as e:
            logger.error(f"[DB] 保存社会科学期刊失败: {e}")

    def get_counts(self) -> Dict[str, int]:
        """获取数据库中的数据量"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                natural_count = conn.execute("SELECT COUNT(*) FROM natural_science_journals").fetchone()[0]
                social_count = conn.execute("SELECT COUNT(*) FROM social_science_journals").fetchone()[0]
                return {"natural": natural_count, "social": social_count}
        except Exception as e:
            logger.error(f"[DB] 获取数据量失败: {e}")
            return {"natural": 0, "social": 0}

# ============== 期刊数据管理 ==============

class JournalDataManager:
    """期刊数据管理器"""

    def __init__(self):
        self.natural_science_journals: Dict[str, List[Dict[str, Any]]] = {}  # 按年份索引
        self.social_science_journals: Dict[str, List[Dict[str, Any]]] = {}  # 按年份索引
        self._all_natural: List[Dict[str, Any]] = []  # 所有自然科学期刊
        self._all_social: List[Dict[str, Any]] = []  # 所有社会科学期刊
        self._natural_index: Dict[str, List[int]] = {}  # 名称索引: name_lower -> [indices]
        self._social_index: Dict[str, List[int]] = {}  # 名称索引
        self._initialized = False
        self._init_error: Optional[str] = None

    def _build_index(self, data: List[Dict[str, Any]], index: Dict[str, List[int]], key: str = "name"):
        """构建名称索引以加速模糊匹配"""
        for i, item in enumerate(data):
            name = item.get(key, "").lower().strip()
            if name:
                # 按名称的前缀建立索引
                for length in range(3, len(name) + 1):
                    prefix = name[:length]
                    if prefix not in index:
                        index[prefix] = []
                    index[prefix].append(i)

    def _fuzzy_search(self, keyword: str, index: Dict[str, List[int]], data: List[Dict[str, Any]]) -> List[int]:
        """基于索引的模糊搜索"""
        keyword_lower = keyword.lower().strip()
        if not keyword_lower:
            return []

        candidates = set()

        # 1. 精确前缀匹配
        for length in range(3, len(keyword_lower) + 1):
            prefix = keyword_lower[:length]
            if prefix in index:
                candidates.update(index[prefix])

        # 2. 如果前缀匹配结果太少，进行全文搜索
        if len(candidates) < 10:
            for i, item in enumerate(data):
                name = item.get("name", "").lower()
                if keyword_lower in name:
                    candidates.add(i)

        return list(candidates)

    def set_natural_science_data(self, data: List[Dict[str, Any]]):
        """设置自然科学期刊数据"""
        self._all_natural = data

        # 按年份分组
        self.natural_science_journals = {}
        for item in data:
            year = item.get("year") or "unknown"
            if year not in self.natural_science_journals:
                self.natural_science_journals[year] = []
            self.natural_science_journals[year].append(item)

        # 构建索引
        self._natural_index = {}
        self._build_index(data, self._natural_index, "name")

        logger.info(f"[DataManager] 自然科学期刊: 共 {len(data)} 条，覆盖年份: {sorted(self.natural_science_journals.keys())}")

    def set_social_science_data(self, data: List[Dict[str, Any]]):
        """设置社会科学期刊数据"""
        self._all_social = data

        # 按年份分组
        self.social_science_journals = {}
        for item in data:
            year = item.get("year") or "unknown"
            if year not in self.social_science_journals:
                self.social_science_journals[year] = []
            self.social_science_journals[year].append(item)

        # 构建索引
        self._social_index = {}
        self._build_index(data, self._social_index, "name")

        logger.info(f"[DataManager] 社会科学期刊: 共 {len(data)} 条，覆盖年份: {sorted(self.social_science_journals.keys())}")

    def search_natural_science(
        self,
        name: str,
        year: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """搜索自然科学期刊"""
        if not self._all_natural:
            logger.warning("[DataManager] 自然科学期刊数据未加载")
            return []

        # 确定搜索范围
        if year is not None:
            year_str = str(year)
            search_data = self.natural_science_journals.get(year_str, [])
            # 重建该年份的临时索引
            temp_index = {}
            self._build_index(search_data, temp_index, "name")
            indices = self._fuzzy_search(name, temp_index, search_data)
            results = [search_data[i] for i in indices]
        else:
            # 搜索所有年份
            indices = self._fuzzy_search(name, self._natural_index, self._all_natural)
            results = [self._all_natural[i] for i in indices]

        logger.info(f"[DataManager] 自然科学搜索: name='{name}', year={year}, 找到 {len(results)} 条")
        return results[:limit]

    def search_social_science(
        self,
        name: str,
        year: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """搜索社会科学期刊"""
        if not self._all_social:
            logger.warning("[DataManager] 社会科学期刊数据未加载")
            return []

        # 确定搜索范围
        if year is not None:
            year_str = str(year)
            search_data = self.social_science_journals.get(year_str, [])
            temp_index = {}
            self._build_index(search_data, temp_index, "name")
            indices = self._fuzzy_search(name, temp_index, search_data)
            results = [search_data[i] for i in indices]
        else:
            indices = self._fuzzy_search(name, self._social_index, self._all_social)
            results = [self._all_social[i] for i in indices]

        logger.info(f"[DataManager] 社会科学搜索: name='{name}', year={year}, 找到 {len(results)} 条")
        return results[:limit]

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def init_error(self) -> Optional[str]:
        return self._init_error

    def get_stats(self) -> Dict[str, Any]:
        """获取数据统计"""
        return {
            "natural_science": {
                "total": len(self._all_natural),
                "years": sorted(self.natural_science_journals.keys()),
                "by_year": {y: len(d) for y, d in self.natural_science_journals.items()}
            },
            "social_science": {
                "total": len(self._all_social),
                "years": sorted(self.social_science_journals.keys()),
                "by_year": {y: len(d) for y, d in self.social_science_journals.items()}
            },
            "initialized": self._initialized
        }

# ============== 全局实例 ==============

dmp_client: Optional[DMPClient] = None
journal_manager: JournalDataManager = JournalDataManager()
journal_db: Optional[JournalDB] = None

# ============== 初始化逻辑 ==============

async def initialize_journal_data(progress_callback: Optional[Callable[[str, int, int], None]] = None):
    """初始化期刊数据（启动时调用）优先从SQLite加载，不存在则从API读取"""
    global dmp_client, journal_db

    if not DMP_APP_KEY or not DMP_APP_SECRET:
        logger.error("[Init] 缺少DMP_APP_KEY或DMP_APP_SECRET配置")
        journal_manager._init_error = "缺少DMP_APP_KEY或DMP_APP_SECRET配置"
        return False

    journal_manager._initialized = False

    # 初始化数据库
    journal_db = JournalDB(str(DB_FILE))
    db_counts = journal_db.get_counts()
    logger.info(f"[Init] 数据库当前数据量: 自然科学={db_counts['natural']}, 社会科学={db_counts['social']}")

    # 定义进度回调
    def on_progress(stage: str, current: int, total: int):
        if progress_callback:
            progress_callback(stage, current, total)
        logger.info(f"[Init] {stage}: {current}/{total}")

    # ========== 加载自然科学期刊 ==========
    logger.info("[Init] 开始加载自然科学期刊...")

    # 优先从数据库加载
    natural_data = journal_db.load_natural_science_journals()

    if not natural_data:
        logger.info("[Init] 数据库中无自然科学期刊数据，从API获取...")
        on_progress("自然科学期刊", 0, 1)

        # 创建DMP客户端并获取数据
        dmp_client = DMPClient(DMP_BASE_URL, DMP_APP_KEY, DMP_APP_SECRET)
        api_data = await dmp_client.fetch_all_data(
            NATURAL_SCIENCE_ENDPOINT,
            log_prefix="[自然科学]"
        )

        if api_data:
            # 标准化数据
            natural_data = []
            for item in api_data:
                natural_data.append({
                    "name": item.get("KM", ""),
                    "issn": item.get("ISSN", ""),
                    "year": item.get("NF", ""),
                    "category": item.get("DLXK", ""),
                    "partition": item.get("DLFQ", ""),
                    "top_journal": item.get("TOPQK", "").strip() if item.get("TOPQK") else ""
                })
            # 保存到数据库
            journal_db.save_natural_science_journals(natural_data)
            await dmp_client.close()
        else:
            logger.warning("[Init] 自然科学期刊API数据为空")
            await dmp_client.close()

    if natural_data:
        journal_manager.set_natural_science_data(natural_data)

    # ========== 加载社会科学期刊 ==========
    logger.info("[Init] 开始加载社会科学期刊...")

    # 优先从数据库加载
    social_data = journal_db.load_social_science_journals()

    if not social_data:
        logger.info("[Init] 数据库中无社会科学期刊数据，从API获取...")

        # 创建DMP客户端并获取数据
        dmp_client = DMPClient(DMP_BASE_URL, DMP_APP_KEY, DMP_APP_SECRET)
        api_data = await dmp_client.fetch_all_data(
            SOCIAL_SCIENCE_ENDPOINT,
            log_prefix="[社会科学]"
        )

        if api_data:
            # 标准化数据
            social_data = []
            for item in api_data:
                social_data.append({
                    "name": item.get("QKMC", ""),
                    "year": item.get("NF", ""),
                    "source": item.get("LY", ""),
                    "partition": item.get("FQ", "")
                })
            # 保存到数据库
            journal_db.save_social_science_journals(social_data)
            await dmp_client.close()
        else:
            logger.warning("[Init] 社会科学期刊API数据为空")
            await dmp_client.close()

    if social_data:
        journal_manager.set_social_science_data(social_data)

    # 关闭HTTP客户端
    if dmp_client:
        await dmp_client.close()

    # 标记初始化完成
    journal_manager._initialized = True

    logger.info(f"[Init] 初始化完成! 统计: {json.dumps(journal_manager.get_stats(), ensure_ascii=False, indent=2)}")
    return True

# ============== FastAPI 应用 ==============

class JournalQueryRequest(BaseModel):
    """期刊查询请求模型"""
    journal_type: str = Field(..., pattern="^(science|social)$", description="期刊类型: science(自然科学) 或 social(社会科学)")
    name: str = Field(..., min_length=1, description="期刊名称关键词（必填，模糊匹配）")
    year: Optional[int] = Field(None, description="年份（可选）")
    limit: int = Field(100, ge=1, le=500, description="返回数量限制")

class JournalQueryResponse(BaseModel):
    """期刊查询响应模型"""
    total: int
    results: List[Dict[str, Any]]
    error: Optional[str] = None

async def query_journals(request: JournalQueryRequest) -> JournalQueryResponse:
    """统一期刊查询"""
    if not journal_manager.is_initialized:
        return JournalQueryResponse(total=0, results=[], error="数据未初始化，请稍后重试")

    logger.info(f"[Query] 类型: {request.journal_type}, 关键词: {request.name}, 年份: {request.year}")

    if request.journal_type == "science":
        results = journal_manager.search_natural_science(request.name, request.year, request.limit)
    else:
        results = journal_manager.search_social_science(request.name, request.year, request.limit)

    logger.info(f"[Query] 返回 {len(results)} 条结果")
    return JournalQueryResponse(total=len(results), results=results)

# ============== MCP Server ==============

mcp = FastMCP("Journal Classification")

@mcp.tool()
async def query_journals_tool(
    journal_type: str,
    name: str,
    year: int = None,
    limit: int = 100
) -> Dict[str, Any]:
    """
    期刊分区查询

    Args:
        journal_type: 期刊类型 - "science" (自然科学) 或 "social" (社会科学)
        name: 期刊名称关键词（必填）
        year: 年份（可选，不填则搜索所有年份）
        limit: 返回数量限制 (默认100)
    """
    request = JournalQueryRequest(
        journal_type=journal_type,
        name=name,
        year=year,
        limit=limit
    )
    result = await query_journals(request)
    return {"total": result.total, "results": result.results, "error": result.error}

# 创建 MCP HTTP 应用 (使用 stateless streamable-http 传输)
mcp_app = mcp.http_app(path="/", transport="streamable-http", stateless_http=True)

# ============== 启动事件 ==============

init_complete = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global init_complete

    logger.info("=" * 50)
    logger.info("[App] 启动应用，初始化期刊数据...")
    logger.info("=" * 50)

    # 初始化数据
    success = await initialize_journal_data()

    if success:
        logger.info("[App] 数据初始化成功!")
        init_complete = True
    else:
        logger.error(f"[App] 数据初始化失败: {journal_manager.init_error}")
        init_complete = False

    yield

    # 清理
    if dmp_client:
        await dmp_client.close()
    logger.info("[App] 应用关闭")

app = FastAPI(
    title="Journal Classification API (DMP)",
    description="期刊分区查询服务 - 使用DMP API数据源，支持REST API和MCP协议",
    version="2.0.0",
    lifespan=mcp_app.lifespan
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# 挂载 MCP 应用 (MCP endpoint at /mcp)
app.mount("/mcp", mcp_app)

# ============== API 端点 ==============

class QueryResponse(BaseModel):
    total: int
    results: List[Any]
    error: Optional[str] = None

class StatsResponse(BaseModel):
    status: str
    initialized: bool
    natural_science: Dict[str, Any]
    social_science: Dict[str, Any]

@app.get("/status", response_model=StatsResponse, tags=["Status"])
async def get_status():
    """服务状态及数据统计"""
    stats = journal_manager.get_stats()
    return StatsResponse(
        status="healthy" if journal_manager.is_initialized else "initializing_failed",
        initialized=journal_manager.is_initialized,
        natural_science=stats["natural_science"],
        social_science=stats["social_science"]
    )

@app.get("/journals", response_model=QueryResponse, tags=["Journals"])
async def query_journals_api(
    journal_type: str = Query(..., pattern="^(science|social)$", description="期刊类型: science(自然科学) 或 social(社会科学)"),
    name: str = Query(..., min_length=1, description="期刊名称关键词（必填）"),
    year: Optional[int] = Query(None, description="年份（可选）"),
    limit: int = Query(100, ge=1, le=500, description="返回数量限制")
):
    """
    期刊分区查询

    - **journal_type**: 期刊类型 - `science`(自然科学) 或 `social`(社会科学)
    - **name**: 期刊名称关键词（必填，支持模糊匹配）
    - **year**: 年份（可选，不指定则搜索所有年份）
    - **limit**: 返回数量限制（默认100，最大500）
    """
    request = JournalQueryRequest(
        journal_type=journal_type,
        name=name,
        year=year,
        limit=limit
    )
    result = await query_journals(request)
    return QueryResponse(total=result.total, results=result.results, error=result.error)

@app.post("/journals", response_model=QueryResponse, tags=["Journals"])
async def query_journals_post(request: JournalQueryRequest):
    """期刊分区查询 (POST)"""
    result = await query_journals(request)
    return QueryResponse(total=result.total, results=result.results, error=result.error)

@app.post("/reload", tags=["Admin"])
async def reload_data():
    """重新加载期刊数据"""
    global init_complete
    init_complete = False
    success = await initialize_journal_data()
    init_complete = success
    return {"success": success, "message": "数据重新加载成功" if success else "数据重新加载失败"}

if __name__ == "__main__":
    import uvicorn
    import asyncio

    # 同步启动方式（用于直接运行）
    async def main():
        print("正在初始化期刊数据...")
        success = await initialize_journal_data()
        if success:
            print("\n数据初始化成功! 启动Web服务...")
            print(f"API文档: http://{HOST}:{PORT}/docs")
            import uvicorn
            config = uvicorn.Config(app, host=HOST, port=PORT, log_level="info")
            server = uvicorn.Server(config)
            await server.serve()
        else:
            print(f"\n数据初始化失败: {journal_manager.init_error}")
            print("请检查DMP_APP_KEY和DMP_APP_SECRET配置")

    asyncio.run(main())