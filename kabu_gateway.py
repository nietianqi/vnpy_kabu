"""
VeighNa Gateway for kabuステーション API (日本券商 Kabu Securities)

Author: AI Assistant
Date: 2025
License: MIT

功能特性:
- 日本股票现物交易 (现货)
- 日本股票信用交易 (融资融券)
- WebSocket实时行情推送
- 动态合约注册 (UI输入股票代码即可订阅)
- 持仓查询和账户查询
- 完整的订单管理

使用方法:
1. 在VeighNa交易界面连接本Gateway
2. 配置API密码和交易参数
3. 在行情窗口输入股票代码(如"7203")回车即可订阅行情
4. 支持现物和信用交易模式切换
"""

from __future__ import annotations

import copy
import json
import time
import traceback
from threading import Thread, Lock
from datetime import datetime
from typing import Dict, Set, List, Optional, Tuple
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP

import requests
import websocket  # websocket-client

from vnpy.event import EventEngine, EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderRequest,
    CancelRequest,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    SubscribeRequest,
    LogData,
)
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Offset,
    OrderType,
    Status,
    Product,
)

# ================= 常量定义 =================

# 默认交易所: 东京证券交易所
try:
    DEFAULT_EXCHANGE = Exchange.TSE
except AttributeError:
    try:
        DEFAULT_EXCHANGE = Exchange.LOCAL
    except AttributeError:
        DEFAULT_EXCHANGE = list(Exchange)[0]

try:
    SMART_EXCHANGE = Exchange.SMART
except AttributeError:
    SMART_EXCHANGE = DEFAULT_EXCHANGE

# Kabu交易所代码映射（新规发单）
# 2026-02-28以降: 新規発注は Exchange=1(東証) 廃止
# 代わりに 9(SOR) または 27(東証+) を使用
EXCHANGE_VT2KABU: Dict[Exchange, int] = {
    DEFAULT_EXCHANGE: 9,  # 9=SOR（智能路由，自动选最优价格）
}
if SMART_EXCHANGE not in EXCHANGE_VT2KABU:
    EXCHANGE_VT2KABU[SMART_EXCHANGE] = 9

# Kabu交易所代码映射（信用返済/平仓专用）
# 信用返済应优先按持仓所在市场返済；若只知道“SMART”则回到配置的 ORDER_EXCHANGE。
EXCHANGE_VT2KABU_CLOSE: Dict[Exchange, int] = {
    DEFAULT_EXCHANGE: 1,
}
if SMART_EXCHANGE not in EXCHANGE_VT2KABU_CLOSE:
    EXCHANGE_VT2KABU_CLOSE[SMART_EXCHANGE] = 9

# Kabu订单状态映射
STATUS_KABU2VT: Dict[int, Status] = {
    1: Status.SUBMITTING,  # 待报
    2: Status.NOTTRADED,   # 未成交
    3: Status.PARTTRADED,  # 部分成交
    4: Status.ALLTRADED,   # 全部成交
    5: Status.CANCELLED,   # 已撤销
    6: Status.REJECTED,    # 已拒绝
}

# 预定义合约列表 (可选)
# 如果为空,将在订阅时动态创建合约
KABU_STOCK_CONTRACTS = [
    ("7203", "トヨタ自動車", 1),
    ("4568", "第一三共", 1),
    ("7014", "名村造船", 1),
    ("6425", "ユニバーサルエンターテインメント", 1),
    ("3697", "SHIFT", 1),
    ("299A", "クラシル", 1),
    # KabuSignalStrategy 运行品种（名称从推送数据自动更新）
    ("4680", "ラウンドワン", 1),
    ("2726", "PALグループ", 1),
    ("6532", "ベイカレント・コンサルティング", 1),
]

# ================= 工具函数 =================

def parse_kabu_datetime(value: Optional[str]) -> datetime:
    """
    解析Kabu API返回的时间字符串
    支持格式:
    - ISO 8601: 2020-09-01T09:00:00+09:00
    - 日期格式: 2020/09/01 09:00:00
    - 日期格式: 2020-09-01 09:00:00
    """
    if not value:
        return datetime.now()

    value = value.strip()

    # ISO 8601格式
    try:
        return datetime.fromisoformat(value.replace('+09:00', ''))
    except Exception:
        pass

    # 常见日期格式
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            continue

    return datetime.now()


def normalize_symbol_exchange(symbol: str, exchange: Optional[Exchange]) -> Tuple[str, Exchange]:
    """
    规范化股票代码和交易所
    支持输入格式:
    - "7014" + 交易所下拉框
    - "7014.TSE"
    """
    s = (symbol or "").strip().upper()
    ex = exchange

    # 检查是否包含交易所后缀
    if "." in s:
        parts = s.split(".", 1)
        s = parts[0].strip()
        ex_str = parts[1].strip().upper()

        # 尝试匹配Exchange枚举
        if ex is None:
            for e in Exchange:
                if str(e.value).upper() == ex_str or str(e.name).upper() == ex_str:
                    ex = e
                    break

    # 默认使用东京证券交易所
    if ex is None:
        ex = DEFAULT_EXCHANGE

    return s, ex


def exchange_to_kabu_code(exchange: Exchange, is_close: bool = False) -> int:
    """
    将VeighNa交易所枚举转换为Kabu API的交易所代码

    2026-02-28以降の仕様変更:
    - 新規発注: Exchange=1(東証)廃止 → 9(SOR)または27(東証+)を使用
    - 信用返済: 若持仓市场未知，则 TSE→1，SMART→ORDER_EXCHANGE

    Args:
        exchange: VeighNa交易所枚举
        is_close: True=信用返済（平仓），False=新规发单（开仓/现物）
    """
    if is_close:
        return EXCHANGE_VT2KABU_CLOSE.get(exchange, 1 if exchange == DEFAULT_EXCHANGE else 9)
    return EXCHANGE_VT2KABU.get(exchange, 9)  # 新规发单用9(SOR)


def to_kabu_side(direction: Direction) -> str:
    """
    将VeighNa方向转换为Kabu API的Side
    1 = 卖出, 2 = 买入
    """
    return "2" if direction == Direction.LONG else "1"


def to_kabu_front_order_type(order_type: OrderType) -> int:
    """
    将VeighNa订单类型转换为Kabu API的FrontOrderType
    10 = 成行 (Market), 20 = 指値 (Limit)
    """
    return 10 if order_type == OrderType.MARKET else 20


TSE_TICK_TABLES: Dict[str, List[Tuple[float, float]]] = {
    # 10000: 普通株式
    "10000": [
        (1_000.0, 1.0),
        (3_000.0, 1.0),
        (5_000.0, 5.0),
        (10_000.0, 10.0),
        (30_000.0, 10.0),
        (50_000.0, 50.0),
        (100_000.0, 100.0),
        (300_000.0, 100.0),
        (500_000.0, 500.0),
        (1_000_000.0, 1_000.0),
        (3_000_000.0, 1_000.0),
        (5_000_000.0, 5_000.0),
        (10_000_000.0, 10_000.0),
        (30_000_000.0, 10_000.0),
        (50_000_000.0, 50_000.0),
    ],
    # 10003: TOPIX500構成銘柄
    "10003": [
        (1_000.0, 0.1),
        (3_000.0, 0.5),
        (10_000.0, 1.0),
        (30_000.0, 5.0),
        (100_000.0, 10.0),
        (300_000.0, 50.0),
        (1_000_000.0, 100.0),
        (3_000_000.0, 500.0),
        (10_000_000.0, 1_000.0),
        (30_000_000.0, 5_000.0),
        (50_000_000.0, 10_000.0),
    ],
    # 10004: 売買単位1口のETF等
    "10004": [
        (1_000.0, 0.1),
        (3_000.0, 1.0),
        (10_000.0, 1.0),
        (30_000.0, 5.0),
        (100_000.0, 10.0),
        (300_000.0, 50.0),
        (1_000_000.0, 100.0),
        (3_000_000.0, 500.0),
        (10_000_000.0, 1_000.0),
        (30_000_000.0, 5_000.0),
        (50_000_000.0, 10_000.0),
    ],
}


def normalize_price_range_group(price_range_group: object) -> str:
    """Normalize kabu PriceRangeGroup into a stable string key."""
    return str(price_range_group or "").strip()


def _get_tse_tick_table(price_range_group: object) -> List[Tuple[float, float]]:
    group = normalize_price_range_group(price_range_group)
    return TSE_TICK_TABLES.get(group, TSE_TICK_TABLES["10000"])


def get_minimum_tse_tick(price_range_group: object = "") -> float:
    """Return the smallest tick for a known TSE price range group."""
    return _get_tse_tick_table(price_range_group)[0][1]


def get_tse_order_input_tick(price: float, price_range_group: object = "") -> float:
    """Return the legal TSE order-entry tick for the given price/group."""
    table = _get_tse_tick_table(price_range_group)
    if price <= 0:
        return table[0][1]
    for upper_bound, tick in table:
        if price <= upper_bound:
            return tick
    return table[-1][1]


def detect_tse_pricetick_from_market_data(data: dict) -> float:
    """Infer current pricetick from board push data using PriceRangeGroup when present."""
    price_range_group = normalize_price_range_group(data.get("PriceRangeGroup"))
    for key in ("CurrentPrice", "BidPrice", "AskPrice", "OpeningPrice", "HighPrice", "LowPrice"):
        try:
            price = float(data.get(key) or 0)
        except (TypeError, ValueError):
            price = 0.0
        if price > 0:
            return get_tse_order_input_tick(price, price_range_group=price_range_group)
    return get_minimum_tse_tick(price_range_group) if price_range_group else 1.0


def kabu_exchange_code_to_vt_exchange(exchange_code: object) -> Exchange:
    """Map kabu exchange codes into the closest vn.py exchange enum."""
    try:
        code = int(exchange_code)
    except (TypeError, ValueError):
        code = 1

    if code in (9, 27):
        return SMART_EXCHANGE
    if code == 1:
        return DEFAULT_EXCHANGE
    return DEFAULT_EXCHANGE


def resolve_close_exchange_code(position_exchange_code: object, fallback_exchange_code: int) -> int:
    """Close on the market where the position is actually held whenever possible."""
    try:
        code = int(position_exchange_code)
    except (TypeError, ValueError):
        code = fallback_exchange_code

    if code in (1, 3, 5, 6, 9, 27):
        return code
    return fallback_exchange_code


def align_price_to_tse_tick(
    price: float,
    side: str = "",
    ref_price: float = 0.0,
    rounding: str = "nearest",
    price_range_group: object = ""
) -> float:
    """Snap order price to a legal TSE tick.

    rounding:
    - ``nearest``: gateway safety net, direction-neutral ROUND_HALF_UP
    - ``up``: round up to the next legal tick
    - ``down``: round down to the previous legal tick
    - ``close``: sell-to-close uses ``up``, buy-to-cover uses ``down``
    """
    if price <= 0:
        return price
    anchor = ref_price if ref_price > 0 else price
    tick = get_tse_order_input_tick(anchor, price_range_group=price_range_group)
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    rounding_mode = ROUND_HALF_UP
    if rounding == "up":
        rounding_mode = ROUND_CEILING
    elif rounding == "down":
        rounding_mode = ROUND_FLOOR
    elif rounding == "close":
        if side == "1":
            rounding_mode = ROUND_CEILING
        elif side == "2":
            rounding_mode = ROUND_FLOOR
    aligned = float((price_dec / tick_dec).to_integral_value(rounding=rounding_mode) * tick_dec)
    return aligned if aligned > 0 else price


def normalize_stock_order_type_price(
    front_order_type: int,
    price: float,
    *,
    requested_order_type: Optional[OrderType] = None,
) -> Tuple[int, float, str]:
    """Normalize stock FrontOrderType/Price into a kabu-valid pair."""
    try:
        ft = int(front_order_type)
    except (TypeError, ValueError):
        ft = 20 if requested_order_type == OrderType.LIMIT else 10

    try:
        px = float(price or 0.0)
    except (TypeError, ValueError):
        px = 0.0

    # Market must always carry Price=0.
    if ft == 10:
        if px != 0.0:
            if requested_order_type == OrderType.LIMIT and px > 0:
                return 20, px, "restore_limit_from_nonzero_price"
            return 10, 0.0, "force_market_zero_price"
        return 10, 0.0, ""

    # Limit must carry a positive price.
    if ft == 20:
        if px > 0:
            return 20, px, ""
        return 20, px, "invalid_limit_price"

    return ft, px, ""


def to_vt_order_type(front_order_type: int, fallback: OrderType) -> OrderType:
    """Map normalized kabu FrontOrderType back to vn.py OrderType."""
    if front_order_type == 10:
        return OrderType.MARKET
    if front_order_type == 20:
        return OrderType.LIMIT
    return fallback


def to_vt_direction(side: str) -> Direction:
    """
    将Kabu API的Side转换为VeighNa方向
    """
    return Direction.LONG if side == "2" else Direction.SHORT


# ================= REST API 封装 =================

class KabuRestApi:
    """
    Kabu Station REST API封装

    主要功能:
    - Token认证
    - 下单 (现物/信用)
    - 撤单
    - 查询持仓
    - 查询账户
    - 行情注册
    """

    def __init__(self, gateway: "KabuGateway") -> None:
        self.gateway = gateway
        self.session = requests.Session()

        # API配置
        self.rest_host: str = "http://localhost:18080"
        self.rest_base: str = ""
        self.api_password: str = ""

        # 交易参数
        self.account_type: int = 4      # 2: 一般, 4: 特定
        self.deliv_type: int = 2        # 2: お預り金
        self.fund_type: str = "02"      # 02: 保護
        self.use_margin: bool = False   # 是否使用信用交易
        self.margin_trade_type: int = 1 # 1: 制度信用

        # 调试模式
        self.debug: bool = False

        # 日志节流 (减少卡顿)
        self._last_log_time: float = 0.0
        self._log_sample_rate: float = 1.0  # 每秒最多1条WS消息日志

        # API Token
        self.token: str = ""
        self.token_lock = Lock()

        # 订单跟踪 (用于检测成交状态变化)
        self.active_orders: Dict[str, OrderData] = {}  # orderid -> OrderData
        self.orders_lock = Lock()

        # 持仓查询优化 (降频策略)
        self._last_position_query_time: float = 0.0
        self._position_query_interval: float = 3.0  # 默认3秒查一次
        self._has_position: bool = False  # 是否有持仓
        self._recent_trade_time: float = 0.0  # 最近成交时间

        # 防止成交回报重复推送 (WS和REST轮询可能同时推送同一笔成交)
        self._ws_pushed_execution_ids: Set[str] = set()  # WS已推送的ExecutionId集合
        self._ws_pushed_order_traded: Dict[str, float] = {}  # orderid -> WS推送时的成交量

        # 符号元数据缓存
        self.symbol_price_range_groups: Dict[str, str] = {}  # symbol -> PriceRangeGroup

    def connect(self, setting: dict) -> None:
        """连接并获取API Token"""
        # 解析REST地址
        self.rest_host = str(setting.get("REST_HOST", "http://localhost:18080")).strip().rstrip("/")
        self.rest_base = self.rest_host if self.rest_host.endswith("/kabusapi") else self.rest_host + "/kabusapi"

        # API密码 (支持多种字段名)
        self.api_password = str(
            setting.get("API_PASSWORD") or
            setting.get("APIPassword") or
            setting.get("PASSWORD") or
            ""
        ).strip()

        if not self.api_password:
            self.gateway.write_log("警告: 未设置API_PASSWORD,请在连接配置中填写kabuステーション的API密码")

        # 交易参数
        self.account_type = int(setting.get("ACCOUNT_TYPE", 4))
        self.deliv_type = int(setting.get("DELIV_TYPE", 2))
        self.fund_type = str(setting.get("FUND_TYPE", "02"))
        self.use_margin = bool(setting.get("USE_MARGIN", False))
        self.margin_trade_type = int(setting.get("MARGIN_TRADE_TYPE", 1))
        self.debug = bool(setting.get("DEBUG", False))

        # 2026-02-28规格变更: 读取新规发单Exchange配置
        # 9=SOR(智能路由), 27=東証+(东证内部撮合)
        order_exchange_code = int(setting.get("ORDER_EXCHANGE", 9))
        if order_exchange_code not in (9, 27):
            self.gateway.write_log(
                f"警告: ORDER_EXCHANGE={order_exchange_code} 不合法，"
                f"2026-02-28后新规发单只允许9(SOR)或27(東証+)，已自动改为9"
            )
            order_exchange_code = 9
        # 动态更新全局映射（影响exchange_to_kabu_code函数）
        EXCHANGE_VT2KABU[DEFAULT_EXCHANGE] = order_exchange_code
        if SMART_EXCHANGE != DEFAULT_EXCHANGE:
            EXCHANGE_VT2KABU[SMART_EXCHANGE] = order_exchange_code
            EXCHANGE_VT2KABU_CLOSE[SMART_EXCHANGE] = order_exchange_code
        # 同时保存为实例属性，供 _send_close_order 等内部方法使用
        self.order_exchange = order_exchange_code

        # 设置HTTP头
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

        self.gateway.write_log(
            f"KABU REST连接: base={self.rest_base}, "
            f"account_type={self.account_type}, deliv_type={self.deliv_type}, "
            f"use_margin={self.use_margin}, "
            f"order_exchange={order_exchange_code}({'SOR' if order_exchange_code == 9 else '東証+'}), "
            f"debug={self.debug}"
        )

        # 获取Token
        self._get_token()

    def _get_token(self) -> bool:
        """
        获取API Token

        Returns:
            bool: 成功返回True,失败返回False
        """
        if not self.api_password:
            self.gateway.write_log("获取Token失败: API_PASSWORD为空")
            return False

        url = f"{self.rest_base}/token"
        payload = {"APIPassword": self.api_password}

        try:
            resp = self.session.post(url, json=payload, timeout=10)

            if resp.status_code != 200:
                # 解析错误信息
                try:
                    error_data = resp.json()
                    error_code = error_data.get("Code", -1)
                    error_msg = error_data.get("Message", "")

                    # Code 4001007: ログイン認証エラー
                    if error_code == 4001007:
                        self.gateway.write_log("=" * 80)
                        self.gateway.write_log("🔴 Token获取失败: 登录认证错误 (Code 4001007)")
                        self.gateway.write_log("")
                        self.gateway.write_log("可能原因:")
                        self.gateway.write_log("  1. Kabu Station未启动或未登录")
                        self.gateway.write_log("  2. API密码未设置或设置错误")
                        self.gateway.write_log("  3. API功能未启用")
                        self.gateway.write_log("")
                        self.gateway.write_log("修复步骤:")
                        self.gateway.write_log("  1. 启动Kabu Station并登录账户")
                        self.gateway.write_log("  2. 菜单: 設定 → API → 設定")
                        self.gateway.write_log("  3. 设置APIパスワード并点击登録")
                        self.gateway.write_log("  4. 将密码配置到run_kabu.py的API_PASSWORD")
                        self.gateway.write_log("")
                        self.gateway.write_log(f"当前配置的密码长度: {len(self.api_password)}字符")
                        self.gateway.write_log("=" * 80)
                    else:
                        self.gateway.write_log(f"获取Token失败: HTTP {resp.status_code} - {resp.text}")
                except Exception:
                    self.gateway.write_log(f"获取Token失败: HTTP {resp.status_code} - {resp.text}")
                return False

            data = resp.json()
            token = data.get("Token", "")

            if not token:
                self.gateway.write_log(f"获取Token失败: 返回数据中无Token字段 - {data}")
                return False

            with self.token_lock:
                self.token = token
                self.session.headers.update({"X-API-KEY": self.token})

            self.gateway.write_log("✅ Token获取成功")
            return True

        except Exception as e:
            self.gateway.write_log(f"获取Token异常: {e}")
            return False

    def update_symbol_price_range_group(self, symbol: str, price_range_group: object) -> str:
        """Update cached PriceRangeGroup for a symbol."""
        normalized_symbol = str(symbol or "").strip()
        group = normalize_price_range_group(price_range_group)
        if normalized_symbol and group:
            self.symbol_price_range_groups[normalized_symbol] = group
        return group

    def get_symbol_price_range_group(self, symbol: str, refresh: bool = False) -> str:
        """Get PriceRangeGroup from cache or `/symbol/{symbol}`."""
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            return ""

        cached = self.symbol_price_range_groups.get(normalized_symbol, "")
        if cached and not refresh:
            return cached

        try:
            resp = self._make_request("GET", f"{self.rest_base}/symbol/{normalized_symbol}", timeout=10)
            if not resp:
                return cached

            data = resp.json()
            group = self.update_symbol_price_range_group(normalized_symbol, data.get("PriceRangeGroup"))
            return group or cached
        except Exception as e:
            if self.debug:
                self.gateway.write_log(f"查询PriceRangeGroup异常: symbol={normalized_symbol} - {e}")
            return cached

    def _make_request(
        self,
        method: str,
        url: str,
        retry_on_auth_error: bool = True,
        **kwargs
    ) -> Optional[requests.Response]:
        """
        通用HTTP请求方法(自动处理token失效)

        Args:
            method: HTTP方法 (GET/POST/PUT/DELETE)
            url: 请求URL
            retry_on_auth_error: 遇到401/403时是否自动刷新token重试
            **kwargs: 传递给requests的其他参数

        Returns:
            Response对象, 失败返回None
        """
        if not self.token and method.upper() != "POST":  # POST可能是获取token本身
            self._get_token()
            if not self.token:
                return None

        try:
            # 第一次请求
            resp = self.session.request(method, url, **kwargs)

            # 检查是否token失效
            if resp.status_code in (401, 403) and retry_on_auth_error:
                self.gateway.write_log(f"⚠️ Token失效(HTTP {resp.status_code}), 尝试刷新...")

                # 刷新token
                if self._get_token():
                    # 重试一次
                    self.gateway.write_log("Token刷新成功, 重试请求...")
                    resp = self.session.request(method, url, **kwargs)
                else:
                    self.gateway.write_log("❌ Token刷新失败")
                    return None

            return resp

        except Exception as e:
            self.gateway.write_log(f"请求异常: {method} {url} - {e}")
            if self.debug:
                self.gateway.write_log(traceback.format_exc())
            return None

    def send_order(self, req: OrderRequest) -> str:
        """
        发送订单 (现物/信用)

        支持:
        - 现物买卖
        - 信用新規 (开仓)
        - 信用返済 (平仓,自动匹配持仓)
        """
        if not self.token:
            self._get_token()
            if not self.token:
                self.gateway.write_log("下单失败: 无Token")
                return ""

        url = f"{self.rest_base}/sendorder"

        # 解析股票代码和交易所
        symbol, ex = normalize_symbol_exchange(req.symbol, req.exchange)
        # 2026-02-28规格变更: 新规发单用 9(SOR)/27(東証+)
        # 信用返済实际在 _send_close_order 中按持仓所在市场决定
        is_close_order = self.use_margin and req.offset not in (Offset.OPEN, Offset.NONE)
        exchange_code = exchange_to_kabu_code(ex, is_close=is_close_order)

        # 订单参数
        side = to_kabu_side(req.direction)
        front_order_type = to_kabu_front_order_type(req.type)
        price = float(req.price) if req.type == OrderType.LIMIT else 0.0
        front_order_type, price, normalize_reason = normalize_stock_order_type_price(
            front_order_type,
            price,
            requested_order_type=req.type,
        )
        if normalize_reason:
            self.gateway.write_log(
                f"下单参数修正: reason={normalize_reason} FrontOrderType={front_order_type} Price={price}"
            )
            if normalize_reason == "invalid_limit_price":
                self.gateway.write_log("Reject order: limit order requires a positive price")
                return ""
        price_range_group = self.get_symbol_price_range_group(symbol) if (front_order_type == 20 and price > 0) else ""
        # 东证 tick 校正（gateway 安全网；策略层已对齐则此处为 no-op）
        if front_order_type == 20 and price > 0:
            aligned = align_price_to_tse_tick(
                price,
                side,
                ref_price=price,
                price_range_group=price_range_group
            )
            if abs(aligned - price) > 1e-9:
                self.gateway.write_log(
                    f"东证Tick校正(开仓): {price:.2f} → {aligned:.2f}"
                )
                price = aligned
        qty = int(req.volume)

        # 判断是现物还是信用
        cash_margin = 1  # 默认现物
        margin_trade_type = None
        close_positions = None

        if self.use_margin:
            if req.offset in (Offset.OPEN, Offset.NONE):
                # 信用新規 (开仓)
                cash_margin = 2
                margin_trade_type = self.margin_trade_type
            else:
                # 信用返済 (平仓) - 需要查询持仓
                return self._send_close_order(symbol, ex, side, front_order_type, price, qty, req=req)

        # 构建订单请求 - 基础字段
        payload = {
            "Symbol": symbol,
            "Exchange": exchange_code,
            "SecurityType": 1,  # 株式
            "Side": side,
            "CashMargin": cash_margin,
            "AccountType": self.account_type,
            "Qty": qty,
            "FrontOrderType": front_order_type,
            "Price": price,
            "ExpireDay": 0,  # 当日有効
        }

        # 现物订单额外字段
        if cash_margin == 1:
            # 根据官方API文档:
            # - 现物買: DelivType需指定(2/4/...), FundType需指定
            # - 现物売: DelivType必须为0, FundType必须是"两个半角空格"
            if side == "2":  # 买入
                payload["DelivType"] = self.deliv_type
                payload["FundType"] = self.fund_type
            else:  # 卖出
                payload["DelivType"] = 0
                payload["FundType"] = "  "  # 两个半角空格
        # 信用新規订单额外字段
        else:
            # 信用新規必须DelivType=0(指定なし) - 根据官方API文档
            payload["DelivType"] = 0
            payload["MarginTradeType"] = margin_trade_type if margin_trade_type is not None else self.margin_trade_type
            # FundType设置为11(信用取引)或不设置(自动设置为11)
            payload["FundType"] = "11"

        if self.debug:
            self.gateway.write_log(f"下单请求: {payload}")

        # 发送订单
        try:
            resp = self.session.post(url, json=payload, timeout=10)

            if resp.status_code != 200:
                self.gateway.write_log(f"下单失败: HTTP {resp.status_code} - {resp.text}")
                return ""

            data = resp.json()
            order_id = data.get("OrderId", "") or data.get("ID", "")

            if not order_id:
                self.gateway.write_log(f"下单失败: 返回数据中无OrderId - {data}")
                return ""

            # 推送订单状态
            order_req = copy.copy(req)
            order_req.volume = float(qty)
            order_req.type = to_vt_order_type(front_order_type, req.type)
            order_req.price = price
            order = order_req.create_order_data(order_id, self.gateway.gateway_name)
            order.status = Status.SUBMITTING
            order.datetime = datetime.now()
            self.gateway.on_order(order)

            # 加入活跃订单跟踪
            with self.orders_lock:
                self.active_orders[order_id] = order

            if self.debug:
                self.gateway.write_log(f"下单成功: order_id={order_id}")

            return order_id

        except Exception as e:
            self.gateway.write_log(f"下单异常: {e}")
            if self.debug:
                self.gateway.write_log(traceback.format_exc())
            return ""

    def _send_close_order(self, symbol: str, ex: Exchange, side: str,
                         front_order_type: int, price: float, qty: int,
                         req: Optional[OrderRequest] = None) -> str:
        """
        发送平仓订单 (信用返済)
        自动查询持仓并匹配ClosePositions
        """
        requested_order_type = req.type if req is not None else None
        front_order_type, price, normalize_reason = normalize_stock_order_type_price(
            front_order_type,
            price,
            requested_order_type=requested_order_type,
        )
        if normalize_reason:
            self.gateway.write_log(
                f"平仓参数修正: reason={normalize_reason} FrontOrderType={front_order_type} Price={price}"
            )
            if normalize_reason == "invalid_limit_price":
                self.gateway.write_log("Reject close order: limit order requires a positive price")
                return ""

        # 查询信用持仓
        open_side_needed = "1" if side == "2" else "2"  # 平多单需要找买建玉,平空单需要找卖建玉
        margin_positions = self.get_positions(product="2", symbol=symbol)

        if self.debug:
            self.gateway.write_log(f"查询到{len(margin_positions)}个信用持仓,需要Side={open_side_needed}")

        # 筛选可平仓位
        holds = []
        for p in margin_positions:
            if self.debug:
                self.gateway.write_log(f"持仓: {p}")

            try:
                leaves = int(float(p.get("LeavesQty", 0) or 0))
            except Exception:
                leaves = 0

            if leaves <= 0:
                if self.debug:
                    self.gateway.write_log(f"跳过: LeavesQty={leaves} <= 0")
                continue

            pos_side = str(p.get("Side", ""))
            if pos_side != open_side_needed:
                if self.debug:
                    self.gateway.write_log(f"跳过: Side={pos_side} != {open_side_needed}")
                continue

            hold_id = str(p.get("ExecutionID", "") or "")
            if not hold_id.startswith("E"):
                if self.debug:
                    self.gateway.write_log(f"跳过: ExecutionID={hold_id} 不是E开头")
                continue

            holds.append(p)

        if not holds:
            self.gateway.write_log(f"未找到{symbol}的信用建玉可平仓 (需要Side={open_side_needed})")
            if margin_positions:
                self.gateway.write_log(f"提示: 查询到{len(margin_positions)}个持仓,但筛选后为空,请检查DEBUG日志")
            # 回退到现物卖出
            return ""

        # 信用返済の DelivType は「お預り金」= 2 が正解。
        # API doc: DelivType=0（指定なし）は信用返済では不可（4001005になる）。
        # DelivType=2 = お預り金（deposits held）= 通常の信用返済売り/返済買い。
        # DelivType=3 = auマネーコネクト（au Money Connect 契約者のみ）。
        close_deliv_type = 2

        # 信用返済必须按持仓所在市场返済，避免 TSE 建玉被错误送到 SOR/東証+。
        # GROUP KEY: (CloseExchange, MarginTradeType)
        groups: Dict[Tuple[int, int], List[dict]] = {}
        for p in holds:
            mt = int(p.get("MarginTradeType", self.margin_trade_type) or self.margin_trade_type)
            close_exchange_code = resolve_close_exchange_code(p.get("Exchange"), self.order_exchange)
            groups.setdefault((close_exchange_code, mt), []).append(p)

        # 东证 tick 校正（平仓价）
        price_range_group = self.get_symbol_price_range_group(symbol) if (front_order_type == 20 and price > 0) else ""
        if front_order_type == 20 and price > 0:
            aligned = align_price_to_tse_tick(
                price,
                side,
                ref_price=price,
                rounding="close",
                price_range_group=price_range_group
            )
            if abs(aligned - price) > 1e-9:
                self.gateway.write_log(
                    f"东证Tick校正(平仓): {price:.2f} → {aligned:.2f}"
                )
                price = aligned

        normalized_order_type = to_vt_order_type(front_order_type, requested_order_type or OrderType.LIMIT)

        # 分批下单
        remaining = qty
        order_ids: List[str] = []

        for close_exchange_code, mt in sorted(groups.keys()):
            if remaining <= 0:
                break

            plist = groups[(close_exchange_code, mt)]
            close_positions = []
            group_qty = 0

            for p in plist:
                if remaining <= 0:
                    break

                hold_id = str(p.get("ExecutionID", "") or "")
                try:
                    leaves = int(float(p.get("LeavesQty", 0) or 0))
                except Exception:
                    leaves = 0

                if leaves <= 0 or not hold_id:
                    continue

                take = min(leaves, remaining)
                close_positions.append({"HoldID": hold_id, "Qty": take})
                group_qty += take
                remaining -= take

            if group_qty <= 0:
                continue

            # 构建返済订单（按持仓所在市场返済）
            payload = {
                "Symbol": symbol,
                "Exchange": close_exchange_code,
                "SecurityType": 1,
                "Side": side,
                "CashMargin": 3,              # 返済
                "MarginTradeType": mt,
                "DelivType": close_deliv_type,  # 2 = お預り金（正常信用返済）
                "AccountType": self.account_type,
                "Qty": group_qty,
                "ClosePositions": close_positions,
                "FrontOrderType": front_order_type,
                "Price": price,
                "ExpireDay": 0,
            }

            if self.debug:
                self.gateway.write_log(f"平仓请求: {payload}")

            try:
                resp = self.session.post(f"{self.rest_base}/sendorder", json=payload, timeout=10)

                if resp.status_code == 200:
                    data = resp.json()
                    order_id = data.get("OrderId", "") or data.get("ID", "")
                    if order_id:
                        order_ids.append(order_id)
                        # 推送 OrderData（使用分组实际数量，而非原始总量）
                        if req is not None:
                            split_req = copy.copy(req)
                            split_req.volume = float(group_qty)
                            split_req.type = normalized_order_type
                            split_req.price = price
                            order = split_req.create_order_data(order_id, self.gateway.gateway_name)
                            order.status = Status.SUBMITTING
                            order.datetime = datetime.now()
                            self.gateway.on_order(order)
                            with self.orders_lock:
                                self.active_orders[order_id] = order
                    else:
                        self.gateway.write_log(
                            f"平仓失败: HTTP 200但无OrderId - API响应: {data} | "
                            f"请求: Symbol={payload.get('Symbol')} Px={payload.get('Price')} "
                            f"Qty={payload.get('Qty')} Exchange={payload.get('Exchange')}"
                        )
                elif resp.status_code == 400:
                    err_text = resp.text
                    self.gateway.write_log(f"平仓失败: HTTP 400 - {err_text}")
                    # Close limit orders can still be rejected by kabu on price/trigger checks.
                    # Retry once as market with the same ClosePositions to avoid exit deadlocks.
                    try:
                        err_data = resp.json()
                    except Exception:
                        err_data = {}
                    err_code = err_data.get("Code")
                    if err_code in (4002004, 4002017) and payload.get("FrontOrderType") == 20:
                        self.gateway.write_log(
                            f"平仓降级: 限价 Price={payload['Price']} 被拒绝({err_code})，"
                            f"自动改为成行(FrontOrderType=10, Price=0)重试"
                        )
                        mkt_payload = dict(payload)
                        mkt_payload["FrontOrderType"] = 10  # 成行
                        mkt_payload["Price"] = 0.0
                        try:
                            mkt_resp = self.session.post(
                                f"{self.rest_base}/sendorder", json=mkt_payload, timeout=10
                            )
                            if mkt_resp.status_code == 200:
                                mkt_data = mkt_resp.json()
                                order_id = mkt_data.get("OrderId", "") or mkt_data.get("ID", "")
                                if order_id:
                                    order_ids.append(order_id)
                                    self.gateway.write_log(f"平仓成行重试成功: order_id={order_id}")
                                    if req is not None:
                                        split_req = copy.copy(req)
                                        split_req.volume = float(group_qty)
                                        split_req.type = OrderType.MARKET
                                        split_req.price = 0.0
                                        order = split_req.create_order_data(order_id, self.gateway.gateway_name)
                                        order.status = Status.SUBMITTING
                                        order.datetime = datetime.now()
                                        self.gateway.on_order(order)
                                        with self.orders_lock:
                                            self.active_orders[order_id] = order
                                else:
                                    self.gateway.write_log(f"平仓成行重试失败: {mkt_data}")
                            else:
                                self.gateway.write_log(
                                    f"平仓成行重试失败: HTTP {mkt_resp.status_code} - {mkt_resp.text}"
                                )
                        except Exception as me:
                            self.gateway.write_log(f"平仓成行重试异常: {me}")
                else:
                    self.gateway.write_log(f"平仓失败: HTTP {resp.status_code} - {resp.text}")

            except Exception as e:
                self.gateway.write_log(f"平仓异常: {e}")

        if remaining > 0:
            self.gateway.write_log(f"平仓提示: 需要平{qty}股,实际仅平{qty - remaining}股,剩余{remaining}股")

        return order_ids[0] if order_ids else ""

    def cancel_order(self, req: CancelRequest) -> None:
        """撤单"""
        if not self.token:
            self._get_token()
            if not self.token:
                self.gateway.write_log("撤单失败: 无Token")
                return

        url = f"{self.rest_base}/cancelorder"

        # 构建撤单请求
        payload = {
            "OrderId": req.orderid,  # Kabu的订单ID
        }

        if self.debug:
            self.gateway.write_log(f"撤单请求: {payload}")

        try:
            resp = self.session.put(url, json=payload, timeout=10)

            if resp.status_code != 200:
                # 解析错误信息
                try:
                    error_data = resp.json()
                    error_code = error_data.get("Code", -1)
                    error_msg = error_data.get("Message", "")

                    # Code 43: 該当注文は既に約定済です (订单已成交)
                    if error_code == 43:
                        self.gateway.write_log(
                            f"⚠️ 撤单失败(订单已成交): {req.orderid} - {error_msg}"
                        )
                        # 立即补查订单状态并触发成交回报
                        self._补查单笔订单成交(req.orderid)
                    else:
                        self.gateway.write_log(f"撤单失败: HTTP {resp.status_code} - {resp.text}")
                except Exception:
                    self.gateway.write_log(f"撤单失败: HTTP {resp.status_code} - {resp.text}")
                return

            data = resp.json()
            result = data.get("Result", -1)

            if result == 0:
                self.gateway.write_log(f"撤单成功: {req.orderid}")
            else:
                self.gateway.write_log(f"撤单失败: {data}")

        except Exception as e:
            self.gateway.write_log(f"撤单异常: {e}")
            if self.debug:
                self.gateway.write_log(traceback.format_exc())

    def _补查单笔订单成交(self, order_id: str) -> None:
        """
        撤单失败时补查单笔订单的成交状态
        用于处理"订单已成交但WS回报丢失"的场景
        """
        if not self.token:
            return

        url = f"{self.rest_base}/orders/{order_id}"

        try:
            resp = self.session.get(url, timeout=10)

            if resp.status_code != 200:
                if self.debug:
                    self.gateway.write_log(f"补查订单失败: HTTP {resp.status_code}")
                return

            order_info = resp.json()

            # 检查是否已成交
            state = int(order_info.get("State", 1))
            if state not in (3, 4):  # 3=部分成交, 4=全部成交
                return

            # 获取跟踪的订单信息
            with self.orders_lock:
                tracked_order = self.active_orders.get(order_id)
                if not tracked_order:
                    return

            # 解析成交数量和价格
            exec_qty = float(order_info.get("ExecutedQty", 0) or order_info.get("CumQty", 0) or 0)
            exec_price = float(order_info.get("ExecutedPrice", 0) or
                              order_info.get("Price", 0) or
                              tracked_order.price)

            # 计算未报告的成交量
            unreported_volume = exec_qty - tracked_order.traded

            if unreported_volume > 0:
                # 生成成交回报
                exec_id = f"{order_id}_补{int(exec_qty)}"
                trade = TradeData(
                    symbol=tracked_order.symbol,
                    exchange=tracked_order.exchange,
                    orderid=order_id,
                    tradeid=exec_id,
                    direction=tracked_order.direction,
                    offset=tracked_order.offset,
                    price=exec_price,
                    volume=unreported_volume,
                    datetime=datetime.now(),
                    gateway_name=self.gateway.gateway_name,
                )

                # 推送成交回报
                self.gateway.on_trade(trade)

                # 更新成交时间戳(用于持仓查询降频优化)
                self._recent_trade_time = time.time()
                self._has_position = True

                self.gateway.write_log(
                    f"✅ [补查成交] {tracked_order.symbol} {tracked_order.direction.value} "
                    f"价格={exec_price} 数量={unreported_volume} OrderId={order_id}"
                )

                # 更新订单状态
                status = Status.ALLTRADED if state == 4 else Status.PARTTRADED
                updated_order = OrderData(
                    symbol=tracked_order.symbol,
                    exchange=tracked_order.exchange,
                    orderid=order_id,
                    direction=tracked_order.direction,
                    offset=tracked_order.offset,
                    type=tracked_order.type,
                    price=tracked_order.price,
                    volume=tracked_order.volume,
                    traded=exec_qty,
                    status=status,
                    datetime=datetime.now(),
                    gateway_name=self.gateway.gateway_name,
                )
                self.gateway.on_order(updated_order)

                # 更新跟踪记录
                with self.orders_lock:
                    if status == Status.ALLTRADED:
                        self.active_orders.pop(order_id, None)
                    else:
                        self.active_orders[order_id] = updated_order

        except Exception as e:
            if self.debug:
                self.gateway.write_log(f"补查订单异常: {e}")
                self.gateway.write_log(traceback.format_exc())

    def get_positions(self, product: str = "0", symbol: Optional[str] = None) -> List[dict]:
        """
        查询持仓
        product: 0=全部, 1=現物, 2=信用
        """
        if not self.token:
            self._get_token()
            if not self.token:
                return []

        url = f"{self.rest_base}/positions"
        params = {"product": product}
        if symbol:
            params["symbol"] = symbol

        try:
            resp = self.session.get(url, params=params, timeout=10)

            if resp.status_code != 200:
                if self.debug:
                    self.gateway.write_log(f"查询持仓失败: HTTP {resp.status_code} - {resp.text}")
                return []

            data = resp.json()
            return data if isinstance(data, list) else []

        except Exception as e:
            if self.debug:
                self.gateway.write_log(f"查询持仓异常: {e}")
            return []

    def query_position(self) -> None:
        """查询并推送持仓数据"""
        try:
            # 查询现物和信用持仓
            cash_positions = self.get_positions(product="1")
            margin_positions = self.get_positions(product="2")

            # 聚合持仓 (symbol, exchange, direction) -> (volume, avg_price)
            agg: Dict[Tuple[str, Exchange, Direction], Dict[str, float]] = {}

            # 持仓数量计数(用于优化降频)
            total_position_volume = 0.0

            # 处理现物持仓 (统一为多头)
            for p in cash_positions:
                symbol = str(p.get("Symbol", "")).strip()
                if not symbol:
                    continue

                try:
                    qty = int(float(p.get("LeavesQty", 0) or 0))
                except Exception:
                    qty = 0

                if qty <= 0:
                    continue

                ex = kabu_exchange_code_to_vt_exchange(p.get("Exchange", 1))
                key = (symbol, ex, Direction.LONG)

                if key not in agg:
                    agg[key] = {"volume": 0.0, "price_sum": 0.0, "count": 0.0}

                price = float(p.get("Price", 0) or 0)
                agg[key]["volume"] += qty
                agg[key]["price_sum"] += price * qty
                agg[key]["count"] += qty

            # 处理信用持仓 (区分多空)
            for p in margin_positions:
                symbol = str(p.get("Symbol", "")).strip()
                if not symbol:
                    continue

                try:
                    qty = int(float(p.get("LeavesQty", 0) or 0))
                except Exception:
                    qty = 0

                if qty <= 0:
                    continue

                side = str(p.get("Side", "2"))
                direction = to_vt_direction(side)
                ex = kabu_exchange_code_to_vt_exchange(p.get("Exchange", 1))
                key = (symbol, ex, direction)

                if key not in agg:
                    agg[key] = {"volume": 0.0, "price_sum": 0.0, "count": 0.0}

                price = float(p.get("Price", 0) or 0)
                agg[key]["volume"] += qty
                agg[key]["price_sum"] += price * qty
                agg[key]["count"] += qty

            # 推送持仓
            for (symbol, ex, direction), v in agg.items():
                vol = v["volume"]
                if vol <= 0:
                    continue

                avg_price = (v["price_sum"] / v["count"]) if v["count"] > 0 else 0.0

                pos = PositionData(
                    symbol=symbol,
                    exchange=ex,
                    direction=direction,
                    volume=vol,
                    price=avg_price,
                    gateway_name=self.gateway.gateway_name,
                )
                self.gateway.on_position(pos)

                # 统计总持仓量
                total_position_volume += vol

            # 更新持仓标志(用于降频优化)
            self._has_position = (total_position_volume > 0)

            if self.debug:
                self.gateway.write_log(f"持仓查询完成: {len(agg)}个持仓, 总量={total_position_volume}")

        except Exception as e:
            self.gateway.write_log(f"查询持仓异常: {e}")
            if self.debug:
                self.gateway.write_log(traceback.format_exc())

    def query_account(self) -> None:
        """查询账户资金 (待实现)"""
        if self.debug:
            self.gateway.write_log("账户查询功能暂未实现,可对接/wallet/cash接口")

    def query_orders(self) -> None:
        """
        查询订单状态并自动补发成交回报
        关键修复:解决WS成交回报丢失导致策略无法挂止盈单的问题
        """
        if not self.token:
            return

        # 获取需要查询的订单列表
        with self.orders_lock:
            if not self.active_orders:
                return
            orders_to_check = list(self.active_orders.values())

        url = f"{self.rest_base}/orders"

        try:
            resp = self.session.get(url, timeout=10)

            if resp.status_code != 200:
                if self.debug:
                    self.gateway.write_log(f"查询订单失败: HTTP {resp.status_code} - {resp.text}")
                return

            data = resp.json()
            if not isinstance(data, list):
                return

            # 处理每个订单状态
            for order_info in data:
                order_id = str(order_info.get("ID", "") or order_info.get("OrderId", ""))
                if not order_id:
                    continue

                # 检查是否是我们跟踪的订单
                with self.orders_lock:
                    if order_id not in self.active_orders:
                        continue
                    tracked_order = self.active_orders[order_id]

                # 解析订单状态
                state = int(order_info.get("State", 1))
                status = STATUS_KABU2VT.get(state, Status.SUBMITTING)

                # 解析成交数量
                exec_qty = float(order_info.get("ExecutedQty", 0) or order_info.get("CumQty", 0) or 0)
                order_qty = float(order_info.get("OrderQty", 0) or tracked_order.volume)

                # 检测到新成交
                if exec_qty > tracked_order.traded:
                    # 检查WS是否已推送该成交量（防止重复推送）
                    # WS推送的_ws_pushed_order_traded记录了该订单通过WS已推送的成交量
                    ws_pushed_qty = self._ws_pushed_order_traded.get(order_id, 0.0)
                    if exec_qty <= ws_pushed_qty:
                        # WS已推送了相同或更多的成交量，REST轮询跳过推送
                        if self.debug:
                            self.gateway.write_log(
                                f"[REST轮询跳过] {order_id} exec_qty={exec_qty} "
                                f"WS已推送={ws_pushed_qty}，避免重复成交回报"
                            )
                        # 仍需更新tracked_order状态
                        pass
                    else:
                        # WS未推送（WS断线或未推送），由REST补发
                        # 计算扣除WS已推送后的增量
                        already_tracked = max(tracked_order.traded, ws_pushed_qty)
                        trade_volume = exec_qty - already_tracked

                        if trade_volume <= 0:
                            pass  # 无需补发
                        else:
                            # 获取成交价格
                            exec_price = float(order_info.get("ExecutedPrice", 0) or
                                              order_info.get("Price", 0) or
                                              tracked_order.price)

                            # 生成成交回报（REST补发，tradeid用Q前缀区分）
                            exec_id = f"{order_id}_Q{int(exec_qty)}"  # 生成唯一成交ID
                            trade = TradeData(
                                symbol=tracked_order.symbol,
                                exchange=tracked_order.exchange,
                                orderid=order_id,
                                tradeid=exec_id,
                                direction=tracked_order.direction,
                                offset=tracked_order.offset,
                                price=exec_price,
                                volume=trade_volume,
                                datetime=datetime.now(),
                                gateway_name=self.gateway.gateway_name,
                            )

                            # 推送成交回报 (触发策略on_trade)
                            self.gateway.on_trade(trade)

                            # 更新成交时间戳(用于持仓查询降频优化)
                            self._recent_trade_time = time.time()
                            self._has_position = True

                            if self.debug:
                                self.gateway.write_log(
                                    f"✅ [REST补发成交] {tracked_order.symbol} {tracked_order.direction.value} "
                                    f"价格={exec_price} 数量={trade_volume} OrderId={order_id}"
                                )

                # 更新订单状态
                updated_order = OrderData(
                    symbol=tracked_order.symbol,
                    exchange=tracked_order.exchange,
                    orderid=order_id,
                    direction=tracked_order.direction,
                    offset=tracked_order.offset,
                    type=tracked_order.type,
                    price=tracked_order.price,
                    volume=order_qty,
                    traded=exec_qty,
                    status=status,
                    datetime=datetime.now(),
                    gateway_name=self.gateway.gateway_name,
                )
                self.gateway.on_order(updated_order)

                # 更新跟踪记录
                with self.orders_lock:
                    self.active_orders[order_id] = updated_order

                # 如果订单已完成,从跟踪列表移除
                if status in (Status.ALLTRADED, Status.CANCELLED, Status.REJECTED):
                    with self.orders_lock:
                        self.active_orders.pop(order_id, None)

        except Exception as e:
            if self.debug:
                self.gateway.write_log(f"查询订单异常: {e}")
                self.gateway.write_log(traceback.format_exc())

    def register(self, symbols: List[Tuple[str, Exchange]]) -> bool:
        """
        注册行情推送
        最多支持50个股票
        """
        if not self.token:
            self._get_token()
            if not self.token:
                self.gateway.write_log("行情注册失败: 无Token")
                return False

        url = f"{self.rest_base}/register"

        # 限制50个
        if len(symbols) > 50:
            self.gateway.write_log(f"行情注册数量超限,最多50个,当前{len(symbols)}个")
            symbols = symbols[:50]

        # ✅ 关键修复：启用成交回报推送（ExecutionDetailsPush）
        # 根据Kabu API文档，addinfo参数应该在payload顶层
        # ⚠️ 注意：/register行情订阅的Exchange必须=1(東証)，
        #   不能用9(SOR)或27(東証+)！SOR/東証+仅用于/sendorder发单路由，
        #   行情订阅API不接受路由代码，否则返回 HTTP 400 (Code:4001018)
        payload = {
            "Symbols": [
                {
                    "Symbol": s,
                    "Exchange": 1  # 行情订阅固定用1(東証)，与发单路由Exchange(9/27)无关
                }
                for s, ex in symbols
            ],
            "addinfo": True  # ✅ 正确位置：顶层启用ExecutionDetailsPush
        }

        if self.debug:
            self.gateway.write_log(f"行情注册: {[s for s, _ in symbols]}")
            self.gateway.write_log(f"🔍 [Register] 完整Payload: {json.dumps(payload, ensure_ascii=False)}")

        try:
            resp = self.session.put(url, json=payload, timeout=10)

            if resp.status_code == 200:
                if self.debug:
                    self.gateway.write_log(f"✅ [Register] 注册成功，响应: {resp.text[:300] if resp.text else 'empty'}")
                return True
            else:
                self.gateway.write_log(f"行情注册失败: HTTP {resp.status_code} - {resp.text}")
                return False

        except Exception as e:
            self.gateway.write_log(f"行情注册异常: {e}")
            if self.debug:
                self.gateway.write_log(traceback.format_exc())
            return False

    def close(self) -> None:
        """关闭连接"""
        self.session.close()


# ================= WebSocket行情推送 =================

class KabuWebsocketApi:
    """
    Kabu Station WebSocket PUSH API

    功能:
    - 订阅实时行情
    - 推送Tick数据
    - 自动重连
    """

    MAX_SYMBOLS = 50

    def __init__(self, gateway: "KabuGateway", rest_api: KabuRestApi) -> None:
        self.gateway = gateway
        self.rest_api = rest_api

        # WebSocket配置
        self.ws_url: str = ""
        self.token: str = ""
        self.debug: bool = False

        # 连接状态
        self.active: bool = False
        self.thread: Optional[Thread] = None
        self.ws: Optional[websocket.WebSocketApp] = None

        # 订阅管理
        self._lock = Lock()
        self.subscribed: Dict[str, Exchange] = {}  # symbol -> exchange

        # 日志节流 (修复Bug: 必须初始化否则AttributeError异常风暴)
        self._last_log_time: float = 0.0
        self._log_sample_rate: float = 1.0  # 每秒最多1条详细日志

    def connect(self, setting: dict) -> None:
        """连接WebSocket"""
        # 解析WebSocket地址
        host = setting.get("REST_HOST", "http://localhost:18080").rstrip("/")
        rest_base = host if host.endswith("/kabusapi") else host + "/kabusapi"
        self.ws_url = rest_base.replace("https", "wss").replace("http", "ws") + "/websocket"

        # 获取Token
        self.token = self.rest_api.token
        self.debug = bool(setting.get("DEBUG", False))

        self.gateway.write_log(f"KABU WebSocket: {self.ws_url}")

        # 启动线程
        self.active = True
        self.thread = Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self) -> None:
        """WebSocket主循环 (自动重连)"""
        while self.active:
            try:
                headers = [f"X-API-KEY: {self.token}"]

                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    header=headers,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open,
                )

                self.gateway.write_log("WebSocket连接中...")
                self.ws.run_forever()

            except Exception as e:
                self.gateway.write_log(f"WebSocket异常: {e}")
                if self.debug:
                    self.gateway.write_log(traceback.format_exc())

            # 自动重连
            if self.active:
                self.gateway.write_log("WebSocket 5秒后重连...")
                time.sleep(5)

    def on_open(self, ws: websocket.WebSocketApp) -> None:
        """WebSocket连接成功 - 立即重新注册所有符号（含addinfo=True）

        ⚠️ 关键修复：
        Kabu的ExecutionDetailsPush（成交回报推送）必须在WebSocket连接建立后，
        通过REST /register (含addinfo=True) 重新绑定到当前WS连接。
        若不在on_open中重新注册，则：
        - WS断线重连后成交推送失效
        - 程序启动时，subscribe()早于WS建立，addinfo绑定到旧连接或根本未绑定
        → on_trade永远不会被触发 → entry_price/entry_time缺失 → 无法挂止盈止损单
        """
        self.gateway.write_log("WebSocket已连接，正在重新注册行情订阅（含成交推送）...")
        # 在新线程中注册，避免阻塞WebSocket消息循环
        from threading import Thread
        t = Thread(target=self._register_all, daemon=True)
        t.start()

    def on_message(self, ws: websocket.WebSocketApp, message) -> None:
        """处理WebSocket消息"""
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")

            data = json.loads(message)

            # 🔍 诊断日志：记录所有WebSocket消息的键 (节流采样,避免卡顿)
            if self.debug:
                current_time = time.time()
                # 节流:每秒最多打印1条详细日志
                if current_time - self._last_log_time >= self._log_sample_rate:
                    self._last_log_time = current_time
                    msg_keys = list(data.keys())
                    self.gateway.write_log(f"[WS消息采样] 收到字段: {msg_keys}")
                    # 如果消息很短，打印完整内容
                    if len(str(data)) < 300:
                        self.gateway.write_log(f"[WS消息采样] 完整内容: {data}")

            # ✅ 修复：判断消息类型
            # ExecutionDetailsPush (成交回报) 有 OrderId/ExecutionId 字段
            # BoardPush (行情) 有 Symbol + CurrentPrice 字段
            if "ExecutionId" in data or "OrderId" in data:
                self.gateway.write_log(f"🔔 检测到成交回报消息！")
                self.on_push_execution(data)  # 处理成交回报
            else:
                self.on_push_tick(data)  # 处理行情数据

        except Exception as e:
            if self.debug:
                self.gateway.write_log(f"WebSocket消息处理异常: {e}")
                self.gateway.write_log(traceback.format_exc())

    def on_error(self, ws: websocket.WebSocketApp, error) -> None:
        """WebSocket错误"""
        self.gateway.write_log(f"WebSocket错误: {error}")

    def on_close(self, ws: websocket.WebSocketApp, close_status_code, close_msg) -> None:
        """WebSocket关闭"""
        self.gateway.write_log(f"WebSocket关闭: code={close_status_code}, msg={close_msg}")

    def on_push_tick(self, data: dict) -> None:
        """
        处理PUSH行情数据

        Kabu PUSH字段:
        - Symbol: 股票代码
        - SymbolName: 股票名称
        - CurrentPrice: 现价
        - CurrentPriceTime: 时间
        - BidPrice/BidQty: 买价/买量
        - AskPrice/AskQty: 卖价/卖量
        - OpeningPrice: 开盘价
        - HighPrice: 最高价
        - LowPrice: 最低价
        - TradingVolume: 成交量
        """
        symbol = data.get("Symbol", "")
        if not symbol:
            return

        # 获取交易所
        with self._lock:
            exchange = self.subscribed.get(symbol, DEFAULT_EXCHANGE)

        # 股票名称
        name = data.get("SymbolName", "")
        self.rest_api.update_symbol_price_range_group(symbol, data.get("PriceRangeGroup"))

        # 如果PUSH数据中有名称,更新合约信息
        if name:
            vt_symbol = f"{symbol}.{exchange.value}"
            # 检查是否需要更新合约名称
            if symbol in self.gateway._registered_symbols:
                # 推送更新后的合约信息(名称更准确)
                updated_contract = ContractData(
                    symbol=symbol,
                    exchange=exchange,
                    name=name,
                    product=Product.EQUITY,
                    size=100,
                    pricetick=detect_tse_pricetick_from_market_data(data),
                    gateway_name=self.gateway.gateway_name,
                )
                self.gateway.on_contract(updated_contract)

        # 解析时间
        dt = parse_kabu_datetime(data.get("CurrentPriceTime"))

        # 构建Tick
        tick = TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=dt,
            name=name,
            volume=int(data.get("TradingVolume") or 0),
            turnover=0.0,
            open_price=float(data.get("OpeningPrice") or 0),
            high_price=float(data.get("HighPrice") or 0),
            low_price=float(data.get("LowPrice") or 0),
            last_price=float(data.get("CurrentPrice") or 0),
            gateway_name=self.gateway.gateway_name,
        )

        # 最优一档
        if data.get("BidPrice") is not None:
            tick.bid_price_1 = float(data["BidPrice"])
            tick.bid_volume_1 = int(data.get("BidQty") or 0)

        if data.get("AskPrice") is not None:
            tick.ask_price_1 = float(data["AskPrice"])
            tick.ask_volume_1 = int(data.get("AskQty") or 0)

        # 推送Tick
        self.gateway.on_tick(tick)

    def on_push_execution(self, data: dict) -> None:
        """
        处理成交回报推送（ExecutionDetailsPush）

        Kabu成交回报字段：
        - OrderId: 订单ID (如 "20260114A02N99968149")
        - ExecutionId: 成交ID (如 "E20260114XXXXX")
        - Symbol: 股票代码
        - SymbolName: 股票名称
        - Side: 买卖方向 (1=卖出, 2=买入) ⚠️ 注意：Kabu API中 1=卖 2=买
        - CashMargin: 现物/信用区分 (1=现物, 2=信用新規/开仓, 3=信用返済/平仓)
        - Qty: 成交数量
        - Price: 成交价格
        - ExecutionDay: 成交日 (YYYYMMDD)
        - ExecutionTime: 成交时间 (YYYYMMDDHHmmss)
        - DelivDay: 受渡日
        """
        order_id = data.get("OrderId", "")
        execution_id = data.get("ExecutionId", "")

        if not order_id or not execution_id:
            return

        symbol = data.get("Symbol", "")
        if not symbol:
            return

        # 获取交易所
        with self._lock:
            exchange = self.subscribed.get(symbol, DEFAULT_EXCHANGE)

        # ✅ 修复：正确解析买卖方向（Kabu API: 1=卖, 2=买）
        side = str(data.get("Side", ""))
        if side == "2":
            direction = Direction.LONG  # 买入
        elif side == "1":
            direction = Direction.SHORT  # 卖出
        else:
            self.gateway.write_log(f"⚠️ 未知的Side值: {side}")
            return

        # ✅ 修复：智能判断开平仓（结合CashMargin和当前持仓）
        cash_margin = int(data.get("CashMargin", 2))  # 默认2=信用新規

        # 优先使用CashMargin判断
        if cash_margin == 3:
            offset = Offset.CLOSE  # 信用返済 = 平仓
        elif cash_margin == 2:
            offset = Offset.OPEN  # 信用新規 = 开仓
        elif cash_margin == 1:
            # 现物交易：需要根据方向推断开平仓
            # 买入(LONG) = 开仓，卖出(SHORT) = 平仓
            if direction == Direction.LONG:
                offset = Offset.OPEN
            elif direction == Direction.SHORT:
                offset = Offset.CLOSE  # 现物卖出通常是平多仓
            else:
                offset = Offset.NONE
        else:
            offset = Offset.NONE

        # 解析时间
        exec_time_str = data.get("ExecutionTime", "")
        if exec_time_str:
            try:
                exec_dt = datetime.strptime(exec_time_str, "%Y%m%d%H%M%S")
            except Exception:
                exec_dt = datetime.now()
        else:
            exec_dt = datetime.now()

        # 创建TradeData
        trade = TradeData(
            symbol=symbol,
            exchange=exchange,
            orderid=order_id,
            tradeid=execution_id,
            direction=direction,
            offset=offset,
            price=float(data.get("Price", 0)),
            volume=float(data.get("Qty", 0)),
            datetime=exec_dt,
            gateway_name=self.gateway.gateway_name,
        )

        # ✅ 关键：推送成交回报（登记ExecutionId防止REST轮询重复推送）
        self.rest_api._ws_pushed_execution_ids.add(execution_id)
        # 同时登记该订单的成交量，REST轮询可用于跳过
        exec_qty = float(data.get("Qty", 0))
        prev = self.rest_api._ws_pushed_order_traded.get(order_id, 0.0)
        self.rest_api._ws_pushed_order_traded[order_id] = prev + exec_qty

        self.gateway.on_trade(trade)

        if self.debug:
            self.gateway.write_log(
                f"✅ [成交回报] {symbol} {direction.value} {offset.value} "
                f"价格={trade.price} 数量={trade.volume} "
                f"OrderId={order_id} ExecutionId={execution_id}"
            )

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        symbol, ex = normalize_symbol_exchange(req.symbol, req.exchange)

        with self._lock:
            # 已订阅则跳过
            if symbol in self.subscribed:
                return

            self.subscribed[symbol] = ex

        # 注册到Kabu服务器
        self._register_all()

    def _register_all(self) -> None:
        """注册所有已订阅的股票"""
        with self._lock:
            pairs = list(self.subscribed.items())

        if not pairs:
            return

        # 限制50个
        if len(pairs) > self.MAX_SYMBOLS:
            self.gateway.write_log(
                f"订阅数量超限(最多{self.MAX_SYMBOLS}个),仅注册前{self.MAX_SYMBOLS}个"
            )
            pairs = pairs[:self.MAX_SYMBOLS]

        # 调用REST API注册
        symbols = [(s, ex) for s, ex in pairs]
        success = self.rest_api.register(symbols)

        if success:
            self.gateway.write_log(f"行情注册成功: {[s for s, _ in symbols]}")

    def close(self) -> None:
        """关闭WebSocket"""
        self.active = False
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass


# ================= VeighNa Gateway主类 =================

class KabuGateway(BaseGateway):
    """
    Kabu Station Gateway for VeighNa

    连接配置项:
    - REST_HOST: Kabu Station地址 (默认: http://localhost:18080)
    - API_PASSWORD: API密码
    - ACCOUNT_TYPE: 账户类型 (2=一般, 4=特定)
    - DELIV_TYPE: 受渡区分 (2=お預り金)
    - FUND_TYPE: 資金区分 (02=保護)
    - USE_MARGIN: 是否使用信用交易
    - MARGIN_TRADE_TYPE: 信用交易类型 (1=制度信用)
    - DEBUG: 调试模式
    """

    default_name = "KABU"

    default_setting = {
        "REST_HOST": "http://localhost:18080",
        "API_PASSWORD": "",
        "ACCOUNT_TYPE": 4,
        "DELIV_TYPE": 2,
        "FUND_TYPE": "02",
        "USE_MARGIN": False,
        "MARGIN_TRADE_TYPE": 1,
        "ORDER_EXCHANGE": 9,    # 新规发单Exchange: 9=SOR, 27=東証+ (2026-02-28规格变更)
        "DEBUG": False,
    }

    exchanges = [DEFAULT_EXCHANGE]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "KABU") -> None:
        super().__init__(event_engine, gateway_name)

        # API实例
        self.rest_api = KabuRestApi(self)
        self.ws_api = KabuWebsocketApi(self, self.rest_api)

        # 已注册合约
        self._registered_symbols: Set[str] = set()

    def write_log(self, msg: str) -> None:
        """写日志"""
        log = LogData(msg=msg, gateway_name=self.gateway_name)
        self.on_log(log)

    def connect(self, setting: dict) -> None:
        """连接Gateway"""
        # 连接REST API
        self.rest_api.connect(setting)

        # 连接WebSocket
        self.ws_api.connect(setting)

        # 注册预定义合约
        self.init_contracts()

        # 注册定时器事件
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

        self.write_log("KABU Gateway连接完成")

    def close(self) -> None:
        """关闭Gateway"""
        self.rest_api.close()
        self.ws_api.close()
        self.write_log("KABU Gateway已关闭")

    def init_contracts(self) -> None:
        """初始化预定义合约"""
        if not KABU_STOCK_CONTRACTS:
            self.write_log("未配置预定义合约,将在订阅时动态创建")
            return

        for symbol, name, pricetick in KABU_STOCK_CONTRACTS:
            price_range_group = self.rest_api.get_symbol_price_range_group(symbol)
            contract = ContractData(
                symbol=symbol,
                exchange=DEFAULT_EXCHANGE,
                name=name,
                product=Product.EQUITY,
                size=100,
                pricetick=get_minimum_tse_tick(price_range_group) if price_range_group else float(pricetick),
                gateway_name=self.gateway_name,
                history_data=False,
            )
            self.on_contract(contract)
            self._registered_symbols.add(symbol)

        if KABU_STOCK_CONTRACTS:
            self.write_log(f"预注册{len(KABU_STOCK_CONTRACTS)}个合约")

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情

        支持在UI界面输入股票代码回车订阅:
        - 自动创建合约对象
        - 注册到Kabu服务器
        - 接收WebSocket推送
        """
        symbol, ex = normalize_symbol_exchange(req.symbol, req.exchange)
        vt_symbol = f"{symbol}.{ex.value}"

        # 动态注册合约
        if symbol not in self._registered_symbols:
            price_range_group = self.rest_api.get_symbol_price_range_group(symbol)
            contract = ContractData(
                symbol=symbol,
                exchange=ex,
                name=symbol,  # 默认用代码,后续从行情更新
                product=Product.EQUITY,
                size=100,
                pricetick=get_minimum_tse_tick(price_range_group) if price_range_group else 1.0,
                gateway_name=self.gateway_name,
                history_data=False,
            )
            self.on_contract(contract)
            self._registered_symbols.add(symbol)
            self.write_log(f"动态注册合约: {vt_symbol}")

        # 订阅WebSocket行情
        self.ws_api.subscribe(SubscribeRequest(symbol=symbol, exchange=ex))

    def send_order(self, req: OrderRequest) -> str:
        """
        发送订单

        重要: 返回 vt_orderid (格式: gateway_name.order_id)
        VeighNa会自动拆分为gateway_name和order_id
        """
        order_id = self.rest_api.send_order(req)

        if not order_id:
            return ""

        # 返回 vt_orderid
        return f"{self.gateway_name}.{order_id}"

    def cancel_order(self, req: CancelRequest) -> None:
        """撤单"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """查询账户"""
        self.rest_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.rest_api.query_position()

    def query_orders(self) -> None:
        """查询订单状态"""
        self.rest_api.query_orders()

    def query_position_adaptive(self) -> None:
        """
        自适应持仓查询 (降频优化)

        策略:
        - 有活跃订单: 每秒查询(及时发现成交后的持仓变化)
        - 有持仓: 每3秒查询
        - 无持仓无订单: 每5秒查询
        - 最近1分钟有成交: 提高到每2秒
        """
        current_time = time.time()

        # 计算动态查询间隔
        with self.rest_api.orders_lock:
            has_active_orders = bool(self.rest_api.active_orders)

        # 最近1分钟有成交,提高频率
        time_since_trade = current_time - self.rest_api._recent_trade_time
        recently_traded = time_since_trade < 60.0

        if has_active_orders:
            # 有活跃订单: 每秒查询
            interval = 1.0
        elif recently_traded:
            # 最近有成交: 每2秒查询
            interval = 2.0
        elif self.rest_api._has_position:
            # 有持仓: 每3秒查询
            interval = 3.0
        else:
            # 空仓无订单: 每5秒查询
            interval = 5.0

        # 检查是否到达查询时间
        time_since_last = current_time - self.rest_api._last_position_query_time
        if time_since_last >= interval:
            self.rest_api._last_position_query_time = current_time
            self.query_position()

    def process_timer_event(self, event) -> None:
        """
        定时任务 (每秒触发)

        实现订单状态轮询,确保成交回报不丢失
        优化策略:
        - 订单查询: 有活跃订单时每秒查询
        - 持仓查询: 自适应降频(3-5秒/次,有成交后暂时提高频率)
        """
        # 订单查询: 只在有活跃订单时才查询
        self.query_orders()

        # 持仓查询: 自适应降频
        self.query_position_adaptive()
