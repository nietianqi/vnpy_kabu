"""
VeighNa Gateway for Kabu Securities (日本 kabuステーション API)

Author: AI Assistant
Date: 2025
License: MIT

功能特性:
- 日本股票现物交易 (现货交易)
- 日本股票信用交易 (融资融券)
- WebSocket实时行情推送
- 动态合约注册 (UI输入股票代码即可订阅)
- 持仓查询和账户查询
- 完整的订单管理

使用方法:
1. 在VeighNa主引擎中添加Gateway:
   from vnpy_kabu import KabuGateway
   main_engine.add_gateway(KabuGateway)

2. 在交易界面连接配置:
   - REST_HOST: http://localhost:18080
   - API_PASSWORD: 您的kabuステーション API密码
   - 其他参数使用默认值即可

3. 在行情窗口输入股票代码订阅:
   - 输入 "7203" 回车即可订阅丰田汽车
   - 输入 "7014" 回车即可订阅名村造船
"""

from .kabu_gateway import KabuGateway

# Gateway类导出 (VeighNa要求)
gateway_class = KabuGateway

__all__ = ["KabuGateway", "gateway_class"]
__version__ = "1.0.0"
