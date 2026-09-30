"""
数据接收层 - 零延迟WebSocket数据接收

职责：
- 接收WebSocket推送的订单簿和Ticker数据
- 立即入队，不做任何处理
- 确保零延迟、零阻塞
"""

import asyncio
import logging
from typing import Dict, Callable, Optional, Any
from datetime import datetime
from collections import defaultdict

from core.adapters.exchanges.models import OrderBookData, TickerData
from core.services.arbitrage_monitor.utils.symbol_converter import SimpleSymbolConverter
from ..config.debug_config import DebugConfig


class DataReceiver:
    """
    数据接收器 - 零延迟设计
    
    设计原则：
    1. 回调函数只做最小验证 + 入队操作
    2. 不进行任何计算或复杂处理
    3. 使用put_nowait避免阻塞
    4. 队列满时丢弃旧数据（保证实时性）
    """
    
    def __init__(
        self,
        orderbook_queue: asyncio.Queue,
        ticker_queue: asyncio.Queue,
        debug_config: DebugConfig
    ):
        """
        初始化数据接收器
        
        Args:
            orderbook_queue: 订单簿队列
            ticker_queue: Ticker队列
            debug_config: Debug配置
        """
        self.orderbook_queue = orderbook_queue
        self.ticker_queue = ticker_queue
        self.debug = debug_config
        self.logger = logging.getLogger(__name__)
        
        # 统计信息
        self.stats = {
            'orderbook_received': 0,
            'orderbook_dropped': 0,
            'ticker_received': 0,
            'ticker_dropped': 0,
            # 🔥 网络流量统计（字节数）
            'network_bytes_received': 0,  # 接收的字节数
            'network_bytes_sent': 0,      # 发送的字节数
        }
        
        # Debug计数器
        self._ws_message_counter = 0
        
        # 适配器注册表
        self.adapters: Dict[str, Any] = {}
        
        # 🚀 Symbol转换器（参考V1）
        logger = logging.getLogger(__name__)
        self.symbol_converter = SimpleSymbolConverter(logger)
        logger.info("✅ Symbol转换器已初始化")
    
    def register_adapter(self, exchange: str, adapter: Any):
        """
        注册交易所适配器
        
        Args:
            exchange: 交易所名称
            adapter: 交易所适配器
        """
        self.adapters[exchange] = adapter
        print(f"✅ [{exchange}] 适配器已注册到数据接收层")
    
    async def subscribe_all(self, symbols: list):
        """
        订阅所有交易对的数据
        
        Args:
            symbols: 交易对列表（标准格式，如 BTC-USDC-PERP）
        
        扩展说明：
        ============================================================
        🔥 新交易所接入指南
        ============================================================
        1. 如果新交易所的回调格式与标准格式相同（callback(symbol, data)）：
           - 无需修改，会自动使用 else 分支的标准订阅模式
        
        2. 如果新交易所的回调格式不同：
           - 在 subscribe_all 方法中添加新的 elif 分支
           - 参考 Lighter 和 EdgeX 的实现方式
           - 确保回调函数正确转换 symbol 并验证数据
        
        3. 回调格式说明：
           - 标准格式：callback(symbol: str, orderbook: OrderBookData)
           - Lighter格式：callback(orderbook: OrderBookData) - 只有orderbook参数
           - EdgeX格式：callback(orderbook: OrderBookData) - 只有orderbook参数
        ============================================================
        """
        # print(f"\n🔍 [DataReceiver] 开始订阅，已注册的适配器: {list(self.adapters.keys())}")
        # print(f"🔍 [DataReceiver] 要订阅的symbols: {symbols}\n")
        
        for exchange, adapter in self.adapters.items():
            try:
                # print(f"\n📡 [DataReceiver] 正在处理交易所: {exchange}")
                # print(f"🔍 [DataReceiver] 适配器类型: {type(adapter).__name__}")
                
                # ============================================================
                # 🔥 交易所特殊处理扩展点
                # ============================================================
                # 如果新交易所的回调格式与标准格式不同，在这里添加特殊处理
                # ============================================================
                
                # 🚀 Lighter特殊处理：使用批量订阅模式（参考EdgeX的实现）
                if exchange in ("lighter", "lighter_rh"):
                    # 🔥 固定 exchange 值，避免闭包变量捕获问题
                    exchange_name = exchange
                    
                    # 创建Lighter专用的统一回调（只有一个参数）
                    # 🔥 使用默认参数绑定，避免闭包捕获问题
                    def lighter_orderbook_callback(orderbook, _exchange_name=exchange_name):
                        """Lighter订单簿统一回调（只接收orderbook参数）"""
                        try:
                            # orderbook.symbol 可能是Lighter格式（如 "BTC"）或标准格式（如 "BTC-USDC-PERP"）
                            # 需要尝试转换，如果转换失败则使用原始symbol
                            try:
                                std_symbol = self.symbol_converter.convert_from_exchange(orderbook.symbol, "lighter")
                            except Exception:
                                # 如果转换失败，可能是已经是标准格式，直接使用
                                std_symbol = orderbook.symbol
                            
                            # 检查symbol是否在监控列表中
                            if std_symbol in symbols:
                                # 直接验证并入队
                                try:
                                    # 验证数据
                                    if not orderbook.best_bid or not orderbook.best_ask:
                                        return  # 静默忽略
                                    
                                    if orderbook.best_bid.price <= 0 or orderbook.best_ask.price <= 0:
                                        return  # 静默忽略
                                    
                                    # 直接入队（使用固定的 _exchange_name）
                                    self.orderbook_queue.put_nowait({
                                        'exchange': _exchange_name,
                                        'symbol': std_symbol,
                                        'data': orderbook,
                                        'timestamp': datetime.now()
                                    })
                                    self.stats['orderbook_received'] += 1
                                except Exception:
                                    self.stats['orderbook_dropped'] = self.stats.get('orderbook_dropped', 0) + 1
                        except Exception:
                            self.stats['orderbook_dropped'] = self.stats.get('orderbook_dropped', 0) + 1
                    
                    # 🔥 使用默认参数绑定，避免闭包捕获问题
                    # 🔥 保存 self 的引用，在闭包中使用
                    receiver_self = self
                    
                    def lighter_ticker_callback(ticker, _exchange_name=exchange_name):
                        """Lighter ticker统一回调（只接收ticker参数）"""
                        try:
                            # 转换symbol到标准格式
                            std_symbol = receiver_self.symbol_converter.convert_from_exchange(ticker.symbol, "lighter")
                            
                            # 检查symbol是否在监控列表中
                            if std_symbol in symbols:
                                # 直接入队，避免二次符号转换
                                receiver_self.ticker_queue.put_nowait({
                                    'exchange': _exchange_name,
                                    'symbol': std_symbol,
                                    'data': ticker,
                                    'timestamp': datetime.now()
                                })
                                receiver_self.stats['ticker_received'] += 1
                            else:
                                # 符号不在监控列表（只记录一次）
                                if not hasattr(receiver_self, '_lighter_ticker_symbol_mismatch_log'):
                                    receiver_self._lighter_ticker_symbol_mismatch_log = True
                                    receiver_self.logger.warning(f"⚠️ [DataReceiver] Lighter ticker symbol不在监控列表: std_symbol={std_symbol}, symbols={symbols}")
                        except Exception as e:
                            receiver_self.logger.error(f"❌ [DataReceiver] lighter ticker回调失败: {e}", exc_info=True)
                    
                    # 转换所有符号为Lighter格式
                    exchange_symbols = []
                    for standard_symbol in symbols:
                        try:
                            # 两个 Lighter 实例使用相同的合约符号规则，但保留独立交易所 ID。
                            exchange_symbol = self.symbol_converter.convert_to_exchange(standard_symbol, "lighter")
                            exchange_symbols.append(exchange_symbol)
                        except Exception:
                            pass  # 静默处理符号转换错误
                    
                    # 使用批量订阅方法（设置统一回调，所有符号共享）
                    if exchange_symbols:
                        await adapter.batch_subscribe_orderbooks(exchange_symbols, callback=lighter_orderbook_callback)
                        await adapter.batch_subscribe_tickers(exchange_symbols, callback=lighter_ticker_callback)
                
                elif exchange == "edgex":
                    # EdgeX特殊处理：使用批量订阅模式（设置全局回调）
                    await asyncio.sleep(5)  # 给EdgeX 5秒时间加载metadata
                    
                    # 🔥 固定 exchange 值，避免闭包变量捕获问题
                    exchange_name_edgex = "edgex"
                    
                    # 🔥 创建EdgeX专用的统一回调（兼容两种调用方式）
                    # EdgeX会同时调用全局回调和特定订阅回调：
                    # - 全局回调：_safe_callback_with_symbol(callback, symbol, orderbook) - 传递两个参数
                    # - 特定订阅回调：_safe_callback(callback, orderbook) - 只传递一个参数
                    # 所以我们需要创建一个包装函数，能够处理两种情况
                    # 🔥 使用默认参数绑定，避免闭包捕获问题
                    async def edgex_orderbook_callback_wrapper(*args, _exchange_name=exchange_name_edgex):
                        """EdgeX订单簿回调包装器（兼容两种调用方式，异步）"""
                        try:
                            # 如果只有一个参数，说明是从特定订阅回调调用的（只有orderbook）
                            # 如果有两个参数，说明是从全局回调调用的（symbol, orderbook）
                            if len(args) == 1:
                                # 只有orderbook，需要从orderbook中提取symbol
                                orderbook = args[0]
                                symbol = orderbook.symbol if hasattr(orderbook, 'symbol') else None
                                if not symbol:
                                    return  # 无法处理，静默忽略
                            elif len(args) == 2:
                                # 有symbol和orderbook
                                symbol, orderbook = args
                            else:
                                return  # 参数错误，静默忽略
                            
                            # 🔥 从symbol转换为标准格式
                            std_symbol = self.symbol_converter.convert_from_exchange(symbol, _exchange_name)
                            
                            # 🔥 检查symbol是否在监控列表中
                            if std_symbol in symbols:
                                # 调用标准回调（需要symbol和orderbook两个参数）
                                self._create_orderbook_callback(_exchange_name)(std_symbol, orderbook)
                        except Exception as e:
                            if self.debug.is_debug_enabled():
                                print(f"❌ [edgex] 订单簿回调失败: {e}")
                    
                    # 🔥 使用默认参数绑定，避免闭包捕获问题
                    async def edgex_ticker_callback_wrapper(*args, _exchange_name=exchange_name_edgex):
                        """EdgeX ticker回调包装器（兼容两种调用方式，异步）"""
                        try:
                            # Ticker回调通常有两个参数 (symbol, ticker)
                            if len(args) == 2:
                                symbol, ticker = args
                                # EdgeX 已经提供了symbol，只需要转换
                                std_symbol = self.symbol_converter.convert_from_exchange(symbol, _exchange_name)
                                if std_symbol in symbols:
                                    self._create_ticker_callback(_exchange_name)(std_symbol, ticker)
                        except Exception as e:
                            if self.debug.is_debug_enabled():
                                print(f"❌ [edgex] ticker回调失败: {e}")
                    
                    # 转换所有符号为EdgeX格式
                    exchange_symbols = []
                    for standard_symbol in symbols:
                        try:
                            exchange_symbol = self.symbol_converter.convert_to_exchange(standard_symbol, exchange)
                            exchange_symbols.append(exchange_symbol)
                        except Exception:
                            pass  # 静默处理符号转换错误
                    
                    # 使用批量订阅方法
                    if exchange_symbols:
                        await adapter.websocket.batch_subscribe_orderbooks(exchange_symbols, callback=edgex_orderbook_callback_wrapper)
                        await adapter.websocket.batch_subscribe_tickers(exchange_symbols, callback=edgex_ticker_callback_wrapper)
                
                elif exchange == "backpack":
                    # Backpack特殊处理：使用批量订阅模式（设置全局回调）
                    # Backpack的回调格式：
                    # - 批量订阅回调：callback(symbol, orderbook) - 两个参数
                    # - 单独订阅回调：callback(orderbook) - 只有一个参数
                    # 所以我们需要创建一个包装器函数来兼容两种调用方式
                    
                    # 🔥 固定 exchange 值，避免闭包变量捕获问题
                    exchange_name_backpack = "backpack"
                    
                    # 创建Backpack专用的统一回调包装器（兼容两种调用方式）
                    # 🔥 使用默认参数绑定，避免闭包捕获问题
                    def backpack_orderbook_callback_wrapper(*args, _exchange_name=exchange_name_backpack):
                        """Backpack订单簿回调包装器（兼容两种调用方式）"""
                        try:
                            if len(args) == 2:
                                # 批量订阅回调：传递了 (symbol, orderbook)
                                symbol, orderbook = args
                            elif len(args) == 1:
                                # 单独订阅回调：只传递了 (orderbook)
                                orderbook = args[0]
                                symbol = orderbook.symbol
                            else:
                                return
                            
                            # 转换symbol到标准格式
                            std_symbol = self.symbol_converter.convert_from_exchange(symbol, _exchange_name)
                            
                            # 检查symbol是否在监控列表中
                            if std_symbol in symbols:
                                # 调用标准回调（使用固定的 _exchange_name）
                                callback = self._create_orderbook_callback(_exchange_name)
                                callback(std_symbol, orderbook)
                        except Exception as e:
                            # 静默处理错误，避免UI刷屏
                            self.stats['orderbook_dropped'] = self.stats.get('orderbook_dropped', 0) + 1
                    
                    # 🔥 使用默认参数绑定，避免闭包捕获问题
                    def backpack_ticker_callback_wrapper(*args, _exchange_name=exchange_name_backpack):
                        """Backpack ticker回调包装器（兼容两种调用方式）"""
                        try:
                            if len(args) == 2:
                                # 批量订阅回调：传递了 (symbol, ticker)
                                symbol, ticker = args
                            elif len(args) == 1:
                                # 单独订阅回调：只传递了 (ticker)
                                ticker = args[0]
                                # 从ticker中提取symbol
                                symbol = ticker.symbol
                            else:
                                return
                            
                            # 🔥 V1逻辑：先转换symbol
                            std_symbol = self.symbol_converter.convert_from_exchange(symbol, _exchange_name)
                            
                            # 🔥 V1逻辑：检查symbol是否在监控列表中
                            if std_symbol in symbols:
                                # 调用标准回调（使用固定的 _exchange_name）
                                callback = self._create_ticker_callback(_exchange_name)
                                callback(std_symbol, ticker)
                        except Exception as e:
                            if self.debug.is_debug_enabled():
                                print(f"⚠️  [backpack] ticker回调失败: {e}")
                    
                    # 转换所有符号为Backpack格式
                    exchange_symbols = []
                    for standard_symbol in symbols:
                        try:
                            exchange_symbol = self.symbol_converter.convert_to_exchange(standard_symbol, exchange)
                            exchange_symbols.append(exchange_symbol)
                        except Exception as e:
                            pass  # 静默处理符号转换错误
                    
                    # 批量订阅方法（BackpackAdapter的batch_subscribe_orderbooks和batch_subscribe_tickers）
                    if exchange_symbols:
                        await adapter.batch_subscribe_orderbooks(exchange_symbols, callback=backpack_orderbook_callback_wrapper)
                        await adapter.batch_subscribe_tickers(exchange_symbols, callback=backpack_ticker_callback_wrapper)
                
                else:
                    # ============================================================
                    # 🔥 通用交易所订阅模式（占位符）
                    # ============================================================
                    # 大多数交易所使用标准订阅模式：
                    # - subscribe_orderbook(symbol, callback) - callback(symbol, orderbook)
                    # - subscribe_ticker(symbol, callback) - callback(symbol, ticker)
                    #
                    # 如果新交易所的回调格式不同，可以在这里添加特殊处理：
                    # if exchange == "new_exchange":
                    #     # 新交易所的特殊处理逻辑
                    #     pass
                    # ============================================================
                    
                    # 标准订阅模式（两个参数：symbol, callback）
                    for standard_symbol in symbols:
                        try:
                            exchange_symbol = self.symbol_converter.convert_to_exchange(standard_symbol, exchange)
                            await adapter.subscribe_orderbook(
                                symbol=exchange_symbol,
                                callback=self._create_orderbook_callback(exchange)
                            )
                        except Exception:
                            pass  # 静默处理订阅错误
                    
                    for standard_symbol in symbols:
                        try:
                            exchange_symbol = self.symbol_converter.convert_to_exchange(standard_symbol, exchange)
                            await adapter.subscribe_ticker(
                                symbol=exchange_symbol,
                                callback=self._create_ticker_callback(exchange)
                            )
                        except Exception:
                            pass  # 静默处理订阅错误
                
                print(f"✅ [{exchange}] 已订阅 {len(symbols)} 个交易对")
                
            except Exception as e:
                print(f"❌ [{exchange}] 订阅失败: {e}")
    
    def _create_orderbook_callback(self, exchange: str) -> Callable:
        """
        创建订单簿回调函数
        
        Args:
            exchange: 交易所名称
            
        Returns:
            回调函数
        """
        def callback(symbol: str, orderbook: OrderBookData):
            """
            订单簿回调 - 零延迟设计
            
            Args:
                symbol: 交易对
                orderbook: 订单簿数据
            """
            # 🚀 统一转换为标准符号（保证各层一致）
            std_symbol = self._normalize_symbol(symbol, exchange)

            # 🚀 快速验证（检查必需字段）
            # 所有交易所统一验证：必须同时有有效的bid和ask
            # （Backpack现在在适配器层维护完整的本地订单簿）
            if not orderbook.best_bid or not orderbook.best_ask:
                return  # 静默忽略
            
            if orderbook.best_bid.price <= 0 or orderbook.best_ask.price <= 0:
                return  # 静默忽略
            
            # 🚀 立即入队（非阻塞）
            received_at = datetime.now()
            exchange_timestamp = getattr(orderbook, 'exchange_timestamp', None) or getattr(orderbook, 'timestamp', None)
            # 标注时间链路
            orderbook.exchange_timestamp = exchange_timestamp
            orderbook.received_timestamp = received_at
            try:
                self.orderbook_queue.put_nowait({
                    'exchange': exchange,
                    'symbol': std_symbol,
                    'data': orderbook,
                    'exchange_timestamp': exchange_timestamp,
                    'received_at': received_at,
                    'timestamp': received_at  # 兼容旧字段
                })
                self.stats['orderbook_received'] += 1
                
                # 🔥 Debug输出已禁用（避免刷屏和NoneType错误）
                # if exchange == "backpack":
                #     bid_price = orderbook.best_bid.price if orderbook.best_bid else None
                #     ask_price = orderbook.best_ask.price if orderbook.best_ask else None
                #     print(f"✅ [DEBUG] Backpack数据已入队: {exchange} {symbol} (bid={bid_price}, ask={ask_price})")
                
                # if self.debug.show_ws_messages and self.debug.should_show_ws_message(self._ws_message_counter):
                #     print(f"📥 [{exchange}] {symbol} 订单簿: Bid={orderbook.best_bid.price:.2f} Ask={orderbook.best_ask.price:.2f}")
                
                self._ws_message_counter += 1
                
            except asyncio.QueueFull:
                # 队列满了，丢弃最旧的数据
                try:
                    self.orderbook_queue.get_nowait()
                    self.orderbook_queue.put_nowait({
                        'exchange': exchange,
                        'symbol': std_symbol,
                        'data': orderbook,
                        'timestamp': datetime.now()
                    })
                except:
                    pass
                self.stats['orderbook_dropped'] += 1
        
        return callback
    
    def _create_ticker_callback(self, exchange: str) -> Callable:
        """
        创建Ticker回调函数
        
        Args:
            exchange: 交易所名称
            
        Returns:
            回调函数
        """
        def callback(symbol: str, ticker: TickerData):
            """
            Ticker回调 - 零延迟设计
            
            Args:
                symbol: 交易对
                ticker: Ticker数据
            """
            # 🚀 统一转换为标准符号
            std_symbol = self._normalize_symbol(symbol, exchange)

            # 🚀 立即入队（非阻塞）
            try:
                self.ticker_queue.put_nowait({
                    'exchange': exchange,
                    'symbol': std_symbol,
                    'data': ticker,
                    'timestamp': datetime.now()
                })
                self.stats['ticker_received'] += 1
                
            except asyncio.QueueFull:
                # 队列满了，丢弃最旧的数据
                try:
                    self.ticker_queue.get_nowait()
                    self.ticker_queue.put_nowait({
                        'exchange': exchange,
                        'symbol': std_symbol,
                        'data': ticker,
                        'timestamp': datetime.now()
                    })
                except:
                    pass
                self.stats['ticker_dropped'] += 1
        
        return callback

    def _normalize_symbol(self, symbol: str, exchange: str) -> str:
        """
        将任意格式的交易对转换为系统标准格式（BTC-USDC-PERP）
        """
        normalized = symbol
        try:
            normalized = self.symbol_converter.convert_from_exchange(symbol, exchange)
        except Exception:
            normalized = symbol

        if not normalized:
            return symbol

        candidate = normalized.replace('/', '-').replace(':', '-')
        return candidate.upper()
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        stats = self.stats.copy()
        
        # 🔥 从适配器获取网络流量统计和重连统计
        total_bytes_received = 0
        total_bytes_sent = 0
        reconnect_stats = {}  # {exchange: reconnect_count}
        
        for exchange, adapter in self.adapters.items():
            try:
                # 尝试从适配器的websocket获取网络流量统计和重连统计
                if hasattr(adapter, 'websocket') and adapter.websocket:
                    ws = adapter.websocket
                    if hasattr(ws, 'get_network_stats'):
                        net_stats = ws.get_network_stats()
                        total_bytes_received += net_stats.get('bytes_received', 0)
                        total_bytes_sent += net_stats.get('bytes_sent', 0)
                    
                    # 🔥 获取重连统计
                    if hasattr(ws, 'get_reconnect_stats'):
                        reconnect_stats[exchange] = ws.get_reconnect_stats().get('reconnect_count', 0)
            except Exception:
                pass  # 静默忽略错误
        
        # 更新网络流量统计
        stats['network_bytes_received'] = total_bytes_received
        stats['network_bytes_sent'] = total_bytes_sent
        
        # 🔥 更新重连统计
        stats['reconnect_stats'] = reconnect_stats
        
        return stats
    
    async def cleanup(self):
        """清理资源"""
        print("🧹 数据接收层正在清理...")
        for exchange, adapter in self.adapters.items():
            try:
                # 🔥 添加3秒超时，避免卡住
                await asyncio.wait_for(adapter.disconnect(), timeout=3.0)
                print(f"✅ [{exchange}] 已断开连接")
            except asyncio.TimeoutError:
                print(f"⏱️  [{exchange}] 断开连接超时，强制跳过")
            except Exception as e:
                print(f"⚠️  [{exchange}] 断开连接失败: {e}")
