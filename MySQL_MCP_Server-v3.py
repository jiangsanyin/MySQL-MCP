#!/usr/bin/env python3
"""
MySQL MCP Server - 配置优化版本
提供MySQL数据库的创建表、增删改查等操作功能
支持通过环境变量和命令行参数配置数据库连接
"""

import asyncio
import json
import logging
import sys
import os
import argparse
from typing import Any, Dict, List, Optional

import aiomysql
from mcp.server.fastmcp import FastMCP
from mcp.server.stdio import stdio_server

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)  # 输出到stderr避免与MCP通信冲突
    ]
)
logger = logging.getLogger(__name__)

## 从环境变量获取端口，如果没有则使用默认端口
port = int(os.environ.get('FC_SERVER_PORT', '9000'))
host = os.environ.get('FC_SERVER_HOST', '0.0.0.0')
# 在创建FastMCP实例时传递host和port参数
app = FastMCP("mysql-mcp-server", host=host, port=port)
#app = FastMCP("mysql-mcp-server")

class MySQLManager:
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        """
        初始化MySQL管理器
        
        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
        """
        # 优先使用传入的参数，然后是环境变量，最后是默认值
        self.db_config = {
            'host': host or os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(port or os.getenv('MYSQL_PORT', 3306)),
            'user': user or os.getenv('MYSQL_USER', 'root'),
            'password': password or os.getenv('MYSQL_PASSWORD', 'root@123'),
            'db': database or os.getenv('MYSQL_DATABASE', 'test01'),
            'charset': 'utf8mb4',
            'autocommit': True
        }
        self.pool = None
        self._lock = asyncio.Lock()
        
        # 验证必要的配置
        if not self.db_config['password']:
            logger.warning("数据库密码为空，请确保这是预期的配置")
    
    def get_config_info(self):
        """获取配置信息（隐藏密码）"""
        config_info = self.db_config.copy()
        config_info['password'] = '*' * len(config_info['password']) if config_info['password'] else 'Empty'
        return config_info
    
    async def get_connection(self):
        """获取数据库连接"""
        async with self._lock:
            if not self.pool:
                try:
                    logger.info(f"正在连接数据库: {self.db_config['host']}:{self.db_config['port']}")
                    self.pool = await aiomysql.create_pool(
                        host=self.db_config['host'],
                        port=self.db_config['port'],
                        user=self.db_config['user'],
                        password=self.db_config['password'],
                        db=self.db_config['db'],
                        charset=self.db_config['charset'],
                        autocommit=self.db_config['autocommit'],
                        minsize=1,
                        maxsize=5,
                        connect_timeout=10,
                        pool_recycle=3600
                    )
                    logger.info("数据库连接池创建成功")
                except Exception as e:
                    logger.error(f"创建数据库连接池失败: {e}")
                    raise
        
        try:
            conn = await self.pool.acquire()
            return conn
        except Exception as e:
            logger.error(f"获取数据库连接失败: {e}")
            raise
    
    async def release_connection(self, conn):
        """释放数据库连接"""
        if conn and self.pool:
            try:
                await self.pool.release(conn)
            except Exception as e:
                logger.error(f"释放数据库连接失败: {e}")
    
    async def close_pool(self):
        """关闭连接池"""
        if self.pool:
            try:
                self.pool.close()
                await self.pool.wait_closed()
                self.pool = None
                logger.info("数据库连接池已关闭")
            except Exception as e:
                logger.error(f"关闭连接池失败: {e}")

# 全局MySQL管理器实例（将在main函数中初始化）
mysql_manager = None

@app.tool()
async def create_table(table_name: str, columns: List[Dict[str, str]]) -> str:
    """
    创建MySQL数据表
    
    Args:
        table_name: 表名
        columns: 列定义数组，每个元素包含name(列名)、type(数据类型)、constraints(约束条件，可选)
    
    Returns:
        创建结果消息
    """
    conn = None
    try:
        conn = await mysql_manager.get_connection()
        async with conn.cursor() as cursor:
            # 构建CREATE TABLE语句
            column_defs = []
            for col in columns:
                col_def = f"`{col['name']}` {col['type']}"
                if col.get('constraints'):
                    col_def += f" {col['constraints']}"
                column_defs.append(col_def)
            
            sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(column_defs)})"
            await cursor.execute(sql)
            logger.info(f"表 '{table_name}' 创建成功")
            return f"表 '{table_name}' 创建成功"
    
    except Exception as e:
        error_msg = f"创建表失败: {str(e)}"
        logger.error(error_msg)
        return error_msg
    finally:
        if conn:
            await mysql_manager.release_connection(conn)

@app.tool()
async def insert_data(table_name: str, data: Dict[str, Any]) -> str:
    """
    向表中插入数据
    
    Args:
        table_name: 表名
        data: 要插入的数据，键值对形式
    
    Returns:
        插入结果消息
    """
    conn = None
    try:
        conn = await mysql_manager.get_connection()
        async with conn.cursor() as cursor:
            columns = list(data.keys())
            values = list(data.values())
            placeholders = ', '.join(['%s'] * len(values))
            column_names = ', '.join([f"`{col}`" for col in columns])
            
            sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"
            await cursor.execute(sql, values)
            result_msg = f"数据插入成功，受影响行数: {cursor.rowcount}"
            logger.info(result_msg)
            return result_msg
    
    except Exception as e:
        error_msg = f"插入数据失败: {str(e)}"
        logger.error(error_msg)
        return error_msg
    finally:
        if conn:
            await mysql_manager.release_connection(conn)

@app.tool()
async def select_data(table_name: str, columns: Optional[List[str]] = None, 
                     where_clause: str = "", limit: int = 100) -> str:
    """
    查询表中的数据
    
    Args:
        table_name: 表名
        columns: 要查询的列名，默认为所有列
        where_clause: WHERE条件子句
        limit: 限制返回行数
    
    Returns:
        查询结果的JSON字符串
    """
    if columns is None:
        columns = ["*"]
    
    conn = None
    try:
        conn = await mysql_manager.get_connection()
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            if columns == ["*"]:
                column_str = "*"
            else:
                column_str = ', '.join([f"`{col}`" for col in columns])
            
            sql = f"SELECT {column_str} FROM `{table_name}`"
            if where_clause:
                sql += f" WHERE {where_clause}"
            sql += f" LIMIT {limit}"
            
            await cursor.execute(sql)
            results = await cursor.fetchall()
            
            if not results:
                return "未找到数据"
            
            # 格式化结果
            formatted_results = []
            for row in results:
                formatted_results.append(dict(row))
            
            return json.dumps(formatted_results, ensure_ascii=False, indent=2, default=str)
    
    except Exception as e:
        error_msg = f"查询数据失败: {str(e)}"
        logger.error(error_msg)
        return error_msg
    finally:
        if conn:
            await mysql_manager.release_connection(conn)

@app.tool()
async def update_data(table_name: str, data: Dict[str, Any], where_clause: str) -> str:
    """
    更新表中的数据
    
    Args:
        table_name: 表名
        data: 要更新的数据，键值对形式
        where_clause: WHERE条件子句
    
    Returns:
        更新结果消息
    """
    conn = None
    try:
        conn = await mysql_manager.get_connection()
        async with conn.cursor() as cursor:
            set_clauses = [f"`{key}` = %s" for key in data.keys()]
            set_clause = ', '.join(set_clauses)
            values = list(data.values())
            
            sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
            await cursor.execute(sql, values)
            result_msg = f"数据更新成功，受影响行数: {cursor.rowcount}"
            logger.info(result_msg)
            return result_msg
    
    except Exception as e:
        error_msg = f"更新数据失败: {str(e)}"
        logger.error(error_msg)
        return error_msg
    finally:
        if conn:
            await mysql_manager.release_connection(conn)

@app.tool()
async def delete_data(table_name: str, where_clause: str) -> str:
    """
    删除表中的数据
    
    Args:
        table_name: 表名
        where_clause: WHERE条件子句
    
    Returns:
        删除结果消息
    """
    conn = None
    try:
        conn = await mysql_manager.get_connection()
        async with conn.cursor() as cursor:
            sql = f"DELETE FROM `{table_name}` WHERE {where_clause}"
            await cursor.execute(sql)
            result_msg = f"数据删除成功，受影响行数: {cursor.rowcount}"
            logger.info(result_msg)
            return result_msg
    
    except Exception as e:
        error_msg = f"删除数据失败: {str(e)}"
        logger.error(error_msg)
        return error_msg
    finally:
        if conn:
            await mysql_manager.release_connection(conn)

@app.tool()
async def show_tables() -> str:
    """
    显示所有数据表
    
    Returns:
        表列表字符串
    """
    conn = None
    try:
        conn = await mysql_manager.get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute("SHOW TABLES")
            tables = await cursor.fetchall()
            
            if not tables:
                return "数据库中没有表"
            
            table_list = [table[0] for table in tables]
            return f"数据库中的表({len(table_list)}个):\n" + '\n'.join([f"- {table}" for table in table_list])
    
    except Exception as e:
        error_msg = f"显示表失败: {str(e)}"
        logger.error(error_msg)
        return error_msg
    finally:
        if conn:
            await mysql_manager.release_connection(conn)

@app.tool()
async def get_database_info() -> str:
    """
    获取当前数据库连接信息和状态
    
    Returns:
        数据库信息
    """
    conn = None
    try:
        conn = await mysql_manager.get_connection()
        async with conn.cursor() as cursor:
            # 获取数据库版本
            await cursor.execute("SELECT VERSION()")
            version = await cursor.fetchone()
            
            # 获取当前数据库
            await cursor.execute("SELECT DATABASE()")
            current_db = await cursor.fetchone()
            
            # 获取连接信息
            config_info = mysql_manager.get_config_info()
            info = {
                **config_info,
                "mysql_version": version[0] if version else "Unknown",
                "current_database": current_db[0] if current_db else "None",
                "connection_status": "Connected"
            }
            
            return json.dumps(info, ensure_ascii=False, indent=2)
    
    except Exception as e:
        error_msg = f"获取数据库信息失败: {str(e)}"
        logger.error(error_msg)
        return error_msg
    finally:
        if conn:
            await mysql_manager.release_connection(conn)

async def test_connection():
    """测试数据库连接"""
    try:
        logger.info("正在测试数据库连接...")
        conn = await mysql_manager.get_connection()
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            result = await cursor.fetchone()
            if result[0] == 1:
                logger.info("数据库连接测试成功")
                return True
        await mysql_manager.release_connection(conn)
    except Exception as e:
        logger.error(f"数据库连接测试失败: {e}")
        return False
    return False

def parse_arguments():
    """解析命令行参数，如果未提供则从环境变量中读取"""
    parser = argparse.ArgumentParser(description='MySQL MCP Server')
    parser.add_argument('--host', type=str, help='MySQL host (default: localhost)')
    parser.add_argument('--port', type=int, help='MySQL port (default: 3306)')
    parser.add_argument('--user', type=str, help='MySQL user (default: root)')
    parser.add_argument('--password', type=str, help='MySQL password')
    parser.add_argument('--database', type=str, help='MySQL database name (default: test01)')
    
    args = parser.parse_args()
    
    # 如果命令行参数未提供，则从环境变量中读取
    if args.host is None:
        args.host = os.getenv('MYSQL_HOST')
    if args.port is None:
        port_env = os.getenv('MYSQL_PORT')
        if port_env:
            args.port = int(port_env)
    if args.user is None:
        args.user = os.getenv('MYSQL_USER')
    if args.password is None:
        args.password = os.getenv('MYSQL_PASSWORD')
    if args.database is None:
        args.database = os.getenv('MYSQL_DATABASE')
    
    return args

async def main():
    """主函数"""
    global mysql_manager
    
    try:
        logger.info("MySQL MCP Server 启动中...")
        
        # 解析命令行参数
        args = parse_arguments()
        
        # 初始化MySQL管理器
        mysql_manager = MySQLManager(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database
        )
        
        # 显示配置信息
        config_info = mysql_manager.get_config_info()
        logger.info(f"数据库配置: {config_info}")
        
        # 测试数据库连接
        if not await test_connection():
            logger.error("数据库连接失败，服务器无法启动")
            return
        
        # 使用stdio服务器运行MCP应用
        async with stdio_server() as (read_stream, write_stream):
            logger.info("MCP服务器正在运行...")
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
    
    except KeyboardInterrupt:
        logger.info("服务器收到中断信号，正在关闭...")
    except Exception as e:
        logger.error(f"服务器运行错误: {e}", exc_info=True)
    finally:
        # 清理资源
        if mysql_manager:
            await mysql_manager.close_pool()
        logger.info("服务器已关闭")

if __name__ == "__main__":
    try:
        # 解析命令行参数并初始化MySQL管理器
        args = parse_arguments()
        mysql_manager = MySQLManager(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database
        )
        
        # 运行FastMCP应用
        app.run(transport="sse")
    except Exception as e:
        logger.error(f"程序启动失败: {e}", exc_info=True)
        sys.exit(1)
