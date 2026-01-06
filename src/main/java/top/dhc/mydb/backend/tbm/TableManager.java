package top.dhc.mydb.backend.tbm;

import top.dhc.mydb.backend.dm.DataManager;
import top.dhc.mydb.backend.parser.statement.Begin;
import top.dhc.mydb.backend.parser.statement.Create;
import top.dhc.mydb.backend.parser.statement.Delete;
import top.dhc.mydb.backend.parser.statement.Insert;
import top.dhc.mydb.backend.parser.statement.Select;
import top.dhc.mydb.backend.parser.statement.Update;
import top.dhc.mydb.backend.utils.Parser;
import top.dhc.mydb.backend.vm.VersionManager;

/**
 * 表管理器接口（Table Manager）
 * 
 * TBM层是数据库系统的最上层，负责：
 * 1. 管理数据库中的表和字段的元数据
 * 2. 解析和执行SQL语句（CREATE、INSERT、SELECT、UPDATE、DELETE等）
 * 3. 管理事务的开始、提交和回滚
 * 4. 维护表的索引结构（B+树）
 * 
 * 在数据库系统架构中的位置：
 * - 位于VM（版本管理器）之上
 * - 接收来自Server层的SQL语句
 * - 将SQL操作转换为对VM层的调用
 * 
 * 核心功能：
 * 1. 事务管理：begin()、commit()、abort()
 * 2. 表管理：create()、show()
 * 3. 数据操作：insert()、read()、update()、delete()
 * 
 * 数据存储结构：
 * - 使用Booter存储第一个表的UID
 * - 每个表通过UID链式连接
 * - 每个表包含多个字段，字段也通过UID存储
 * - 支持字段索引，使用B+树实现
 */
public interface TableManager {
    /**
     * 开始一个事务
     * 
     * @param begin 开始事务的语句对象，包含隔离级别信息
     * @return 事务开始结果，包含事务ID和执行结果
     */
    BeginRes begin(Begin begin);
    
    /**
     * 提交事务
     * 
     * @param xid 事务ID
     * @return 提交结果的字节数组
     * @throws Exception 提交过程中的异常
     */
    byte[] commit(long xid) throws Exception;
    
    /**
     * 回滚事务
     * 
     * @param xid 事务ID
     * @return 回滚结果的字节数组
     */
    byte[] abort(long xid);

    /**
     * 显示所有表的信息
     * 
     * @param xid 事务ID
     * @return 表信息的字节数组
     */
    byte[] show(long xid);
    
    /**
     * 创建表
     * 
     * @param xid 事务ID
     * @param create 创建表的语句对象，包含表名、字段定义、索引信息
     * @return 创建结果的字节数组
     * @throws Exception 创建过程中的异常
     */
    byte[] create(long xid, Create create) throws Exception;

    /**
     * 插入数据
     * 
     * @param xid 事务ID
     * @param insert 插入语句对象，包含表名和要插入的值
     * @return 插入结果的字节数组
     * @throws Exception 插入过程中的异常
     */
    byte[] insert(long xid, Insert insert) throws Exception;
    
    /**
     * 查询数据（SELECT）
     * 
     * @param xid 事务ID
     * @param select 查询语句对象，包含表名、字段列表、WHERE条件
     * @return 查询结果的字节数组
     * @throws Exception 查询过程中的异常
     */
    byte[] read(long xid, Select select) throws Exception;
    
    /**
     * 更新数据
     * 
     * @param xid 事务ID
     * @param update 更新语句对象，包含表名、SET子句、WHERE条件
     * @return 更新结果的字节数组
     * @throws Exception 更新过程中的异常
     */
    byte[] update(long xid, Update update) throws Exception;
    
    /**
     * 删除数据
     * 
     * @param xid 事务ID
     * @param delete 删除语句对象，包含表名和WHERE条件
     * @return 删除结果的字节数组
     * @throws Exception 删除过程中的异常
     */
    byte[] delete(long xid, Delete delete) throws Exception;

    /**
     * 创建新的表管理器
     * 
     * 用于初始化全新的数据库：
     * 1. 创建Booter文件，存储第一个表的UID（初始为0）
     * 2. 创建TableManagerImpl实例
     * 
     * @param path 数据库文件路径（不含后缀）
     * @param vm 版本管理器
     * @param dm 数据管理器
     * @return 表管理器实例
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        // 创建Booter文件，用于存储第一个表的UID
        Booter booter = Booter.create(path);
        // 初始化第一个表的UID为0（表示还没有表）
        booter.update(Parser.long2Byte(0));
        // 创建表管理器实现
        return new TableManagerImpl(vm, dm, booter);
    }

    /**
     * 打开已存在的表管理器
     * 
     * 用于打开已有的数据库：
     * 1. 打开Booter文件，读取第一个表的UID
     * 2. 创建TableManagerImpl实例
     * 
     * @param path 数据库文件路径（不含后缀）
     * @param vm 版本管理器
     * @param dm 数据管理器
     * @return 表管理器实例
     */
    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        // 打开Booter文件
        Booter booter = Booter.open(path);
        // 创建表管理器实现
        return new TableManagerImpl(vm, dm, booter);
    }
}
