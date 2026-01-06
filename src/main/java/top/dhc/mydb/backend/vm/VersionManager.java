package top.dhc.mydb.backend.vm;

import top.dhc.mydb.backend.dm.DataManager;
import top.dhc.mydb.backend.tm.TransactionManager;

/**
 * 版本管理器接口（Version Manager）
 * 
 * VM层实现了MVCC（多版本并发控制），负责：
 * 1. 管理数据的多个版本，实现读写并发
 * 2. 实现事务的隔离级别（读提交、可重复读）
 * 3. 处理事务的可见性判断
 * 4. 管理事务的快照（Snapshot）
 * 5. 实现死锁检测和避免
 * 
 * 在数据库系统架构中的位置：
 * - 位于TM（事务管理器）和DM（数据管理器）之上
 * - 为TBM（表管理器）提供版本化的数据访问接口
 * 
 * MVCC核心概念：
 * - Entry: 数据项的一个版本，包含XMIN（创建事务ID）和XMAX（删除事务ID）
 * - 可见性：根据事务的隔离级别和快照判断数据版本是否可见
 * - 版本链：同一个数据项的多个版本通过XMAX链接
 * 
 * 事务隔离级别：
 * - level 0: 读提交（Read Committed）
 * - level 1: 可重复读（Repeatable Read）
 * 
 * 工作流程：
 * 1. 读取数据时，根据事务隔离级别和快照判断可见性
 * 2. 插入数据时，创建新的Entry，设置XMIN为当前事务ID
 * 3. 删除数据时，设置Entry的XMAX为当前事务ID（逻辑删除）
 * 4. 提交时，更新事务状态，释放锁
 * 5. 回滚时，撤销所有修改，释放锁
 */
public interface VersionManager {
    /**
     * 读取数据
     * 
     * 根据MVCC规则判断数据版本是否可见：
     * - 检查Entry的XMIN和XMAX
     * - 根据事务隔离级别和快照判断可见性
     * - 如果不可见，返回null
     * 
     * @param xid 事务ID
     * @param uid 数据项的UID
     * @return 数据内容的字节数组，如果不可见则返回null
     * @throws Exception 读取过程中的异常
     */
    byte[] read(long xid, long uid) throws Exception;
    
    /**
     * 插入数据
     * 
     * 创建新的Entry：
     * - 设置XMIN为当前事务ID
     * - 设置XMAX为0（表示未删除）
     * - 将数据写入DM层
     * 
     * @param xid 事务ID
     * @param data 要插入的数据
     * @return 新插入数据的UID
     * @throws Exception 插入过程中的异常
     */
    long insert(long xid, byte[] data) throws Exception;
    
    /**
     * 删除数据（逻辑删除）
     * 
     * 逻辑删除流程：
     * - 检查数据是否可见
     * - 获取锁，防止并发修改
     * - 设置Entry的XMAX为当前事务ID
     * - 如果数据已被其他事务删除，返回false
     * 
     * @param xid 事务ID
     * @param uid 要删除的数据项的UID
     * @return true表示删除成功，false表示数据不存在或已被删除
     * @throws Exception 删除过程中的异常（如死锁、并发冲突）
     */
    boolean delete(long xid, long uid) throws Exception;

    /**
     * 开始一个新事务
     * 
     * @param level 事务隔离级别（0=读提交，1=可重复读）
     * @return 新事务的XID
     */
    long begin(int level);
    
    /**
     * 提交事务
     * 
     * 提交流程：
     * - 检查事务是否有错误
     * - 从活跃事务列表中移除
     * - 释放所有锁
     * - 调用TM提交事务
     * 
     * @param xid 要提交的事务ID
     * @throws Exception 提交过程中的异常
     */
    void commit(long xid) throws Exception;
    
    /**
     * 回滚事务
     * 
     * 回滚流程：
     * - 从活跃事务列表中移除
     * - 释放所有锁
     * - 调用TM回滚事务
     * - DM层会自动撤销所有修改（通过日志）
     * 
     * @param xid 要回滚的事务ID
     */
    void abort(long xid);

    /**
     * 创建版本管理器实例
     * 
     * @param tm 事务管理器
     * @param dm 数据管理器
     * @return 版本管理器实例
     */
    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
