package top.dhc.mydb.backend.dm;

import top.dhc.mydb.backend.dm.dataItem.DataItem;
import top.dhc.mydb.backend.dm.logger.Logger;
import top.dhc.mydb.backend.dm.page.PageOne;
import top.dhc.mydb.backend.dm.pageCache.PageCache;
import top.dhc.mydb.backend.tm.TransactionManager;
import top.dhc.mydb.backend.dm.DataManagerImpl;
import top.dhc.mydb.backend.dm.Recover;



// 数据管理器接口，是数据管理层DM的核心接口
// 负责管理数据的读写、事务日志、页面缓存等核心功能
// DM层位于事务管理器TM之上，为上层提供数据的读写接口
public interface DataManager {

    // 根据UID读取数据项
    // uid是数据项的唯一标识符，由页号和页内偏移组成
    // 返回DataItem对象，包含数据内容和相关元信息
    DataItem read(long uid) throws Exception;

    // 插入数据，返回数据项的UID
    // xid是事务ID，标识是哪个事务在插入数据
    // data是要插入的字节数组数据
    // 返回新插入数据的UID，用于后续读取和引用
    long insert(long xid, byte[] data) throws Exception;

    // 关闭数据管理器
    // 将所有脏页写回磁盘，关闭日志和页面缓存，释放资源
    void close();

    // 创建一个新的数据管理器
    // 用于初始化一个全新的数据库文件
    // 创建流程：
    // 1. 创建页面缓存PageCache，用于管理页面的读写
    // 2. 创建日志Logger，用于记录事务操作日志
    // 3. 创建DataManagerImpl实例，传入缓存、日志和事务管理器
    // 4. 初始化第一页PageOne，这是数据库的元数据页
    // path: 数据库文件路径
    // mem: 分配给页面缓存的内存大小
    // tm: 事务管理器，用于管理事务的生命周期
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);  // 创建页面缓存
        Logger lg = Logger.create(path);  // 创建日志文件

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();  // 初始化第一页，存储数据库启动信息
        return dm;
    }

    // 打开一个已存在的数据管理器
    // 用于打开已有的数据库文件
    // 打开流程：
    // 1. 打开页面缓存PageCache
    // 2. 打开日志Logger
    // 3. 创建DataManagerImpl实例
    // 4. 检查第一页PageOne的校验，判断上次是否正常关闭
    // 5. 如果校验失败，说明数据库异常关闭，需要执行崩溃恢复
    // 6. 填充页面索引，重建空闲页面的索引结构
    // 7. 设置第一页的vcOpen标志，表示数据库已打开
    // 8. 刷新第一页到磁盘
    // path: 数据库文件路径
    // mem: 分配给页面缓存的内存大小
    // tm: 事务管理器
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);  // 打开页面缓存
        Logger lg = Logger.open(path);  // 打开日志文件
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);

        // 加载并校验第一页，检查数据库是否正常关闭
        if(!dm.loadCheckPageOne()) {
            // 校验失败，执行崩溃恢复
            // 通过重放日志来恢复数据库到一致状态
            Recover.recover(tm, lg, pc);
        }

        // 填充页面索引，扫描所有页面构建空闲空间索引
        dm.fillPageIndex();

        // 设置第一页的vcOpen字段，标记数据库已打开
        PageOne.setVcOpen(dm.pageOne);

        // 将第一页刷新到磁盘，确保打开标志被持久化
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}