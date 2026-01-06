package top.dhc.mydb.backend.dm.dataItem;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import top.dhc.mydb.backend.common.SubArray;
import top.dhc.mydb.backend.dm.DataManagerImpl;
import top.dhc.mydb.backend.dm.page.Page;

/**
 * DataItemImpl 数据项的具体实现类
 *
 * dataItem 在页面中的存储结构：
 * [ValidFlag](1字节) [DataSize](2字节) [Data](DataSize字节)
 * - ValidFlag: 0为合法（有效），1为非法（已删除）
 * - DataSize: 标识 Data 部分的长度
 * - Data: 实际存储的用户数据
 *
 * 设计要点：
 * 1. 使用读写锁实现并发控制，支持多读单写
 * 2. 通过 oldRaw 保存修改前的数据，支持事务回滚
 * 3. 与 Page 和 DataManager 协同工作，实现数据持久化
 * 4. 采用 SubArray 避免不必要的数组复制
 */
public class DataItemImpl implements DataItem {

    // ValidFlag 字段在数据项中的偏移位置（第0字节）
    static final int OF_VALID = 0;

    // DataSize 字段在数据项中的偏移位置（第1字节开始）
    static final int OF_SIZE = 1;

    // Data 字段在数据项中的偏移位置（第3字节开始）
    // 因为前面有 ValidFlag(1字节) + DataSize(2字节) = 3字节
    static final int OF_DATA = 3;

    // 指向数据项在页面中的完整字节数组区间
    // 包含 [ValidFlag] + [DataSize] + [Data] 三部分
    private SubArray raw;

    // 保存数据项修改前的原始数据
    // 用于事务回滚（UNDO 操作）
    // 长度与 raw 的长度相同
    private byte[] oldRaw;

    // 读锁：多个事务可以同时持有读锁来读取数据
    private Lock rLock;

    // 写锁：只有一个事务可以持有写锁来修改数据
    // 持有写锁时，其他事务无法获取读锁或写锁
    private Lock wLock;

    // 数据管理器的引用
    // 用于：
    // 1. 记录修改日志（logDataItem）
    // 2. 释放数据项缓存（releaseDataItem）
    private DataManagerImpl dm;

    // 数据项的唯一标识符
    // 由页号和页内偏移组成，可以唯一定位数据库中的任意数据项
    private long uid;

    // 数据项所在的页面对象
    // 用于标记页面脏位、获取页面数据等
    private Page pg;

    /**
     * 构造一个 DataItem 对象
     *
     * @param raw 指向数据项在页面中的字节数组区间
     * @param oldRaw 用于保存修改前数据的缓冲区
     * @param pg 数据项所在的页面
     * @param uid 数据项的唯一标识符
     * @param dm 数据管理器
     */
    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;

        // 创建读写锁
        // ReentrantReadWriteLock 支持：
        // - 多个线程同时获取读锁
        // - 只有一个线程能获取写锁
        // - 持有写锁时，其他线程无法获取任何锁
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();

        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /**
     * 判断数据项是否有效（未被删除）
     *
     * @return true 表示有效（ValidFlag=0），false 表示已删除（ValidFlag=1）
     *
     * 实现细节：
     * - raw.start 是数据项在页面中的起始位置
     * - raw.start + OF_VALID 定位到 ValidFlag 字段
     * - 读取该字节，如果是0则有效，是1则无效
     */
    public boolean isValid() {
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    /**
     * 获取数据项的实际数据部分（不包含 ValidFlag 和 DataSize）
     *
     * @return SubArray 指向数据部分的子数组视图
     *
     * 实现细节：
     * - raw.start + OF_DATA 是数据部分的起始位置
     * - raw.end 是整个数据项的结束位置
     * - 返回的 SubArray 只包含用户数据，不包含元数据
     *
     * 使用场景：
     * 上层模块（如 VM）通过此方法获取实际数据进行读写
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    /**
     * 在修改数据之前调用，执行三个关键操作：
     *
     * 1. 获取写锁：确保修改过程中没有其他事务访问此数据项
     * 2. 标记页面为脏：告诉缓存管理器此页面已被修改，需要写回磁盘
     * 3. 保存旧数据：将当前数据复制到 oldRaw，用于事务回滚
     *
     * 调用时机：
     * 在任何修改数据项内容之前必须先调用此方法
     *
     * 典型使用流程：
     * dataItem.before();           // 1. 准备修改
     * // 修改 dataItem.data() 的内容  // 2. 执行修改
     * dataItem.after(xid);         // 3. 完成修改
     */
    @Override
    public void before() {
        wLock.lock();  // 获取写锁，阻塞其他所有访问
        pg.setDirty(true);  // 标记页面为脏页，确保会被写回磁盘
        // 将当前完整数据复制到 oldRaw
        // 参数：源数组, 源起始位置, 目标数组, 目标起始位置, 复制长度
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 撤销 before() 操作，用于事务回滚
     *
     * 执行两个操作：
     * 1. 从 oldRaw 恢复原始数据：撤销所有修改
     * 2. 释放写锁：允许其他事务访问
     *
     * 使用场景：
     * - 事务执行过程中发生错误，需要回滚
     * - 事务决定放弃修改
     *
     * 典型使用流程：
     * try {
     *     dataItem.before();
     *     // 修改数据...
     *     // 发生错误
     * } catch (Exception e) {
     *     dataItem.unBefore();  // 回滚修改
     * }
     */
    @Override
    public void unBefore() {
        // 将保存的旧数据恢复到原位置
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();  // 释放写锁
    }

    /**
     * 在修改数据之后调用，完成修改操作
     *
     * @param xid 事务ID，标识是哪个事务进行了此次修改
     *
     * 执行两个操作：
     * 1. 记录日志：将修改操作记录到日志中（可能是 REDO 日志）
     * 2. 释放写锁：允许其他事务访问
     *
     * 日志的作用：
     * - 用于崩溃恢复：系统崩溃后可以重放日志恢复数据
     * - 用于事务管理：记录事务的修改历史
     *
     * 注意：
     * after() 调用后，修改操作才算真正完成
     * 在 after() 之前，数据仍然可以通过 unBefore() 回滚
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);  // 记录修改日志
        wLock.unlock();  // 释放写锁
    }

    /**
     * 释放对数据项的引用
     *
     * 作用：
     * - 通知数据管理器不再使用此数据项
     * - 减少该数据项在缓存中的引用计数
     * - 当引用计数为0时，数据项可以从缓存中驱逐
     *
     * 调用时机：
     * 当上层模块（如 VM）使用完数据项后，必须调用此方法
     *
     * 与 AbstractCache 的关系：
     * DataManager 继承自 AbstractCache，release() 会调用
     * AbstractCache 的引用计数机制来管理缓存
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    /**
     * 获取写锁（排他锁）
     *
     * 使用场景：
     * 当需要修改数据项但不通过 before/after 流程时使用
     */
    @Override
    public void lock() {
        wLock.lock();
    }

    /**
     * 释放写锁
     */
    @Override
    public void unlock() {
        wLock.unlock();
    }

    /**
     * 获取读锁（共享锁）
     *
     * 使用场景：
     * 当只需要读取数据项内容，不进行修改时使用
     *
     * 优势：
     * 多个事务可以同时持有读锁，提高并发性能
     */
    @Override
    public void rLock() {
        rLock.lock();
    }

    /**
     * 释放读锁
     */
    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    /**
     * 获取数据项所在的页面对象
     *
     * @return Page 对象
     *
     * 使用场景：
     * - 需要访问页面级别的信息
     * - 需要对页面进行操作（如设置脏位）
     */
    @Override
    public Page page() {
        return pg;
    }

    /**
     * 获取数据项的唯一标识符
     *
     * @return UID，由页号和页内偏移组成
     *
     * 作用：
     * - 可以通过 UID 快速定位到数据库中的任意数据项
     * - 用作数据项在缓存中的 key
     */
    @Override
    public long getUid() {
        return uid;
    }

    /**
     * 获取修改前保存的原始数据
     *
     * @return oldRaw 数组
     *
     * 使用场景：
     * - 事务回滚时需要使用旧数据恢复
     * - 可能用于记录 UNDO 日志
     */
    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    /**
     * 获取数据项的完整原始字节数组视图
     *
     * @return SubArray，包含 ValidFlag + DataSize + Data
     *
     * 与 data() 的区别：
     * - getRaw() 返回完整的数据项（包含元数据）
     * - data() 只返回数据部分（不包含元数据）
     */
    @Override
    public SubArray getRaw() {
        return raw;
    }

}