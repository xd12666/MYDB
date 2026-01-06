package top.dhc.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.dhc.mydb.backend.dm.pageCache.PageCache;

/**
 * PageImpl 页面的具体实现类
 *
 * 设计要点：
 * 1. 封装页面的所有状态：页号、数据、脏标记
 * 2. 使用 ReentrantLock 实现线程安全的并发控制
 * 3. 持有 PageCache 引用，用于释放页面时通知缓存管理器
 * 4. 实现简单直接，主要作为数据载体和状态管理
 *
 * 与其他组件的关系：
 * PageImpl 是 Page 接口的实现，由 PageCache 创建和管理，被 DataItem 使用来存储实际数据
 */
public class PageImpl implements Page {

    // 页号：页面在数据库文件中的唯一标识
    // 页号从1开始，通过页号可以计算出页面在文件中的物理位置
    // 计算公式：文件偏移量 = pageNumber * PAGE_SIZE
    private int pageNumber;

    // 页面数据：存储页面的实际字节内容
    // 通常是固定大小，如8KB
    // 这个数组中包含了页面元数据和多个 DataItem 的数据
    // 结构示例：[页面元数据][DataItem1][DataItem2]...[空闲空间]
    private byte[] data;

    // 脏标记：标识页面是否被修改过
    // true 表示页面已被修改，内存中的数据与磁盘上的不一致，需要写回磁盘
    // false 表示页面未被修改，内存中的数据与磁盘上的一致，可以直接丢弃
    private boolean dirty;

    // 页面级锁：用于保护页面数据的并发访问
    // ReentrantLock 是可重入锁，支持同一线程多次获取锁
    // 读写页面数据时需要先获取此锁，保证线程安全
    private Lock lock;

    // PageCache 引用：指向管理此页面的缓存管理器
    // 用途：
    // 1. 释放页面时通知 PageCache 减少引用计数
    // 2. PageCache 负责页面的生命周期管理
    // 3. PageCache 决定何时将页面写回磁盘或从缓存中驱逐
    private PageCache pc;

    /**
     * 构造一个页面对象
     *
     * 参数说明：
     * pageNumber 页号，页面在文件中的唯一标识
     * data 页面的字节数据，通常是固定大小的数组
     * pc PageCache 对象，负责管理此页面的缓存
     *
     * 初始状态：
     * dirty 默认为 false，表示刚加载的页面是干净的
     * lock 初始化为新的 ReentrantLock，保证线程安全
     */
    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    /**
     * 获取页面锁
     *
     * 实现：调用 ReentrantLock 的 lock 方法
     *
     * 行为：
     * 如果锁可用则立即获取，如果锁被其他线程持有则当前线程阻塞等待直到获取到锁
     *
     * 使用场景：
     * 在读取或修改 data 数组之前必须先调用此方法获取锁
     */
    public void lock() {
        lock.lock();
    }

    /**
     * 释放页面锁
     *
     * 实现：调用 ReentrantLock 的 unlock 方法
     *
     * 注意事项：
     * 必须与 lock 成对使用，建议在 finally 块中调用确保锁一定会被释放
     * 只有持有锁的线程才能释放锁，否则会抛出异常
     */
    public void unlock() {
        lock.unlock();
    }

    /**
     * 释放对页面的引用
     *
     * 实现：将释放操作委托给 PageCache 处理
     *
     * 工作流程：
     * 1. 调用 pc.release(this) 通知 PageCache
     * 2. PageCache 会减少此页面的引用计数
     * 3. 如果引用计数降为0，PageCache 可以选择驱逐此页面
     * 4. 如果页面是脏页，驱逐前会先写回磁盘
     *
     * 为什么委托给 PageCache：
     * PageCache 统一管理所有页面的引用计数和缓存策略，Page 只是数据载体，不负责缓存逻辑
     */
    public void release() {
        pc.release(this);
    }

    /**
     * 设置页面的脏标记
     *
     * 参数 dirty：true 标记为脏页，false 标记为干净页
     *
     * 实现：直接设置 dirty 字段
     *
     * 调用时机：
     * 当修改了 data 数组的内容后，必须调用 setDirty(true) 通知系统此页面已被修改
     *
     * 重要性：
     * 如果修改后不设置脏标记，修改的数据可能不会被写回磁盘，导致数据丢失
     */
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    /**
     * 判断页面是否为脏页
     *
     * 返回值：true 表示脏页，false 表示干净页
     *
     * 实现：直接返回 dirty 字段的值
     *
     * 使用场景：
     * PageCache 在驱逐页面或执行检查点时，会检查页面是否为脏页，脏页需要先写回磁盘再驱逐
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * 获取页面的页号
     *
     * 返回值：页号，页面在文件中的唯一标识
     *
     * 实现：直接返回 pageNumber 字段
     *
     * 使用场景：
     * 1. 计算页面在文件中的物理位置
     * 2. 作为 PageCache 中的缓存键
     * 3. 组成 DataItem 的 UID 的高位部分
     * 4. 日志记录中标识是哪个页面被修改
     */
    public int getPageNumber() {
        return pageNumber;
    }

    /**
     * 获取页面的原始字节数据
     *
     * 返回值：页面数据的字节数组
     *
     * 实现：直接返回 data 数组的引用，不是副本
     *
     * 重要提示：
     * 1. 返回的是数组引用，不是副本，直接修改会影响页面内容
     * 2. 修改前必须先调用 lock 获取锁
     * 3. 修改后必须调用 setDirty(true) 标记为脏页
     * 4. 使用完必须调用 unlock 释放锁
     *
     * 典型使用模式：
     * page.lock();
     * try {
     *     byte[] data = page.getData();
     *     修改 data 的内容
     *     page.setDirty(true);
     * } finally {
     *     page.unlock();
     * }
     */
    public byte[] getData() {
        return data;
    }

}