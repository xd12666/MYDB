package top.dhc.mydb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.dhc.mydb.backend.common.AbstractCache;
import top.dhc.mydb.backend.dm.page.Page;
import top.dhc.mydb.backend.dm.page.PageImpl;
import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.common.Error;

/**
 * PageCacheImpl 页面缓存实现类
 *
 * 这是PageCache接口的具体实现，继承自AbstractCache框架类
 *
 * 核心功能：
 * 1. 管理页面在内存和磁盘之间的交换
 * 2. 维护页面的缓存池，使用LRU策略淘汰页面
 * 3. 处理页面的读写操作，保证线程安全
 * 4. 跟踪数据库文件中的页面总数
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    // 最小缓存页面数：10个页面(80KB)，低于此值系统无法正常运行
    private static final int MEM_MIN_LIM = 10;

    // 数据库文件后缀名
    public static final String DB_SUFFIX = ".db";

    // 随机访问文件对象，用于读写数据库文件
    private RandomAccessFile file;

    // 文件通道，提供高效的文件I/O操作
    private FileChannel fc;

    // 文件锁，保证多线程环境下文件读写的线程安全
    private Lock fileLock;

    // 原子整数，记录当前数据库文件中的页面总数
    // 使用AtomicInteger保证多线程环境下的原子性操作
    private AtomicInteger pageNumbers;

    /**
     * 构造函数
     *
     * @param file 数据库文件的RandomAccessFile对象
     * @param fileChannel 文件通道
     * @param maxResource 最大缓存页面数(由分配的内存大小决定)
     */
    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);  // 调用父类AbstractCache的构造函数，初始化缓存框架

        // 检查缓存页面数是否低于最小限制
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);  // 内存太小，无法运行
        }

        // 获取数据库文件的长度，用于计算现有页面数
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();  // 创建可重入锁

        // 计算文件中已有的页面数 = 文件大小 / 页面大小
        // 例如：文件大小16KB，页面大小8KB，则有2个页面
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    /**
     * 创建新页面
     *
     * 流程：
     * 1. 页面计数器加1，获得新页号
     * 2. 创建Page对象并填充初始数据
     * 3. 立即刷新到磁盘文件
     * 4. 返回新页号
     *
     * 注意：新页面不会被加入缓存，直接写入磁盘
     *
     * @param initData 页面初始化数据(通常是8KB的字节数组)
     * @return 新页面的页号
     */
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();  // 原子操作：页号+1
        Page pg = new PageImpl(pgno, initData, null);  // 创建页面对象，cache参数为null表示不关联缓存
        flush(pg);  // 立即将新页面写入磁盘
        return pgno;  // 返回新页号
    }

    /**
     * 根据页号获取页面
     *
     * 通过父类AbstractCache的get方法实现：
     * - 如果页面在缓存中，直接返回
     * - 如果不在缓存中，调用getForCache从磁盘加载
     *
     * @param pgno 页号
     * @return Page对象
     * @throws Exception 页面不存在或读取失败
     */
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);  // 将int页号转为long，调用父类的get方法
    }

    /**
     * 从磁盘加载页面到缓存 (AbstractCache框架的回调方法)
     *
     * 当缓存未命中时，AbstractCache会调用此方法从数据源加载数据
     *
     * 加载流程：
     * 1. 根据页号计算文件偏移量
     * 2. 创建8KB的ByteBuffer
     * 3. 加锁，移动文件指针到对应位置
     * 4. 读取8KB数据到Buffer
     * 5. 解锁
     * 6. 将数据包装成Page对象返回
     *
     * @param key 页号(long类型)
     * @return 从磁盘读取的Page对象
     * @throws Exception 读取失败
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;  // 将long转回int页号
        long offset = PageCacheImpl.pageOffset(pgno);  // 计算页面在文件中的字节偏移

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);  // 分配8KB的ByteBuffer
        fileLock.lock();  // 加锁，保证文件读取的线程安全
        try {
            fc.position(offset);  // 移动文件指针到页面起始位置
            fc.read(buf);  // 从文件读取8KB数据到buffer
        } catch(IOException e) {
            Panic.panic(e);  // 读取失败，触发panic
        }
        fileLock.unlock();  // 解锁

        // 创建Page对象：页号、数据字节数组、关联的缓存对象(this)
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 释放页面从缓存 (AbstractCache框架的回调方法)
     *
     * 当页面被驱逐出缓存时，AbstractCache会调用此方法
     * 如果页面是脏页(被修改过)，需要先写回磁盘
     *
     * @param pg 要释放的页面
     */
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {  // 检查是否为脏页
            flush(pg);  // 脏页写回磁盘
            pg.setDirty(false);  // 清除脏标记
        }
    }

    /**
     * 释放页面引用
     *
     * 将页面的引用计数减1，当引用计数为0且缓存满时，页面可被驱逐
     *
     * @param page 要释放的页面
     */
    public void release(Page page) {
        release((long)page.getPageNumber());  // 调用父类的release方法
    }

    /**
     * 强制刷新页面到磁盘
     *
     * @param pg 要刷新的页面
     */
    public void flushPage(Page pg) {
        flush(pg);
    }

    /**
     * 将页面数据写入磁盘文件
     *
     * 写入流程：
     * 1. 计算页面在文件中的偏移位置
     * 2. 加锁
     * 3. 将页面数据包装到ByteBuffer
     * 4. 移动文件指针到对应位置
     * 5. 写入数据
     * 6. 强制刷新到磁盘(force)
     * 7. 解锁
     *
     * @param pg 要写入的页面
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();  // 获取页号
        long offset = pageOffset(pgno);  // 计算文件偏移

        fileLock.lock();  // 加锁，保证写入的线程安全
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());  // 将页面数据包装到ByteBuffer
            fc.position(offset);  // 移动文件指针
            fc.write(buf);  // 写入数据
            fc.force(false);  // 强制刷新到磁盘，false表示不刷新元数据(文件大小等)
        } catch(IOException e) {
            Panic.panic(e);  // 写入失败，触发panic
        } finally {
            fileLock.unlock();  // 确保解锁，即使发生异常
        }
    }

    /**
     * 根据页号截断数据库文件
     *
     * 删除页号大于maxPgno的所有页面，用于回滚操作
     *
     * 例如：maxPgno=5，则保留页面1-5，删除页面6及以后的所有页面
     *
     * @param maxPgno 保留的最大页号
     */
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);  // 计算截断后的文件大小
        try {
            file.setLength(size);  // 截断文件到指定大小
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);  // 更新页面计数器
    }

    /**
     * 关闭页面缓存
     *
     * 关闭流程：
     * 1. 调用父类close()：刷新所有脏页，清空缓存
     * 2. 关闭文件通道
     * 3. 关闭文件
     */
    @Override
    public void close() {
        super.close();  // 调用AbstractCache的close，处理缓存中的所有页面
        try {
            fc.close();  // 关闭文件通道
            file.close();  // 关闭文件
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 获取数据库文件中的页面总数
     *
     * @return 页面总数
     */
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 计算页号对应的文件偏移量
     *
     * 页号从1开始，文件偏移从0开始
     * 计算公式：offset = (pgno - 1) * PAGE_SIZE
     *
     * 例如：
     * - 页号1：offset = 0 (文件开头)
     * - 页号2：offset = 8192 (第2个8KB块)
     * - 页号3：offset = 16384 (第3个8KB块)
     *
     * @param pgno 页号
     * @return 文件偏移量(字节)
     */
    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;
    }

}