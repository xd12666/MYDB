package top.dhc.mydb.backend.dm.pageCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import top.dhc.mydb.backend.dm.page.Page;
import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.common.Error;
import top.dhc.mydb.backend.dm.pageCache.PageCacheImpl;

/**
 * PageCache 页面缓存接口
 *
 * 这是数据库存储引擎的核心组件之一，负责管理页面的缓存
 * 主要功能：
 * 1. 页面的创建、读取、释放
 * 2. 页面在内存和磁盘之间的交换
 * 3. 控制内存中缓存的页面数量
 * 4. 提供数据库文件的创建和打开接口
 */
public interface PageCache {

    // 页面大小常量：1 << 13 = 8192字节 = 8KB
    // 数据库中所有页面统一使用8KB大小，这是数据库系统的基本存储单位
    public static final int PAGE_SIZE = 1 << 13;

    /**
     * 创建一个新页面
     * @param initData 初始化数据，用于填充新页面的内容
     * @return 新页面的页号(page number)，从1开始编号
     */
    int newPage(byte[] initData);

    /**
     * 根据页号获取页面
     * 如果页面在缓存中则直接返回，否则从磁盘读取并加载到缓存
     * @param pgno 页号，从1开始
     * @return Page对象
     * @throws Exception 页面不存在或读取失败时抛出异常
     */
    Page getPage(int pgno) throws Exception;

    /**
     * 关闭页面缓存
     * 将所有脏页(被修改过的页面)写回磁盘，释放资源，关闭文件
     */
    void close();

    /**
     * 释放页面
     * 将页面的引用计数减1，当引用计数为0时，页面可以被驱逐出缓存
     * @param page 要释放的页面对象
     */
    void release(Page page);

    /**
     * 根据页号截断文件
     * 删除页号大于maxPgno的所有页面，用于回滚操作
     * @param maxPgno 保留的最大页号
     */
    void truncateByBgno(int maxPgno);

    /**
     * 获取当前数据库文件的页面数量
     * @return 页面总数
     */
    int getPageNumber();

    /**
     * 强制刷新页面到磁盘
     * 将指定页面立即写回磁盘文件，无论是否为脏页
     * @param pg 要刷新的页面
     */
    void flushPage(Page pg);

    /**
     * 创建一个新的数据库文件并返回其页面缓存
     *
     * 创建流程：
     * 1. 创建.db后缀的数据库文件
     * 2. 检查文件是否可读写
     * 3. 打开文件通道用于I/O操作
     * 4. 创建PageCacheImpl实例，根据内存大小计算缓存页数
     *
     * @param path 数据库文件路径(不含后缀)
     * @param memory 分配给页面缓存的内存大小(字节)
     * @return PageCacheImpl实例
     */
    public static PageCacheImpl create(String path, long memory) {
        // 拼接.db后缀创建完整文件路径
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            // 创建新文件，如果文件已存在则createNewFile返回false
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);  // 文件已存在，抛出异常
            }
        } catch (Exception e) {
            Panic.panic(e);  // 创建文件失败，抛出异常
        }

        // 检查文件权限：必须同时具有读和写权限
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);  // 文件无读写权限，抛出异常
        }

        FileChannel fc = null;  // 文件通道，用于高效的文件I/O操作
        RandomAccessFile raf = null;  // 随机访问文件，支持在文件任意位置读写
        try {
            // 以读写模式打开文件
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();  // 获取文件通道
        } catch (FileNotFoundException e) {
            Panic.panic(e);  // 文件未找到，抛出异常
        }

        // 创建PageCacheImpl实例
        // memory/PAGE_SIZE 计算可以缓存的页面数量
        // 例如：memory=64MB，PAGE_SIZE=8KB，则可缓存 64*1024*1024/8192 = 8192个页面
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    /**
     * 打开一个已存在的数据库文件并返回其页面缓存
     *
     * 打开流程：
     * 1. 检查数据库文件是否存在
     * 2. 检查文件是否可读写
     * 3. 打开文件通道用于I/O操作
     * 4. 创建PageCacheImpl实例
     *
     * @param path 数据库文件路径(不含后缀)
     * @param memory 分配给页面缓存的内存大小(字节)
     * @return PageCacheImpl实例
     */
    public static PageCacheImpl open(String path, long memory) {
        // 拼接.db后缀创建完整文件路径
        File f = new File(path+ PageCacheImpl.DB_SUFFIX);

        // 检查文件是否存在
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);  // 文件不存在，抛出异常
        }

        // 检查文件权限：必须同时具有读和写权限
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);  // 文件无读写权限，抛出异常
        }

        FileChannel fc = null;  // 文件通道
        RandomAccessFile raf = null;  // 随机访问文件
        try {
            // 以读写模式打开文件
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();  // 获取文件通道
        } catch (FileNotFoundException e) {
            Panic.panic(e);  // 文件未找到，抛出异常
        }

        // 创建PageCacheImpl实例，参数与create方法相同
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}