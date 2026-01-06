package top.dhc.mydb.backend.dm.logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import top.dhc.mydb.backend.dm.logger.LoggerImpl;
import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.backend.utils.Parser;
import top.dhc.mydb.common.Error;

/**
 * Logger 日志管理器接口
 *
 * 设计目的：
 * Logger 是数据库系统中的日志子系统，用于实现 WAL (Write-Ahead Logging)
 *
 * 核心作用：
 * 1. 崩溃恢复：系统崩溃后，通过重放日志恢复数据到一致状态
 * 2. 事务持久化：保证已提交事务的修改不会丢失（REDO）
 * 3. 事务回滚：记录修改前的数据，支持事务回滚（UNDO）
 *
 * 日志文件结构：
 * [XChecksum](4字节) [Log1][Log2][Log3]...
 * - XChecksum: 用于校验日志文件的完整性，记录有效日志的边界
 * - Log: 单条日志记录
 *
 * 使用场景：
 * - 数据修改前先写日志，再修改数据（Write-Ahead Logging）
 * - 系统启动时读取日志进行恢复
 * - 事务提交时确保日志已持久化到磁盘
 */
public interface Logger {

    /**
     * 写入一条日志
     *
     * @param data 日志数据的字节数组
     *
     * 日志的典型内容：
     * - 事务ID
     * - 操作类型（INSERT/UPDATE/DELETE）
     * - 修改前的数据（用于UNDO）
     * - 修改后的数据（用于REDO）
     *
     * 特点：
     * - 日志顺序追加到文件末尾
     * - 写入后通常需要调用 force() 确保持久化
     */
    void log(byte[] data);

    /**
     * 截断日志文件
     *
     * @param x 截断位置，保留从文件开头到 x 位置的内容，删除 x 之后的内容
     * @throws Exception 截断失败时抛出异常
     *
     * 使用场景：
     * - 日志恢复完成后，清理无效的日志
     * - 检查点（Checkpoint）操作后，删除已经应用到数据文件的旧日志
     *
     * 作用：
     * 防止日志文件无限增长，节省磁盘空间
     */
    void truncate(long x) throws Exception;

    /**
     * 读取下一条日志
     *
     * @return 日志数据的字节数组，如果没有更多日志则返回 null
     *
     * 使用场景：
     * - 系统启动时，顺序读取所有日志进行恢复
     * - 遍历日志文件进行分析或审计
     *
     * 注意：
     * 需要配合 rewind() 使用，rewind() 将读取位置重置到开头
     */
    byte[] next();

    /**
     * 重置日志读取位置到文件开头
     *
     * 使用场景：
     * - 准备重新读取日志文件
     * - 多次遍历日志时，每次遍历前调用 rewind()
     *
     * 典型使用流程：
     * logger.rewind();           // 重置到开头
     * while(true) {
     *     byte[] log = logger.next();
     *     if(log == null) break;  // 读取完毕
     *     // 处理日志...
     * }
     */
    void rewind();

    /**
     * 关闭日志管理器
     *
     * 作用：
     * - 关闭文件句柄
     * - 释放系统资源
     * - 确保所有缓冲的日志都已写入磁盘
     *
     * 调用时机：
     * 数据库系统正常关闭时调用
     */
    void close();

    /**
     * 创建一个新的日志文件
     *
     * @param path 日志文件的路径（不包含后缀）
     * @return Logger 对象
     *
     * 创建流程：
     * 1. 创建日志文件（文件名 = path + LOG_SUFFIX）
     * 2. 检查文件是否可读写
     * 3. 打开文件的 FileChannel
     * 4. 在文件开头写入 XChecksum 初始值（4字节的0）
     * 5. 强制刷新到磁盘
     * 6. 创建 LoggerImpl 对象
     *
     * XChecksum 的作用：
     * - 记录有效日志的边界位置
     * - 初始值为0，表示没有任何有效日志
     * - 每次写入日志后更新 XChecksum
     * - 用于崩溃恢复时确定哪些日志是有效的
     *
     * 使用场景：
     * 初次创建数据库时调用，建立日志文件
     */
    public static Logger create(String path) {
        // 拼接日志文件的完整路径
        File f = new File(path+ LoggerImpl.LOG_SUFFIX);
        try {
            // 创建新文件，如果文件已存在则抛出异常
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        // 检查文件权限，必须可读可写
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            // 以读写模式打开文件
            // RandomAccessFile 支持随机访问，可以在文件任意位置读写
            raf = new RandomAccessFile(f, "rw");
            // 获取 FileChannel，用于高效的文件 I/O 操作
            // FileChannel 比传统的流式 I/O 性能更好，支持内存映射等高级特性
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 在文件开头写入 XChecksum 初始值
        // XChecksum 占4字节，初始值为0
        // Parser.int2Byte(0) 将整数0转换为4字节数组
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);  // 定位到文件开头
            fc.write(buf);   // 写入4字节的0
            // 强制刷新到磁盘
            // force(false) 表示只刷新文件内容，不刷新元数据（如文件修改时间）
            // 这对于日志系统很重要，确保日志真正持久化，防止崩溃时丢失
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 创建 LoggerImpl 对象
        // 参数：RandomAccessFile, FileChannel, XChecksum初始值(0)
        return new LoggerImpl(raf, fc, 0);
    }

    /**
     * 打开一个已存在的日志文件
     *
     * @param path 日志文件的路径（不包含后缀）
     * @return Logger 对象
     *
     * 打开流程：
     * 1. 检查文件是否存在
     * 2. 检查文件是否可读写
     * 3. 打开文件的 FileChannel
     * 4. 创建 LoggerImpl 对象
     * 5. 调用 init() 方法初始化（校验日志文件，读取 XChecksum）
     *
     * 与 create() 的区别：
     * - create() 用于创建新文件，会初始化 XChecksum 为0
     * - open() 用于打开已有文件，会从文件中读取 XChecksum
     *
     * 使用场景：
     * 数据库启动时调用，打开已有的日志文件进行恢复或追加日志
     */
    public static Logger open(String path) {
        // 拼接日志文件的完整路径
        File f = new File(path+LoggerImpl.LOG_SUFFIX);

        // 检查文件是否存在
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }

        // 检查文件权限
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            // 以读写模式打开文件
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 创建 LoggerImpl 对象（不传入 XChecksum，需要从文件中读取）
        LoggerImpl lg = new LoggerImpl(raf, fc);

        // 初始化日志管理器
        // init() 方法会：
        // 1. 检查日志文件的完整性
        // 2. 从文件开头读取 XChecksum
        // 3. 校验日志的有效性
        // 4. 可能会截断损坏的日志
        lg.init();

        return lg;
    }
}