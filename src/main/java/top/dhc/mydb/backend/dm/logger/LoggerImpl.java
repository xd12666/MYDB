package top.dhc.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.backend.utils.Parser;
import top.dhc.mydb.common.Error;

/**
 * LoggerImpl 日志管理器的具体实现
 *
 * 日志文件的标准格式：
 * [XChecksum](4字节) [Log1] [Log2] ... [LogN] [BadTail]
 *
 * - XChecksum: 4字节整数，是后续所有有效日志计算的累积校验和
 *   作用：快速定位有效日志的边界，用于崩溃恢复时判断哪些日志是完整的
 *
 * - Log1, Log2, ..., LogN: 完整的日志记录
 *
 * - BadTail: 可能存在的损坏日志（系统崩溃时正在写入的日志）
 *   checkAndRemoveTail() 会在初始化时检测并移除这些损坏的日志
 *
 * 每条正确日志的格式：
 * [Size](4字节) [Checksum](4字节) [Data](Size字节)
 *
 * - Size: 4字节整数，标识 Data 部分的长度
 * - Checksum: 4字节整数，Data 部分的校验和，用于检测日志是否损坏
 * - Data: 实际的日志数据（事务操作信息）
 *
 * 设计要点：
 * 1. 双重校验机制：单条日志的 Checksum + 全局的 XChecksum
 * 2. 顺序写入：日志总是追加到文件末尾
 * 3. 崩溃恢复：通过 XChecksum 验证日志完整性，移除损坏的 BadTail
 * 4. 线程安全：使用 ReentrantLock 保护并发访问
 */
public class LoggerImpl implements Logger {

    // 校验和计算的种子值，用于计算 Checksum 和 XChecksum
    // 使用质数可以减少哈希冲突，提高校验的准确性
    private static final int SEED = 13331;

    // 日志格式中各字段的偏移位置
    private static final int OF_SIZE = 0;           // Size 字段的偏移：0
    private static final int OF_CHECKSUM = OF_SIZE + 4;  // Checksum 字段的偏移：4
    private static final int OF_DATA = OF_CHECKSUM + 4;  // Data 字段的偏移：8

    // 日志文件的后缀名
    public static final String LOG_SUFFIX = ".log";

    // 日志文件的 RandomAccessFile 对象，支持随机访问
    private RandomAccessFile file;

    // 日志文件的 FileChannel，用于高效的文件 I/O 操作
    private FileChannel fc;

    // 锁，用于保护并发访问
    private Lock lock;

    // 当前日志读取指针的位置
    // 用于 next() 方法顺序读取日志
    // rewind() 会将其重置到文件开头（跳过 XChecksum）
    private long position;

    // 日志文件的大小，在初始化时记录
    // 注意：log() 操作不会更新这个值，它主要用于读取时的边界检查
    private long fileSize;

    // XChecksum：所有有效日志的累积校验和
    // 每写入一条日志后会更新 XChecksum
    // 用于崩溃恢复时验证日志的完整性
    private int xChecksum;

    /**
     * 构造函数：用于打开已存在的日志文件
     *
     * @param raf RandomAccessFile 对象
     * @param fc FileChannel 对象
     *
     * 注意：这个构造函数不传入 xChecksum，需要调用 init() 从文件中读取
     */
    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    /**
     * 构造函数：用于创建新的日志文件
     *
     * @param raf RandomAccessFile 对象
     * @param fc FileChannel 对象
     * @param xChecksum 初始的 XChecksum 值（通常为0）
     */
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /**
     * 初始化日志管理器
     *
     * 执行步骤：
     * 1. 获取日志文件的大小
     * 2. 从文件开头读取 XChecksum
     * 3. 调用 checkAndRemoveTail() 检查并移除损坏的日志
     *
     * 这个方法在打开已存在的日志文件时调用
     */
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 日志文件至少要有4字节（XChecksum）
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        // 从文件开头读取 XChecksum（4字节）
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);  // 定位到文件开头
            fc.read(raw);    // 读取4字节
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 解析 XChecksum
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        // 检查并移除损坏的日志尾部
        checkAndRemoveTail();
    }

    /**
     * 检查并移除损坏的日志尾部（BadTail）
     *
     * 工作原理：
     * 1. 从头到尾读取所有日志，重新计算 XChecksum
     * 2. 将重新计算的 XChecksum 与文件中保存的 XChecksum 对比
     * 3. 如果不一致，说明有损坏的日志（BadTail），截断文件
     *
     * BadTail 产生的原因：
     * - 系统崩溃时正在写入日志，导致日志不完整
     * - 写入过程中断电，日志只写了一部分
     *
     * 这个方法确保了日志文件的一致性，是崩溃恢复的关键
     */
    private void checkAndRemoveTail() {
        rewind();  // 重置读取位置到文件开头（跳过 XChecksum）

        int xCheck = 0;
        // 重新读取所有日志，计算 XChecksum
        while(true) {
            byte[] log = internNext();  // 读取下一条日志（包含 Size+Checksum+Data）
            if(log == null) break;      // 没有更多日志了
            xCheck = calChecksum(xCheck, log);  // 累积计算 XChecksum
        }

        // 对比重新计算的 XChecksum 和文件中保存的 XChecksum
        if(xCheck != xChecksum) {
            // 不一致说明日志文件损坏，这是致命错误
            Panic.panic(Error.BadLogFileException);
        }

        // XChecksum 校验通过，截断文件到当前位置
        // 这会移除可能存在的不完整日志（BadTail）
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        // 将文件指针移动到截断后的位置，准备追加新日志
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 重置读取位置
        rewind();
    }

    /**
     * 计算校验和
     *
     * @param xCheck 当前的校验和值
     * @param log 日志数据
     * @return 新的校验和值
     *
     * 算法：xCheck = xCheck * SEED + byte
     * 这是一个简单但有效的滚动哈希算法
     *
     * 特点：
     * - 顺序敏感：字节顺序不同，结果不同
     * - 累积性：可以逐步累加，适合计算多条日志的 XChecksum
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 写入一条日志
     *
     * @param data 要写入的日志数据
     *
     * 执行步骤：
     * 1. 将 data 包装成完整的日志格式：[Size][Checksum][Data]
     * 2. 追加到文件末尾
     * 3. 更新 XChecksum 并写回文件开头
     *
     * 注意：
     * - 使用锁保证线程安全
     * - 总是追加到文件末尾（顺序写入）
     * - 每次写入后都会更新 XChecksum（包括强制刷盘）
     */
    @Override
    public void log(byte[] data) {
        // 包装成完整的日志格式
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);

        lock.lock();
        try {
            // 定位到文件末尾
            fc.position(fc.size());
            // 写入日志
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }

        // 更新 XChecksum
        updateXChecksum(log);
    }

    /**
     * 更新 XChecksum 并写回文件
     *
     * @param log 新写入的日志（完整格式，包含 Size+Checksum+Data）
     *
     * 执行步骤：
     * 1. 用新日志更新 XChecksum
     * 2. 将新的 XChecksum 写回文件开头
     * 3. 强制刷新到磁盘
     *
     * 为什么要立即刷盘（force）：
     * - 确保 XChecksum 持久化，防止崩溃时丢失
     * - XChecksum 是恢复的关键，必须保证其准确性
     * - 即使日志数据还在缓冲区，XChecksum 也要先写入磁盘
     */
    private void updateXChecksum(byte[] log) {
        // 累积计算新的 XChecksum
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            // 定位到文件开头
            fc.position(0);
            // 写入新的 XChecksum
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            // 强制刷新到磁盘
            // force(false) 表示只刷新文件内容，不刷新元数据
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将原始数据包装成完整的日志格式
     *
     * @param data 原始日志数据
     * @return 完整的日志：[Size][Checksum][Data]
     *
     * 包装过程：
     * 1. 计算 data 的 Checksum
     * 2. 计算 data 的长度（Size）
     * 3. 按顺序拼接：Size + Checksum + Data
     */
    private byte[] wrapLog(byte[] data) {
        // 计算 data 的校验和
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        // 计算 data 的长度
        byte[] size = Parser.int2Byte(data.length);
        // 拼接三部分：size(4) + checksum(4) + data(n)
        return Bytes.concat(size, checksum, data);
    }

    /**
     * 截断日志文件
     *
     * @param x 截断位置，保留 [0, x) 的内容，删除 x 之后的内容
     * @throws Exception 截断失败时抛出异常
     *
     * 使用场景：
     * - checkAndRemoveTail() 中移除损坏的日志
     * - 日志恢复完成后，清理无效的旧日志
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 内部方法：读取下一条日志（不加锁）
     *
     * @return 完整的日志字节数组（包含 Size+Checksum+Data），如果没有更多日志则返回 null
     *
     * 读取流程：
     * 1. 检查是否还有足够的字节读取日志头（Size+Checksum）
     * 2. 读取 Size 字段，确定日志的总长度
     * 3. 检查是否有足够的字节读取完整日志
     * 4. 读取完整的日志（Size+Checksum+Data）
     * 5. 验证 Checksum，如果不匹配则返回 null（说明日志损坏）
     * 6. 更新 position 指针
     * 7. 返回完整的日志数据
     *
     * 注意：
     * - 返回的是完整日志，包含 Size 和 Checksum
     * - 调用者需要自己提取 Data 部分
     */
    private byte[] internNext() {
        // 检查是否还有足够的字节读取日志头
        // OF_DATA = 8，即 Size(4) + Checksum(4)
        if(position + OF_DATA >= fileSize) {
            return null;  // 文件已读完
        }

        // 读取 Size 字段（4字节）
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());

        // 检查是否有足够的字节读取完整日志
        if(position + size + OF_DATA > fileSize) {
            return null;  // 日志不完整，可能是 BadTail
        }

        // 读取完整的日志：Size(4) + Checksum(4) + Data(size)
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();

        // 验证 Checksum
        // checkSum1: 根据 Data 重新计算的校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // checkSum2: 日志中保存的校验和
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));

        if(checkSum1 != checkSum2) {
            return null;  // 校验和不匹配，日志损坏
        }

        // 移动指针到下一条日志
        position += log.length;
        return log;
    }

    /**
     * 读取下一条日志（公开方法）
     *
     * @return 日志的 Data 部分（不包含 Size 和 Checksum），如果没有更多日志则返回 null
     *
     * 与 internNext() 的区别：
     * - internNext() 返回完整日志（Size+Checksum+Data）
     * - next() 只返回 Data 部分
     * - next() 加锁，线程安全
     *
     * 使用场景：
     * 系统启动时，顺序读取所有日志进行恢复
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            // 只返回 Data 部分：从 OF_DATA(8) 到末尾
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重置日志读取位置到文件开头
     *
     * position 设置为4，跳过文件开头的 XChecksum（4字节）
     *
     * 使用场景：
     * - 准备从头读取所有日志
     * - checkAndRemoveTail() 中重新验证日志
     */
    @Override
    public void rewind() {
        position = 4;  // 跳过 XChecksum，从第一条日志开始
    }

    /**
     * 关闭日志管理器
     *
     * 关闭 FileChannel 和 RandomAccessFile，释放系统资源
     */
    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

}