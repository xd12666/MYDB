package top.dhc.mydb.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.common.Error;

/**
 * 事务管理器接口（Transaction Manager）
 * 
 * TM层是数据库系统的最底层，负责：
 * 1. 管理事务的生命周期（开始、提交、回滚）
 * 2. 维护事务的状态（活跃、已提交、已回滚）
 * 3. 持久化事务状态到XID文件
 * 
 * 在数据库系统架构中的位置：
 * - 位于最底层，为上层（DM、VM、TBM）提供事务管理服务
 * - 所有其他组件都依赖TM来管理事务状态
 * 
 * 事务状态管理：
 * - 每个事务有一个唯一的XID（事务ID）
 * - 事务状态存储在XID文件中：
 *   - ACTIVE: 事务正在进行中
 *   - COMMITTED: 事务已提交
 *   - ABORTED: 事务已回滚
 * 
 * XID文件格式：
 * - 文件头：8字节，存储事务数量
 * - 事务状态：每个事务1字节
 *   - 0: ACTIVE
 *   - 1: COMMITTED
 *   - 2: ABORTED
 */
public interface TransactionManager {
    /**
     * 开始一个新事务
     * 
     * @return 新事务的XID（事务ID）
     */
    long begin();
    
    /**
     * 提交事务
     * 
     * @param xid 要提交的事务ID
     */
    void commit(long xid);
    
    /**
     * 回滚事务
     * 
     * @param xid 要回滚的事务ID
     */
    void abort(long xid);
    
    /**
     * 检查事务是否处于活跃状态
     * 
     * @param xid 事务ID
     * @return true表示事务正在执行中
     */
    boolean isActive(long xid);
    
    /**
     * 检查事务是否已提交
     * 
     * @param xid 事务ID
     * @return true表示事务已成功提交
     */
    boolean isCommitted(long xid);
    
    /**
     * 检查事务是否已回滚
     * 
     * @param xid 事务ID
     * @return true表示事务已回滚
     */
    boolean isAborted(long xid);
    
    /**
     * 关闭事务管理器
     * 关闭文件通道，释放资源
     */
    void close();

    public static TransactionManagerImpl create(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
