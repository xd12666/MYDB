package top.dhc.mydb.backend.dm.dataItem;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import top.dhc.mydb.backend.common.SubArray;
import top.dhc.mydb.backend.dm.DataManagerImpl;
import top.dhc.mydb.backend.dm.DataManagerImpl;
import top.dhc.mydb.backend.dm.dataItem.DataItemImpl;
import top.dhc.mydb.backend.dm.page.Page;
import top.dhc.mydb.backend.utils.Parser;
import top.dhc.mydb.backend.utils.Types;

/**
 * DataItem 数据项接口
 *
 * 设计核心：
 * DataItem 是 DM（Data Manager）层向上层提供的数据抽象
 * 上层模块（如 VM - Version Manager）通过 DataItem 来读写底层数据
 *
 * DataItem 的物理结构（在页面中的存储格式）：
 * [ValidFlag](1字节) + [DataSize](2字节) + [Data](DataSize字节)
 * - ValidFlag: 0表示有效，1表示无效（已删除）
 * - DataSize: 数据部分的长度
 * - Data: 实际存储的数据内容
 *
 * 设计职责：
 * 1. 数据的读写访问
 * 2. 事务相关的日志记录（before/after）
 * 3. 并发控制的锁管理
 * 4. 缓存管理（通过 release）
 */
public interface DataItem {

    /**
     * 获取数据项的实际数据部分（不包含 ValidFlag 和 DataSize）
     *
     * @return SubArray 指向实际数据的子数组视图
     */
    SubArray data();

    /**
     * 在修改数据项之前调用，用于记录修改前的数据
     *
     * 用途：为事务回滚做准备
     * - 在修改数据之前，先调用 before() 保存旧值
     * - 如果事务需要回滚，可以使用保存的旧值恢复数据
     *
     * 这是实现 UNDO 日志的关键方法
     */
    void before();

    /**
     * 撤销 before() 操作
     *
     * 使用场景：
     * - 当事务决定不修改数据时，撤销之前的 before() 调用
     * - 释放 before() 可能占用的资源
     */
    void unBefore();

    /**
     * 在修改数据项之后调用，用于记录修改操作
     *
     * @param xid 事务ID，标识是哪个事务修改了这个数据项
     *
     * 用途：
     * - 记录是哪个事务修改了这个数据
     * - 为 MVCC（多版本并发控制）提供版本信息
     * - 可能会写入 REDO 日志，用于事务提交后的持久化
     */
    void after(long xid);

    /**
     * 释放对数据项的引用
     *
     * 作用：
     * - 减少该数据项在缓存中的引用计数
     * - 当引用计数为0时，该数据项可以被从缓存中驱逐
     * - 配合 AbstractCache 的引用计数机制使用
     */
    void release();

    /**
     * 获取写锁（排他锁）
     *
     * 使用场景：当要修改数据项时调用
     */
    void lock();

    /**
     * 释放写锁
     */
    void unlock();

    /**
     * 获取读锁（共享锁）
     *
     * 使用场景：当只需要读取数据项时调用
     */
    void rLock();

    /**
     * 释放读锁
     */
    void rUnLock();

    /**
     * 获取数据项所在的页面
     *
     * @return Page 数据页对象
     */
    Page page();

    /**
     * 获取数据项的唯一标识符（UID）
     *
     * UID 的组成：
     * - 高位：页号（PageNumber）
     * - 低位：页内偏移（Offset）
     *
     * 通过 UID 可以唯一定位到数据库中的某个数据项
     *
     * @return 数据项的唯一标识符
     */
    long getUid();

    /**
     * 获取修改前的原始数据
     *
     * @return before() 方法保存的旧数据，用于事务回滚
     */
    byte[] getOldRaw();

    /**
     * 获取数据项的完整原始数据（包含 ValidFlag、DataSize 和 Data）
     *
     * @return SubArray 指向完整数据项的子数组视图
     */
    SubArray getRaw();

    /**
     * 将原始数据包装成 DataItem 的存储格式
     *
     * 存储格式：[ValidFlag=0](1字节) + [DataSize](2字节) + [原始数据]
     *
     * @param raw 原始数据
     * @return 包装后的字节数组，可以直接写入数据页
     *
     * 使用场景：
     * 当插入新数据时，需要将用户数据包装成 DataItem 格式
     *
     * 示例：
     * byte[] userData = "Hello".getBytes();
     * byte[] wrapped = DataItem.wrapDataItemRaw(userData);
     * // wrapped = [0, 0, 5, 'H', 'e', 'l', 'l', 'o']
     * //           ↑  ↑--↑  ↑-----------------↑
     * //         valid size      data
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];  // valid 字段，默认为0（有效）
        byte[] size = Parser.short2Byte((short)raw.length);  // size 字段，2字节
        return Bytes.concat(valid, size, raw);  // 拼接三部分
    }

    /**
     * 从页面的指定偏移位置解析出一个 DataItem 对象
     *
     * 解析过程：
     * 1. 从页面数据中读取 offset 位置的 size 字段（2字节）
     * 2. 计算 DataItem 的总长度 = size + OF_DATA（即 valid + size + data 的总长度）
     * 3. 根据页号和偏移计算出 UID
     * 4. 创建 DataItemImpl 对象
     *
     * @param pg 数据页对象
     * @param offset 数据项在页面中的偏移位置
     * @param dm 数据管理器，用于管理数据项的缓存和持久化
     * @return 解析出的 DataItem 对象
     *
     * 使用场景：
     * - 从磁盘加载数据页后，需要将页面中的字节数据解析成 DataItem 对象
     * - 扫描数据页时，逐个解析页面中的所有数据项
     */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        // 读取 size 字段：从 offset+OF_SIZE 位置读取2字节
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        // 计算完整长度：OF_DATA 是数据部分的起始偏移，size 是数据部分的长度
        short length = (short)(size + DataItemImpl.OF_DATA);
        // 根据页号和页内偏移计算 UID
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        // 创建 DataItemImpl 对象
        // 参数1: SubArray(raw, offset, offset+length) - 指向完整数据项的视图
        // 参数2: new byte[length] - 用于保存修改前的旧数据
        // 参数3: pg - 所在的页面
        // 参数4: uid - 唯一标识符
        // 参数5: dm - 数据管理器
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    /**
     * 将数据项标记为无效（删除）
     *
     * 实现方式：将 ValidFlag 字段设置为1
     *
     * @param raw 数据项的原始字节数组
     *
     * 设计思想：
     * - 采用"软删除"策略，不立即从页面中移除数据
     * - 只是标记为无效，物理空间稍后可以被回收复用
     * - 这种设计简化了删除操作，避免了复杂的页面空间整理
     *
     * 使用场景：
     * 当事务删除一条记录时，调用此方法标记数据项无效
     */
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;  // OF_VALID 是 valid 字段的偏移位置（0）
    }
}