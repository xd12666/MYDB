package top.dhc.mydb.backend.dm.page;

import java.util.Arrays;

import top.dhc.mydb.backend.dm.pageCache.PageCache;
import top.dhc.mydb.backend.utils.Parser;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {

    // FSO(Free Space Offset)在页面中的偏移位置，固定为0，即页面的前2个字节存储FSO
    private static final short OF_FREE = 0;

    // 数据区域的起始偏移位置，固定为2，即从第3个字节开始存储实际数据
    private static final short OF_DATA = 2;

    // 页面最大可用空间 = 页面总大小(8192字节) - FSO占用的2字节 = 8190字节
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /**
     * 初始化一个空白的原始页面数据
     * 创建一个PAGE_SIZE大小的字节数组，并将FSO初始化为OF_DATA(2)
     * 表示数据区域从第2个字节后开始，前2字节用于存储FSO值
     * @return 初始化后的字节数组
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];  // 创建8KB的字节数组
        setFSO(raw, OF_DATA);  // 设置初始FSO为2，表示空闲空间从第2字节开始
        return raw;
    }

    /**
     * 设置页面的FSO(空闲空间偏移)值
     * 将short类型的偏移值转换为2字节，写入到页面的前2个字节位置
     * @param raw 页面字节数组
     * @param ofData 要设置的FSO值
     */
    private static void setFSO(byte[] raw, short ofData) {
        // 将short转为2字节数组，复制到raw的0-1位置(OF_FREE到OF_FREE+OF_DATA)
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取页面对象的FSO值
     * @param pg 页面对象
     * @return FSO值，表示当前空闲空间的起始偏移
     */
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());  // 委托给重载方法处理
    }

    /**
     * 从字节数组中读取FSO值
     * 读取前2个字节并解析为short类型
     * @param raw 页面字节数组
     * @return FSO值
     */
    private static short getFSO(byte[] raw) {
        // 复制raw的0-1字节(共2字节)，解析为short返回
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将数据插入到页面中
     * 在页面的空闲位置(FSO指向的位置)插入数据，然后更新FSO
     * @param pg 目标页面对象
     * @param raw 要插入的数据字节数组
     * @return 数据插入的起始偏移位置
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);  // 标记页面为脏页，表示页面已被修改，需要写回磁盘
        short offset = getFSO(pg.getData());  // 获取当前空闲空间的起始位置
        // 将raw数据复制到页面的offset位置
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 更新FSO = 原offset + 插入数据的长度，指向新的空闲位置
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;  // 返回本次插入的起始位置，用于后续定位这条数据
    }

    /**
     * 获取页面的剩余空闲空间大小
     * @param pg 页面对象
     * @return 剩余可用字节数
     */
    public static int getFreeSpace(Page pg) {
        // 空闲空间 = 页面总大小 - 当前FSO值
        // 例如：FSO=100，表示0-99已使用，100-8191可用，空闲=8192-100=8092
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /**
     * 恢复插入操作 - 用于数据库崩溃恢复
     * 在指定的offset位置插入数据，并根据情况更新FSO
     * 与普通insert不同的是，这里的offset是指定的，不是从FSO获取的
     *
     * @param pg 目标页面对象
     * @param raw 要插入的数据
     * @param offset 指定的插入位置偏移
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);  // 标记页面为脏页
        // 将数据复制到指定的offset位置
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        // 获取当前页面的FSO值
        short rawFSO = getFSO(pg.getData());
        // 如果新数据的结束位置超过了当前FSO，则更新FSO为新的结束位置
        // 这样做是因为恢复时操作可能乱序执行，需要保证FSO指向最大的已使用位置
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    /**
     * 恢复更新操作 - 用于数据库崩溃恢复
     * 在指定的offset位置覆写数据，不更新FSO
     * 因为是更新已有数据，不改变页面的空闲空间位置
     *
     * @param pg 目标页面对象
     * @param raw 要写入的数据
     * @param offset 指定的写入位置偏移
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);  // 标记页面为脏页
        // 将数据复制到指定的offset位置，覆盖原有数据
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 注意：这里不更新FSO，因为只是更新现有数据，没有占用新的空间
    }
}