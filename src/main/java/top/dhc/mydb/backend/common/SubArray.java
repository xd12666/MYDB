package top.dhc.mydb.backend.common;

/**
 * SubArray 表示一个字节数组的子区间
 *
 * 设计目的：
 * 1. 避免频繁的数组复制操作，提高性能
 * 2. 在数据库系统中，经常需要处理大块数据的某个片段
 *    例如：从一个数据页中提取某条记录，从日志块中读取某个事务日志
 * 3. 通过引用原数组+起止位置的方式，实现零拷贝的子数组视图
 *
 * 应用场景举例：
 * - 从8KB的数据页中提取某条记录的字节数据
 * - 从日志缓冲区中定位某个事务的日志数据
 * - 解析网络包时，引用数据包的某个字段部分
 *
 * 与 Arrays.copyOfRange() 的对比：
 * - Arrays.copyOfRange() 会创建新数组，消耗内存和CPU
 * - SubArray 只是记录起止位置，不复制数据，性能更好
 */
public class SubArray {

    // 原始字节数组的引用
    // 指向实际存储数据的底层数组，可能是数据页、日志块等
    public byte[] raw;

    // 子区间的起始位置（包含）
    // 表示从 raw 数组的哪个索引开始是有效数据
    public int start;

    // 子区间的结束位置（不包含）
    // 表示到 raw 数组的哪个索引结束（左闭右开区间：[start, end)）
    // 因此实际的数据长度为：end - start
    public int end;

    /**
     * 构造一个字节数组的子区间视图
     *
     * @param raw 原始字节数组
     * @param start 起始位置（包含）
     * @param end 结束位置（不包含），区间为 [start, end)
     *
     * 使用示例：
     * byte[] page = new byte[8192];  // 一个8KB的数据页
     * SubArray record = new SubArray(page, 100, 250);  // 提取第100-249字节，表示一条记录
     * // 此时 record 的实际长度为 250-100=150 字节
     */
    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}