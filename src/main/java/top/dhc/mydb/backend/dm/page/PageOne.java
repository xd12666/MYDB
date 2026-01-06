package top.dhc.mydb.backend.dm.page;

import java.util.Arrays;

import top.dhc.mydb.backend.dm.pageCache.PageCache;
import top.dhc.mydb.backend.utils.RandomUtil;

/**
 * PageOne 数据库文件的第一页特殊管理类
 *
 * 核心功能：ValidCheck 有效性检查机制
 * 用于判断数据库上一次是否正常关闭，防止数据损坏
 *
 * ValidCheck 工作原理：
 * 1. 数据库启动时：在第一页的100-107字节处写入一个随机的8字节数据
 * 2. 数据库关闭时：将100-107字节的数据拷贝到108-115字节
 * 3. 下次启动时：比较100-107字节和108-115字节是否相同
 *    - 相同：说明上次正常关闭，数据库状态一致
 *    - 不同：说明上次异常关闭（崩溃、断电等），需要进行恢复
 *
 * 设计思路：
 * 通过对比启动标记和关闭标记来判断数据库状态
 * 如果数据库异常退出（崩溃、强制终止），关闭标记不会被写入，两段数据就不相同
 *
 * 为什么使用100-115字节：
 * 避开页面开头可能被其他元数据使用的区域，选择一个相对安全的位置存储检查标记
 *
 * 使用场景：
 * 数据库启动时调用 checkVc 检查状态，如果检查失败则触发崩溃恢复流程
 */
public class PageOne {

    // ValidCheck 起始偏移位置：第100字节
    // 这个位置存储启动时写入的随机字节
    private static final int OF_VC = 100;

    // ValidCheck 数据长度：8字节
    // 使用8字节的随机数，碰撞概率极低，足够保证检查的准确性
    private static final int LEN_VC = 8;

    /**
     * 初始化第一页的原始数据
     *
     * 调用时机：创建新数据库时调用
     *
     * 返回值：初始化好的第一页字节数组，大小为 PAGE_SIZE
     *
     * 执行步骤：
     * 1. 创建一个页面大小的字节数组
     * 2. 调用 setVcOpen 写入启动标记
     * 3. 返回初始化好的数组
     *
     * 注意：此时108-115字节是空的（全0），与100-107字节不同
     * 这表示数据库刚创建，尚未正常关闭过
     */
    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 设置数据库启动标记（Page 版本）
     *
     * 参数 pg：第一页的 Page 对象
     *
     * 调用时机：数据库启动时调用
     *
     * 执行步骤：
     * 1. 标记页面为脏页，确保会被写回磁盘
     * 2. 在100-107字节处写入随机数据
     *
     * 作用：
     * 写入新的启动标记，此时108-115字节还是旧的关闭标记（或空）
     * 两段数据不同，表示数据库正在运行中
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 设置数据库启动标记（字节数组版本）
     *
     * 参数 raw：第一页的字节数组
     *
     * 实现：
     * 在100-107字节处写入8字节的随机数据
     *
     * 随机数据的作用：
     * 每次启动都不同，防止意外碰撞，确保检查机制的可靠性
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 设置数据库关闭标记（Page 版本）
     *
     * 参数 pg：第一页的 Page 对象
     *
     * 调用时机：数据库正常关闭时调用
     *
     * 执行步骤：
     * 1. 标记页面为脏页，确保会被写回磁盘
     * 2. 将100-107字节的启动标记拷贝到108-115字节
     *
     * 作用：
     * 将启动标记复制为关闭标记，使两段数据相同
     * 这表示数据库已正常关闭，下次启动时检查会通过
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 设置数据库关闭标记（字节数组版本）
     *
     * 参数 raw：第一页的字节数组
     *
     * 实现：
     * 将100-107字节的数据拷贝到108-115字节
     *
     * 结果：
     * 100-107字节（启动标记）和108-115字节（关闭标记）内容相同
     * 表示数据库正常关闭，两个标记匹配
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    /**
     * 检查数据库是否正常关闭（Page 版本）
     *
     * 参数 pg：第一页的 Page 对象
     *
     * 返回值：true 表示上次正常关闭，false 表示上次异常关闭
     *
     * 调用时机：数据库启动时调用
     *
     * 检查逻辑：
     * 对比100-107字节和108-115字节是否相同
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 检查数据库是否正常关闭（字节数组版本）
     *
     * 参数 raw：第一页的字节数组
     *
     * 返回值：true 表示上次正常关闭，false 表示上次异常关闭
     *
     * 实现：
     * 使用 Arrays.equals 比较100-107字节和108-115字节是否完全相同
     *
     * 判断逻辑：
     * 1. 相同：上次调用了 setVcClose，数据库正常关闭
     * 2. 不同：上次未调用 setVcClose，数据库异常关闭（崩溃、断电、强制终止等）
     *
     * 后续处理：
     * 如果返回 false，需要触发崩溃恢复流程，通过日志恢复数据到一致状态
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}