package top.dhc.mydb.backend.dm;

import top.dhc.mydb.backend.common.AbstractCache;
import top.dhc.mydb.backend.dm.dataItem.DataItem;
import top.dhc.mydb.backend.dm.dataItem.DataItemImpl;
import top.dhc.mydb.backend.dm.logger.Logger;
import top.dhc.mydb.backend.dm.page.Page;
import top.dhc.mydb.backend.dm.page.PageOne;
import top.dhc.mydb.backend.dm.page.PageX;
import top.dhc.mydb.backend.dm.pageCache.PageCache;
import top.dhc.mydb.backend.dm.pageIndex.PageIndex;
import top.dhc.mydb.backend.dm.pageIndex.PageInfo;
import top.dhc.mydb.backend.tm.TransactionManager;
import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.backend.utils.Types;
import top.dhc.mydb.common.Error;
import top.dhc.mydb.backend.dm.Recover;

// 数据管理器实现类，是DM层的核心实现
// 继承AbstractCache实现DataItem的缓存管理
// 负责数据的读写、日志记录、页面管理、事务协调等核心功能
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    // 事务管理器，用于检查事务状态和管理事务
    TransactionManager tm;

    // 页面缓存，管理页面在内存和磁盘之间的交换
    PageCache pc;

    // 日志管理器，记录所有的修改操作日志，用于崩溃恢复
    Logger logger;

    // 页面索引，快速查找有足够空闲空间的页面
    PageIndex pIndex;

    // 第一页，存储数据库的元信息，如启动检查字段
    Page pageOne;

    // 构造函数
    // pc: 页面缓存
    // logger: 日志管理器
    // tm: 事务管理器
    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);  // 调用父类构造函数，0表示不限制DataItem缓存数量
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();  // 初始化页面索引
    }

    // 根据UID读取数据项
    // 先从缓存获取DataItem，如果不在缓存则通过getForCache从磁盘加载
    // 检查数据项的有效性（valid标志），无效则返回null
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);  // 从缓存获取或加载
        if(!di.isValid()) {  // 检查数据项是否有效
            di.release();  // 无效数据释放引用
            return null;
        }
        return di;
    }

    // 插入数据，返回数据的UID
    // 插入流程：
    // 1. 将原始数据包装成DataItem格式（添加valid标志和size字段）
    // 2. 检查数据大小是否超过页面最大空闲空间
    // 3. 从页面索引中选择有足够空间的页面，最多尝试5次
    // 4. 如果没有合适页面，创建新页面并加入索引
    // 5. 获取选中的页面，记录插入日志
    // 6. 将数据插入页面，返回UID
    // 7. 更新页面索引（将页面的最新空闲空间重新加入索引）
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 将原始数据包装成DataItem格式，添加valid和size字段
        byte[] raw = DataItem.wrapDataItemRaw(data);

        // 检查数据大小是否超过单页最大空闲空间（8190字节）
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 从页面索引中查找合适的页面，最多尝试5次
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);  // 查找有足够空间的页面
            if (pi != null) {
                break;  // 找到合适页面，退出循环
            } else {
                // 没有合适页面，创建新页面
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);  // 将新页面加入索引
            }
        }

        // 5次尝试后仍未找到合适页面，说明数据库繁忙
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            // 获取选中的页面
            pg = pc.getPage(pi.pgno);

            // 生成插入操作的日志并写入日志文件
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            // 将数据插入页面，返回插入位置的偏移量
            short offset = PageX.insert(pg, raw);

            pg.release();  // 释放页面引用

            // 将页号和偏移量组合成UID返回
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将页面的最新空闲空间重新加入索引
            // 无论插入成功还是失败，都需要更新索引
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    // 关闭数据管理器
    // 关闭流程：
    // 1. 调用父类close，刷新所有缓存的DataItem
    // 2. 关闭日志文件
    // 3. 设置PageOne的vcClose标志，表示正常关闭
    // 4. 释放PageOne并刷新到磁盘
    // 5. 关闭页面缓存，刷新所有脏页
    @Override
    public void close() {
        super.close();  // 关闭DataItem缓存
        logger.close();  // 关闭日志

        PageOne.setVcClose(pageOne);  // 设置正常关闭标志
        pageOne.release();  // 释放第一页
        pc.close();  // 关闭页面缓存
    }

    // 为数据项的修改操作生成update日志
    // 当DataItem被修改时调用此方法记录日志
    // xid: 修改数据的事务ID
    // di: 被修改的数据项
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    // 释放数据项的引用
    // 减少DataItem的引用计数，当引用为0时可以被驱逐出缓存
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    // 从磁盘加载DataItem（AbstractCache的回调方法）
    // 当缓存未命中时，通过UID从页面中解析出DataItem
    // UID的结构：高32位是页号，低16位是页内偏移
    // 解析流程：
    // 1. 从UID中提取页内偏移（低16位）
    // 2. 从UID中提取页号（中间32位）
    // 3. 获取对应的页面
    // 4. 从页面中解析出DataItem
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        // 提取页内偏移：取UID的低16位
        short offset = (short)(uid & ((1L << 16) - 1));
        // 右移32位，将页号移到低位
        uid >>>= 32;
        // 提取页号：取低32位
        int pgno = (int)(uid & ((1L << 32) - 1));

        // 获取页面
        Page pg = pc.getPage(pgno);
        // 从页面的offset位置解析DataItem
        return DataItem.parseDataItem(pg, offset, this);
    }

    // 释放DataItem时的回调方法（AbstractCache的回调方法）
    // 当DataItem被驱逐出缓存时调用
    // 释放DataItem所在页面的引用
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 初始化第一页PageOne
    // 在创建新数据库文件时调用
    // PageOne存储数据库的启动检查信息，用于判断数据库是否正常关闭
    void initPageOne() {
        // 创建第一页，页号必须是1
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;  // 断言第一页的页号是1
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);  // 刷新第一页到磁盘
    }

    // 加载并校验第一页PageOne
    // 在打开已有数据库文件时调用
    // 检查第一页的校验字段，判断数据库上次是否正常关闭
    // 返回true表示正常关闭，返回false表示异常关闭需要恢复
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);  // 读取第一页
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);  // 校验第一页的vc字段
    }

    // 填充页面索引
    // 在打开数据库时调用，扫描所有页面构建空闲空间索引
    // 从第2页开始扫描（第1页是PageOne元数据页）
    // 将每个页面的空闲空间信息加入PageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();  // 获取总页数

        // 从第2页开始遍历（第1页是PageOne）
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);  // 获取页面
            } catch (Exception e) {
                Panic.panic(e);
            }
            // 将页面的空闲空间信息加入索引
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();  // 释放页面引用
        }
    }

}