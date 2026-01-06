package top.dhc.mydb.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import top.dhc.mydb.backend.common.SubArray;
import top.dhc.mydb.backend.dm.dataItem.DataItem;
import top.dhc.mydb.backend.dm.logger.Logger;
import top.dhc.mydb.backend.dm.page.Page;
import top.dhc.mydb.backend.dm.page.PageX;
import top.dhc.mydb.backend.dm.pageCache.PageCache;
import top.dhc.mydb.backend.tm.TransactionManager;
import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.backend.utils.Parser;

// 崩溃恢复类，实现数据库的崩溃恢复机制
// 当数据库异常关闭时，通过重放日志来恢复数据到一致状态
// 采用类似ARIES的恢复算法：分析、重做、撤销三个阶段
// 核心思想：已提交事务的修改要重做REDO，未提交事务的修改要撤销UNDO
public class Recover {

    // 日志类型：插入操作
    private static final byte LOG_TYPE_INSERT = 0;

    // 日志类型：更新操作
    private static final byte LOG_TYPE_UPDATE = 1;

    // 恢复操作类型：重做，用于已提交事务
    private static final int REDO = 0;

    // 恢复操作类型：撤销，用于未提交事务
    private static final int UNDO = 1;

    // 插入日志信息结构
    // 记录插入操作的所有必要信息，用于恢复时重放或撤销
    static class InsertLogInfo {
        long xid;       // 事务ID
        int pgno;       // 页号
        short offset;   // 页内偏移
        byte[] raw;     // 插入的原始数据
    }

    // 更新日志信息结构
    // 记录更新操作的旧值和新值，支持重做和撤销
    static class UpdateLogInfo {
        long xid;       // 事务ID
        int pgno;       // 页号
        short offset;   // 页内偏移
        byte[] oldRaw;  // 更新前的数据
        byte[] newRaw;  // 更新后的数据
    }

    // 崩溃恢复的主流程
    // 恢复分为三个阶段：
    // 1. 分析阶段：扫描日志，确定需要恢复的最大页号，截断文件
    // 2. 重做阶段：对所有已提交事务的操作进行重做
    // 3. 撤销阶段：对所有未提交事务的操作进行撤销
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        // 分析阶段：扫描日志，找到涉及的最大页号
        lg.rewind();  // 将日志指针重置到开头
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();  // 读取下一条日志
            if(log == null) break;  // 日志读完，退出循环

            int pgno;
            // 判断日志类型并解析出页号
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            // 记录最大页号
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }

        // 如果没有日志，至少保留第一页
        if(maxPgno == 0) {
            maxPgno = 1;
        }

        // 截断数据库文件到最大页号
        // 删除日志中未涉及的多余页面，这些页面可能是崩溃时写入一半的脏数据
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        // 重做阶段：重放所有已提交事务的操作
        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        // 撤销阶段：回滚所有未提交事务的操作
        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    // 重做阶段：重放已提交事务的所有操作
    // 遍历所有日志，对已提交（非active）的事务进行重做
    // 重做INSERT：将数据重新插入到页面
    // 重做UPDATE：将数据更新为新值
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();  // 将日志指针重置到开头
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;

            if(isInsertLog(log)) {
                // 解析插入日志
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                // 如果事务不是active状态（已提交或已回滚），则重做
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 解析更新日志
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                // 如果事务不是active状态，则重做
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    // 撤销阶段：回滚未提交事务的所有操作
    // 步骤：
    // 1. 遍历日志，收集所有active事务的日志到logCache
    // 2. 对每个active事务的日志倒序执行撤销操作
    // 3. 将这些事务标记为已回滚
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 用Map存储每个active事务的所有日志
        // key: 事务ID，value: 该事务的所有日志列表
        Map<Long, List<byte[]>> logCache = new HashMap<>();

        lg.rewind();
        // 第一次扫描：收集所有active事务的日志
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;

            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                // 如果事务是active状态（未提交）
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active事务的日志进行倒序撤销
        // 倒序是因为要先撤销后执行的操作，再撤销先执行的操作
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            // 从后往前遍历日志列表
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);  // 撤销插入：将数据标记为无效
                } else {
                    doUpdateLog(pc, log, UNDO);  // 撤销更新：恢复为旧值
                }
            }
            // 将事务标记为已回滚
            tm.abort(entry.getKey());
        }
    }

    // 判断是否为插入日志
    // 通过日志的第一个字节（类型标识）判断
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // 更新日志的格式：[LogType] [XID] [UID] [OldRaw] [NewRaw]
    // LogType: 1字节，日志类型
    // XID: 8字节，事务ID
    // UID: 8字节，数据项的唯一标识
    // OldRaw: 变长，更新前的数据
    // NewRaw: 变长，更新后的数据
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    // 生成更新日志
    // 将DataItem的修改操作序列化为日志字节数组
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();  // 获取修改前的数据
        SubArray raw = di.getRaw();      // 获取修改后的数据
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        // 将所有字段拼接成完整的日志
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    // 解析更新日志
    // 从日志字节数组中提取各个字段
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        // 解析事务ID
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        // 解析UID，并从中提取页号和偏移
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));  // UID低16位是偏移
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));      // UID中间32位是页号

        // 解析旧值和新值
        // 剩余数据长度的一半是旧值，另一半是新值
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    // 执行更新日志的恢复操作
    // flag=REDO：使用新值进行更新（重做）
    // flag=UNDO：使用旧值进行更新（撤销）
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;

        if(flag == REDO) {
            // 重做：使用新值
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            // 撤销：使用旧值
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }

        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 在页面的指定位置写入数据（不更新FSO）
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // 插入日志的格式：[LogType] [XID] [Pgno] [Offset] [Raw]
    // LogType: 1字节，日志类型
    // XID: 8字节，事务ID
    // Pgno: 4字节，页号
    // Offset: 2字节，页内偏移
    // Raw: 变长，插入的数据
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    // 生成插入日志
    // 记录在哪个页面的哪个位置插入了什么数据
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));  // 记录插入时的FSO位置
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    // 解析插入日志
    // 从日志字节数组中提取各个字段
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    // 执行插入日志的恢复操作
    // flag=REDO：重新插入数据（数据保持有效）
    // flag=UNDO：撤销插入（将数据标记为无效）
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                // 撤销插入：将数据的valid标志设为无效
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 在指定位置插入数据（或重新插入已标记为无效的数据）
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}