package top.dhc.mydb.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;

import top.dhc.mydb.backend.parser.statement.Create;
import top.dhc.mydb.backend.parser.statement.Delete;
import top.dhc.mydb.backend.parser.statement.Insert;
import top.dhc.mydb.backend.parser.statement.Select;
import top.dhc.mydb.backend.parser.statement.Update;
import top.dhc.mydb.backend.parser.statement.Where;
import top.dhc.mydb.backend.tbm.Field.ParseValueRes;
import top.dhc.mydb.backend.tm.TransactionManagerImpl;
import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.backend.utils.ParseStringRes;
import top.dhc.mydb.backend.utils.Parser;
import top.dhc.mydb.common.Error;

/**
 * Table 维护了表结构和字段信息
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    top.dhc.mydb.backend.tbm.TableManager tbm; // 表管理器，用于管理表操作
    long uid; // 表的唯一标识符
    String name; // 表名
    byte status; // 表状态
    long nextUid; // 下一个表的 UID
    List<Field> fields = new ArrayList<>(); // 表的字段列表

    /**
     * 根据 UID 加载表信息
     */
    public static Table loadTable(top.dhc.mydb.backend.tbm.TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((top.dhc.mydb.backend.tbm.TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid); // 从虚拟存储中读取表数据
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw); // 解析表数据
    }

    /**
     * 创建新表
     */
    public static Table createTable(top.dhc.mydb.backend.tbm.TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);

        // 根据创建语句中字段信息创建 Field 对象
        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for (int j = 0; j < create.index.length; j++) {
                if (fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid); // 持久化表结构
    }

    public Table(top.dhc.mydb.backend.tbm.TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(top.dhc.mydb.backend.tbm.TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 解析表的二进制数据
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw); // 解析表名
        name = res.str;
        position += res.next;

        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8)); // 解析 nextUid
        position += 8;

        // 解析字段列表
        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }

        return this;
    }

    /**
     * 将表持久化
     */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }

        uid = ((top.dhc.mydb.backend.tbm.TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * 删除数据
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where); // 获取满足条件的 UID 列表
        int count = 0;
        for (Long uid : uids) {
            if (((top.dhc.mydb.backend.tbm.TableManagerImpl) tbm).vm.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 更新数据
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if (fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((top.dhc.mydb.backend.tbm.TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;

            ((top.dhc.mydb.backend.tbm.TableManagerImpl) tbm).vm.delete(xid, uid);

            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            raw = entry2Raw(entry);
            long uuid = ((top.dhc.mydb.backend.tbm.TableManagerImpl) tbm).vm.insert(xid, raw);

            count++;

            // 更新索引
            for (Field field : fields) {
                if (field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 查询数据
     */
    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((top.dhc.mydb.backend.tbm.TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 插入数据
     */
    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long uid = ((top.dhc.mydb.backend.tbm.TableManagerImpl) tbm).vm.insert(xid, raw);
        for (Field field : fields) {
            if (field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    /**
     * 将字符串数组转换为字段值字典
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /**
     * 解析 Where 条件，返回匹配的 UID 列表
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single = false;
        Field fd = null;

        if (where == null) {
            // 默认返回全部数据，取第一个索引字段
            for (Field field : fields) {
                if (field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field field : fields) {
                if (field.fieldName.equals(where.singleExp1.field)) {
                    if (!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if (fd == null) {
                throw Error.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }

        List<Long> uids = fd.search(l0, r0);
        if (!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    /**
     * 用于解析 Where 条件的结果
     */
    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        FieldCalRes r;
        switch (where.logicOp) {
            case "":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                if (res.l1 > res.l0) res.l0 = res.l1;
                if (res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    /**
     * 将记录转为可打印字符串
     */
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * 解析记录的二进制数据为字段值 Map
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    /**
     * 将字段值 Map 转换为二进制存储
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for (Field field : fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
