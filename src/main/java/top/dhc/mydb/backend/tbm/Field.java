package top.dhc.mydb.backend.tbm;

import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Bytes;

import top.dhc.mydb.backend.im.BPlusTree;
import top.dhc.mydb.backend.parser.statement.SingleExpression;
import top.dhc.mydb.backend.tm.TransactionManagerImpl;
import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.backend.utils.ParseStringRes;
import top.dhc.mydb.backend.utils.Parser;
import top.dhc.mydb.common.Error;

/**
 * Field 表示数据库中的字段信息。
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果字段没有索引，IndexUid 为 0。
 */
public class Field {
    long uid;  // 字段的唯一标识符（UID）。
    private Table tb;  // 所属的表。
    String fieldName;  // 字段名称。
    String fieldType;  // 字段类型（如：int32, int64, string）。
    private long index;  // 索引的 UID，如果没有索引则为 0。
    private BPlusTree bt;  // BPlusTree 用于索引字段。

    // 从数据库中加载字段信息。
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((top.dhc.mydb.backend.tbm.TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);  // 从虚拟机读取字段数据。
        } catch (Exception e) {
            Panic.panic(e);  // 如果读取过程中发生错误，触发 panic。
        }
        assert raw != null;  // 确保数据不为空。
        return new Field(uid, tb).parseSelf(raw);  // 解析并返回字段对象。
    }

    // 构造函数，用于初始化字段对象。
    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    // 构造函数，提供字段名称、类型和是否有索引。
    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    // 解析字段的二进制数据。
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);  // 解析字段名称。
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));  // 解析字段类型。
        fieldType = res.str;
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));  // 解析索引 UID。

        // 如果字段有索引，加载 BPlusTree。
        if(index != 0) {
            try {
                bt = BPlusTree.load(index, ((top.dhc.mydb.backend.tbm.TableManagerImpl)tb.tbm).dm);  // 加载 BPlusTree。
            } catch(Exception e) {
                Panic.panic(e);  // 如果加载 BPlusTree 失败，触发 panic。
            }
        }
        return this;
    }

    // 创建一个新的字段。
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);  // 检查字段类型是否合法。
        Field f = new Field(tb, fieldName, fieldType, 0);  // 创建字段对象。

        // 如果需要索引，创建 BPlusTree。
        if(indexed) {
            long index = BPlusTree.create(((top.dhc.mydb.backend.tbm.TableManagerImpl)tb.tbm).dm);  // 创建索引。
            BPlusTree bt = BPlusTree.load(index, ((top.dhc.mydb.backend.tbm.TableManagerImpl)tb.tbm).dm);  // 加载 BPlusTree。
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);  // 将字段保存到数据库。
        return f;
    }

    // 将字段对象保存到数据库。
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);  // 将字段名称转换为字节数组。
        byte[] typeRaw = Parser.string2Byte(fieldType);  // 将字段类型转换为字节数组。
        byte[] indexRaw = Parser.long2Byte(index);  // 将索引 UID 转换为字节数组。

        // 将数据插入到虚拟机，并获取字段 UID。
        this.uid = ((top.dhc.mydb.backend.tbm.TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    // 检查字段类型是否合法。
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;  // 如果字段类型不合法，抛出异常。
        }
    }

    // 判断字段是否有索引。
    public boolean isIndexed() {
        return index != 0;
    }

    // 向字段的索引中插入数据。
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);  // 将字段的键转换为 UID。
        bt.insert(uKey, uid);  // 向索引中插入数据。
    }

    // 查询字段的索引范围。
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);  // 返回索引范围内的数据。
    }

    // 将字符串转换为字段的值。
    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);  // 字符串转 int32。
            case "int64":
                return Long.parseLong(str);  // 字符串转 int64。
            case "string":
                return str;  // 直接返回字符串。
        }
        return null;
    }

    // 将字段的值转换为 UID。
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);  // 字符串转 UID。
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;  // int32 转 UID。
            case "int64":
                uid = (long)key;  // int64 转 UID。
                break;
        }
        return uid;
    }

    // 将字段的值转换为字节数组。
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);  // int32 转字节数组。
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);  // int64 转字节数组。
                break;
            case "string":
                raw = Parser.string2Byte((String)v);  // 字符串转字节数组。
                break;
        }
        return raw;
    }

    // 解析字段值。
    class ParseValueRes {
        Object v;
        int shift;
    }

    // 根据二进制数据解析字段值。
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));  // 解析 int32。
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));  // 解析 int64。
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);  // 解析字符串。
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    // 打印字段值。
    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);  // 打印 int32。
                break;
            case "int64":
                str = String.valueOf((long)v);  // 打印 int64。
                break;
            case "string":
                str = (String)v;  // 打印字符串。
                break;
        }
        return str;
    }

    // 重写 toString() 方法，打印字段信息。
    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index != 0 ? ", Index" : ", NoIndex")
                .append(")")
                .toString();
    }

    // 计算字段表达式的值。
    public top.dhc.mydb.backend.tbm.FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        top.dhc.mydb.backend.tbm.FieldCalRes res = new top.dhc.mydb.backend.tbm.FieldCalRes();
        switch(exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if(res.right > 0) {
                    res.right--;  // 如果是 < 操作，将右边值减 1。
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;  // 如果是 = 操作，左右值相等。
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;  // 如果是 > 操作，左边值加 1。
                break;
        }
        return res;
    }
}
