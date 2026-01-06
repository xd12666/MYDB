// 定义一个名为 Parser 的类，该类位于 top.dhc.mydb.backend.parser 包中。
// 这个类的主要功能是解析传入的 SQL 语句，根据不同的命令执行不同的操作。
// 解析的过程是通过将 SQL 语句分解为标记（token），然后通过不同的解析方法处理每种命令。

package top.dhc.mydb.backend.parser;

// 导入所需的类，包括数据结构（如 ArrayList）、以及各个 SQL 语句的声明类（如 Abort、Begin、Commit 等）。
import java.util.ArrayList;
import java.util.List;

import top.dhc.mydb.backend.parser.statement.Abort;
import top.dhc.mydb.backend.parser.statement.Begin;
import top.dhc.mydb.backend.parser.statement.Commit;
import top.dhc.mydb.backend.parser.statement.Create;
import top.dhc.mydb.backend.parser.statement.Delete;
import top.dhc.mydb.backend.parser.statement.Drop;
import top.dhc.mydb.backend.parser.statement.Insert;
import top.dhc.mydb.backend.parser.statement.Select;
import top.dhc.mydb.backend.parser.statement.Show;
import top.dhc.mydb.backend.parser.statement.SingleExpression;
import top.dhc.mydb.backend.parser.statement.Update;
import top.dhc.mydb.backend.parser.statement.Where;
import top.dhc.mydb.common.Error;

// Parser 类负责解析 SQL 语句，返回适当的语句对象。
public class Parser {

    // Parse 方法是整个解析的入口，接收一个字节数组作为 SQL 语句，返回一个 Object 类型的解析结果（具体是某种 SQL 语句的实例）。
    // 解析过程依赖于 Tokenizer 来逐个标记地处理 SQL 语句。
    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);  // 初始化一个 Tokenizer 用于分解 SQL 语句。
        String token = tokenizer.peek();  // 获取当前的第一个标记。
        tokenizer.pop();  // 弹出第一个标记。

        Object stat = null;  // 用来保存解析结果的对象。
        Exception statErr = null;  // 用来保存解析过程中遇到的异常。

        try {
            // 根据第一个标记来判断是哪种 SQL 命令，并调用相应的解析方法。
            switch(token) {
                case "begin":
                    stat = parseBegin(tokenizer);  // 解析 BEGIN 语句
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);  // 解析 COMMIT 语句
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);  // 解析 ABORT 语句
                    break;
                case "create":
                    stat = parseCreate(tokenizer);  // 解析 CREATE 语句
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);  // 解析 DROP 语句
                    break;
                case "select":
                    stat = parseSelect(tokenizer);  // 解析 SELECT 语句
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);  // 解析 INSERT 语句
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);  // 解析 DELETE 语句
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);  // 解析 UPDATE 语句
                    break;
                case "show":
                    stat = parseShow(tokenizer);  // 解析 SHOW 语句
                    break;
                default:
                    throw Error.InvalidCommandException;  // 如果是无效的 SQL 命令，抛出异常。
            }
        } catch(Exception e) {
            statErr = e;  // 如果解析过程中发生异常，记录异常。
        }

        try {
            String next = tokenizer.peek();  // 检查是否还有多余的标记。
            if(!"".equals(next)) {  // 如果还有标记，说明语句不完整，抛出异常。
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch(Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }

        if(statErr != null) {
            throw statErr;  // 如果存在解析错误，抛出错误。
        }
        return stat;  // 返回解析后的 SQL 语句对象。
    }

    // 解析 SHOW 语句的方法。如果没有其他标记，则返回一个新的 Show 对象。
    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            return new Show();
        }
        throw Error.InvalidCommandException;  // 如果标记不符合预期，抛出异常。
    }

    // 解析 UPDATE 语句的方法。根据 SQL 标记的顺序，提取表名、字段名和值等信息。
    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();  // 获取表名
        tokenizer.pop();

        if(!"set".equals(tokenizer.peek())) {  // 检查是否有 "set"
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();  // 获取字段名
        tokenizer.pop();

        if(!"=".equals(tokenizer.peek())) {  // 检查是否有 "="
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.value = tokenizer.peek();  // 获取字段值
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {  // 如果没有其他标记，结束解析
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);  // 解析 WHERE 子句
        return update;
    }

    // 解析 DELETE 语句的方法，提取表名和 WHERE 子句。
    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if(!"from".equals(tokenizer.peek())) {  // 检查是否有 "from"
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();  // 获取表名
        if(!isName(tableName)) {  // 如果表名不合法，抛出异常
            throw Error.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);  // 解析 WHERE 子句
        return delete;
    }

    // 解析 INSERT 语句的方法，提取表名和插入的值。
    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if(!"into".equals(tokenizer.peek())) {  // 检查是否有 "into"
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();  // 获取表名
        if(!isName(tableName)) {  // 如果表名不合法，抛出异常
            throw Error.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if(!"values".equals(tokenizer.peek())) {  // 检查是否有 "values"
            throw Error.InvalidCommandException;
        }

        // 解析插入的值
        List<String> values = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }

    // 解析 SELECT 语句的方法，提取查询的字段和表名。
    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if("*".equals(asterisk)) {  // 如果是 "*", 表示查询所有字段
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while(true) {
                String field = tokenizer.peek();
                if(!isName(field)) {  // 如果字段名不合法，抛出异常
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if(",".equals(tokenizer.peek())) {  // 如果有逗号，继续解析下一个字段
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);

        if(!"from".equals(tokenizer.peek())) {  // 检查是否有 "from"
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();  // 获取表名
        if(!isName(tableName)) {  // 如果表名不合法，抛出异常
            throw Error.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {  // 如果没有 WHERE 子句，结束解析
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);  // 解析 WHERE 子句
        return read;
    }

    // 解析 WHERE 子句的方法。
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!"where".equals(tokenizer.peek())) {  // 检查是否有 "where"
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);  // 解析第一个表达式
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {  // 如果没有逻辑操作符，结束解析
            where.logicOp = logicOp;
            return where;
        }
        if(!isLogicOp(logicOp)) {  // 如果逻辑操作符不合法，抛出异常
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);  // 解析第二个表达式
        where.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {  // 如果还有多余的标记，抛出异常
            throw Error.InvalidCommandException;
        }
        return where;
    }

    // 解析单个表达式（字段、操作符和值）的方法。
    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if(!isName(field)) {  // 如果字段名不合法，抛出异常
            throw Error.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if(!isCmpOp(op)) {  // 如果操作符不合法，抛出异常
            throw Error.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();  // 获取字段值
        tokenizer.pop();
        return exp;
    }

    // 判断操作符是否是合法的比较操作符（=、>、<）。
    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    // 判断操作符是否是合法的逻辑操作符（and、or）。
    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    // 解析 DROP 语句的方法，提取要删除的表名。
    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {  // 检查是否有 "table"
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();  // 获取表名
        if(!isName(tableName)) {  // 如果表名不合法，抛出异常
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {  // 如果有额外的标记，抛出异常
            throw Error.InvalidCommandException;
        }

        Drop drop = new Drop();
        drop.tableName = tableName;  // 设置要删除的表名
        return drop;
    }

    // 解析 CREATE 语句的方法，提取表名和字段信息。
    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {  // 检查是否有 "table"
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();  // 获取表名
        if(!isName(name)) {  // 如果表名不合法，抛出异常
            throw Error.InvalidCommandException;
        }
        create.tableName = name;

        // 解析字段信息，包括字段名和字段类型。
        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if("(".equals(field)) {  // 如果遇到 "(", 则开始解析字段定义
                break;
            }

            if(!isName(field)) {  // 如果字段名不合法，抛出异常
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {  // 如果字段类型不合法，抛出异常
                throw Error.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            String next = tokenizer.peek();
            if(",".equals(next)) {  // 如果有逗号，继续解析下一个字段
                continue;
            } else if("".equals(next)) {  // 如果没有更多字段，则抛出异常
                throw Error.TableNoIndexException;
            } else if("(".equals(next)) {  // 如果遇到 "(" 结束字段解析
                break;
            } else {
                throw Error.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {  // 检查是否有 "index"
            throw Error.InvalidCommandException;
        }

        // 解析索引信息
        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {  // 如果遇到 ")", 结束索引解析
                break;
            }
            if(!isName(field)) {  // 如果字段名不合法，抛出异常
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {  // 如果还有多余的标记，抛出异常
            throw Error.InvalidCommandException;
        }
        return create;
    }

    // 判断数据类型是否合法。
    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }

    // 解析 ABORT 语句的方法。如果没有其他标记，返回一个新的 Abort 对象。
    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {  // 如果有多余标记，抛出异常
            throw Error.InvalidCommandException;
        }
        return new Abort();
    }

    // 解析 COMMIT 语句的方法。如果没有其他标记，返回一个新的 Commit 对象。
    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {  // 如果有多余标记，抛出异常
            throw Error.InvalidCommandException;
        }
        return new Commit();
    }

    // 解析 BEGIN 语句的方法，根据 SQL 语句中的隔离级别来设置 BEGIN 对象。
    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();  // 获取隔离级别
        Begin begin = new Begin();
        if("".equals(isolation)) {  // 如果没有隔离级别，返回默认的 BEGIN 对象
            return begin;
        }
        if(!"isolation".equals(isolation)) {  // 检查是否有 "isolation"
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        String level = tokenizer.peek();  // 获取隔离级别
        if(!"level".equals(level)) {  // 如果没有 "level"，抛出异常
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if("read".equals(tmp1)) {  // 如果是 "read"，表示隔离级别为读取已提交
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("committed".equals(tmp2)) {
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {  // 如果有多余的标记，抛出异常
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else if("repeatable".equals(tmp1)) {  // 如果是 "repeatable"，表示隔离级别为可重复读取
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {  // 如果有多余的标记，抛出异常
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else {
            throw Error.InvalidCommandException;  // 如果不符合任何合法隔离级别，抛出异常
        }
    }

    // 判断字段名是否合法。
    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
