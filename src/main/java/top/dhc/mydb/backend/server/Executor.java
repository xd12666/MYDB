package top.dhc.mydb.backend.server;

// 导入解析器、语句对象、错误处理和表管理器相关的类。
import top.dhc.mydb.backend.parser.Parser;
import top.dhc.mydb.backend.parser.statement.Abort;
import top.dhc.mydb.backend.parser.statement.Begin;
import top.dhc.mydb.backend.parser.statement.Commit;
import top.dhc.mydb.backend.parser.statement.Create;
import top.dhc.mydb.backend.parser.statement.Delete;
import top.dhc.mydb.backend.parser.statement.Insert;
import top.dhc.mydb.backend.parser.statement.Select;
import top.dhc.mydb.backend.parser.statement.Show;
import top.dhc.mydb.backend.parser.statement.Update;
import top.dhc.mydb.backend.tbm.BeginRes;
import top.dhc.mydb.backend.tbm.TableManager;
import top.dhc.mydb.common.Error;

public class Executor {
    private long xid;  // 用于跟踪当前事务的 ID。
    TableManager tbm;  // 表管理器，用于操作数据库表。

    // 构造函数，接受一个表管理器对象。
    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;  // 初始时没有事务 ID。
    }

    // close() 方法：在执行器关闭时，检查是否存在未提交的事务，如果有，则执行回滚操作。
    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);  // 如果存在未提交事务，打印异常信息。
            tbm.abort(xid);  // 执行回滚操作。
        }
    }

    // execute() 方法：执行 SQL 语句，根据不同的语句类型（如 BEGIN、COMMIT、ABORT 等）执行不同操作。
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));  // 打印执行的 SQL 语句。
        // 使用 Parser 解析 SQL 语句，获取解析后的对象。
        Object stat = Parser.Parse(sql);
        if(Begin.class.isInstance(stat)) {  // 如果是 BEGIN 语句。
            if(xid != 0) {  // 如果当前已有事务，抛出异常。
                throw Error.NestedTransactionException;
            }
            // 启动新事务，并返回事务结果。
            BeginRes r = tbm.begin((Begin)stat);
            xid = r.xid;  // 设置当前事务 ID。
            return r.result;  // 返回事务结果。
        } else if(Commit.class.isInstance(stat)) {  // 如果是 COMMIT 语句。
            if(xid == 0) {  // 如果没有事务，抛出异常。
                throw Error.NoTransactionException;
            }
            // 提交事务，并返回结果。
            byte[] res = tbm.commit(xid);
            xid = 0;  // 重置事务 ID。
            return res;  // 返回提交结果。
        } else if(Abort.class.isInstance(stat)) {  // 如果是 ABORT 语句。
            if(xid == 0) {  // 如果没有事务，抛出异常。
                throw Error.NoTransactionException;
            }
            // 回滚事务，并返回结果。
            byte[] res = tbm.abort(xid);
            xid = 0;  // 重置事务 ID。
            return res;  // 返回回滚结果。
        } else {
            // 其他 SQL 语句，调用 execute2() 方法处理。
            return execute2(stat);
        }
    }

    // execute2() 方法：处理除事务控制语句外的其他 SQL 语句，如 SHOW、CREATE、SELECT、INSERT、DELETE、UPDATE 等。
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;  // 标记是否需要临时事务。
        Exception e = null;  // 用于捕获异常。
        // 如果当前没有事务，则启动一个临时事务。
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());  // 启动新事务。
            xid = r.xid;  // 设置事务 ID。
        }
        try {
            byte[] res = null;
            // 根据语句类型调用不同的表管理器方法处理 SQL 语句。
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);  // 处理 SHOW 语句。
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);  // 处理 CREATE 语句。
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);  // 处理 SELECT 语句。
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);  // 处理 INSERT 语句。
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);  // 处理 DELETE 语句。
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);  // 处理 UPDATE 语句。
            }
            return res;  // 返回执行结果。
        } catch(Exception e1) {
            e = e1;  // 捕获异常。
            throw e;  // 抛出异常。
        } finally {
            // 如果是临时事务，处理事务提交或回滚。
            if(tmpTransaction) {
                if(e != null) {  // 如果发生异常，回滚事务。
                    tbm.abort(xid);
                } else {  // 如果没有异常，提交事务。
                    tbm.commit(xid);
                }
                xid = 0;  // 重置事务 ID。
            }
        }
    }
}
