package top.dhc.mydb.backend.tbm;

public class BeginRes {
    public long xid;  // 事务的 ID，用于标识一个事务。
    public byte[] result;  // 存储事务开始操作的结果，通常是一些反馈信息或状态。
}
