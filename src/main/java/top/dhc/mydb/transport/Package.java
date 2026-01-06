package top.dhc.mydb.transport;

/**
 * 数据包（Package）
 * 
 * 职责：
 * 封装客户端和服务器之间传输的数据
 * 可以包含正常数据或错误信息
 * 
 * 数据包类型：
 * - 正常数据包：data不为null，err为null
 * - 错误数据包：data为null，err不为null
 * 
 * 使用场景：
 * - 客户端发送SQL语句：Package(sql_bytes, null)
 * - 服务器返回结果：Package(result_bytes, null)
 * - 服务器返回错误：Package(null, exception)
 */
public class Package {
    /** 数据内容（正常情况下的数据） */
    byte[] data;
    /** 错误信息（异常情况下的错误） */
    Exception err;

    /**
     * 构造函数
     * 
     * @param data 数据内容，正常情况不为null
     * @param err 错误信息，异常情况不为null
     */
    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    /**
     * 获取数据内容
     * 
     * @return 数据内容的字节数组
     */
    public byte[] getData() {
        return data;
    }

    /**
     * 获取错误信息
     * 
     * @return 异常对象，如果没有错误则返回null
     */
    public Exception getErr() {
        return err;
    }
}
