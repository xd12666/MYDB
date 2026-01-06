package top.dhc.mydb.client;

import top.dhc.mydb.transport.Package;
import top.dhc.mydb.transport.Packager;

/**
 * 数据库客户端核心类
 * 
 * 职责：
 * 1. 封装与服务器的通信逻辑
 * 2. 发送SQL语句到服务器
 * 3. 接收并处理服务器返回的结果
 * 4. 处理执行过程中的异常
 * 
 * 设计模式：
 * - 使用RoundTripper实现请求-响应模式
 * - 将网络通信细节封装在Packager中
 * 
 * 工作流程：
 * 1. 客户端调用execute()方法，传入SQL语句（字节数组）
 * 2. 将SQL语句打包成Package对象
 * 3. 通过RoundTripper发送到服务器并等待响应
 * 4. 如果服务器返回错误，抛出异常
 * 5. 如果执行成功，返回结果数据（字节数组）
 */
public class Client {
    /** 往返通信器，负责发送请求并接收响应 */
    private RoundTripper rt;

    /**
     * 构造函数
     * 
     * @param packager 数据包打包器，负责数据的编码和传输
     */
    public Client(Packager packager) {
        // 创建往返通信器，封装打包器
        this.rt = new RoundTripper(packager);
    }

    /**
     * 执行SQL语句
     * 
     * 执行流程：
     * 1. 将SQL语句（字节数组）打包成Package对象
     * 2. 通过RoundTripper发送到服务器并等待响应
     * 3. 检查响应中是否包含错误
     * 4. 如果有错误，抛出异常
     * 5. 如果成功，返回结果数据
     * 
     * @param stat SQL语句的字节数组表示
     * @return 执行结果的字节数组
     * @throws Exception 如果服务器返回错误或网络异常
     */
    public byte[] execute(byte[] stat) throws Exception {
        // 创建请求包，包含SQL语句，无错误
        Package pkg = new Package(stat, null);
        // 发送请求并接收响应（往返通信）
        Package resPkg = rt.roundTrip(pkg);
        // 检查响应中是否包含错误
        if (resPkg.getErr() != null) {
            // 如果有错误，抛出异常
            throw resPkg.getErr();
        }
        // 返回执行结果
        return resPkg.getData();
    }

    /**
     * 关闭客户端连接
     * 
     * 释放资源：
     * - 关闭RoundTripper，进而关闭Packager
     * - 关闭底层的Socket连接
     */
    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
            // 忽略关闭时的异常
        }
    }

}
