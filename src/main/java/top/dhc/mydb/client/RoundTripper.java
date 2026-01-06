package top.dhc.mydb.client;

import top.dhc.mydb.transport.Package;
import top.dhc.mydb.transport.Packager;

/**
 * 往返通信器（Round Tripper）
 * 
 * 职责：
 * 实现请求-响应模式的网络通信
 * 封装发送请求和接收响应的逻辑
 * 
 * 设计模式：
 * - 封装了Packager的send和receive操作
 * - 提供简单的roundTrip接口，一次调用完成请求-响应
 * 
 * 使用场景：
 * 客户端需要发送SQL语句并等待服务器响应时使用
 */
public class RoundTripper {
    /** 数据包打包器，负责数据的编码和传输 */
    private Packager packager;

    /**
     * 构造函数
     * 
     * @param packager 数据包打包器
     */
    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 执行往返通信（发送请求并接收响应）
     * 
     * 执行流程：
     * 1. 发送请求包到服务器
     * 2. 等待并接收服务器的响应包
     * 3. 返回响应包
     * 
     * @param pkg 要发送的请求包
     * @return 服务器返回的响应包
     * @throws Exception 网络通信异常
     */
    public Package roundTrip(Package pkg) throws Exception {
        // 发送请求包
        packager.send(pkg);
        // 接收并返回响应包
        return packager.receive();
    }

    /**
     * 关闭连接
     * 
     * @throws Exception 关闭过程中的异常
     */
    public void close() throws Exception {
        packager.close();
    }
}
