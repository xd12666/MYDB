package top.dhc.mydb.transport;

/**
 * 数据包打包器（Packager）
 * 
 * 职责：
 * 封装Transporter和Encoder，提供高级数据包接口
 * 将Package对象的发送和接收与底层传输细节分离
 * 
 * 设计模式：
 * - 组合模式：组合Transporter和Encoder
 * - 外观模式：为上层提供简化的接口
 * 
 * 工作流程：
 * - 发送：Package -> Encoder编码 -> Transporter发送
 * - 接收：Transporter接收 -> Encoder解码 -> Package
 */
public class Packager {
    /** 数据传输器，负责底层字节流传输 */
    private Transporter transpoter;
    /** 数据包编码器，负责数据包的编码和解码 */
    private Encoder encoder;

    /**
     * 构造函数
     * 
     * @param transpoter 数据传输器
     * @param encoder 数据包编码器
     */
    public Packager(Transporter transpoter, Encoder encoder) {
        this.transpoter = transpoter;
        this.encoder = encoder;
    }

    /**
     * 发送数据包
     * 
     * 执行流程：
     * 1. 使用Encoder将Package编码为字节数组
     * 2. 使用Transporter发送字节数组
     * 
     * @param pkg 要发送的数据包
     * @throws Exception 发送过程中的异常
     */
    public void send(Package pkg) throws Exception {
        // 编码数据包
        byte[] data = encoder.encode(pkg);
        // 发送数据
        transpoter.send(data);
    }

    /**
     * 接收数据包
     * 
     * 执行流程：
     * 1. 使用Transporter接收字节数组
     * 2. 使用Encoder将字节数组解码为Package
     * 3. 返回Package对象
     * 
     * @return 接收到的数据包
     * @throws Exception 接收过程中的异常
     */
    public Package receive() throws Exception {
        // 接收数据
        byte[] data = transpoter.receive();
        // 解码数据包
        return encoder.decode(data);
    }

    /**
     * 关闭连接
     * 
     * @throws Exception 关闭过程中的异常
     */
    public void close() throws Exception {
        transpoter.close();
    }
}
