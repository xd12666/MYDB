package top.dhc.mydb.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * 数据传输器（Transporter）
 * 
 * 职责：
 * 负责底层字节流的传输
 * 使用十六进制编码将字节数组转换为字符串进行传输
 * 
 * 设计原因：
 * - 使用十六进制编码可以避免二进制数据在文本传输中的问题
 * - 每行一个数据包，使用换行符分隔
 * - 简化了数据包的边界识别
 * 
 * 传输格式：
 * - 发送：将字节数组转换为十六进制字符串，末尾加换行符
 * - 接收：读取一行十六进制字符串，转换为字节数组
 * 
 * 示例：
 * - 字节数组 [0x01, 0x02, 0xFF] 编码为 "0102ff\n"
 */
public class Transporter {
    /** Socket连接 */
    private Socket socket;
    /** 输入流读取器，用于接收数据 */
    private BufferedReader reader;
    /** 输出流写入器，用于发送数据 */
    private BufferedWriter writer;

    /**
     * 构造函数
     * 
     * 初始化输入输出流：
     * - 从Socket获取输入流，创建BufferedReader
     * - 从Socket获取输出流，创建BufferedWriter
     * 
     * @param socket Socket连接
     * @throws IOException 流初始化异常
     */
    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        // 创建输入流读取器
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        // 创建输出流写入器
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 发送数据
     * 
     * 执行流程：
     * 1. 将字节数组编码为十六进制字符串
     * 2. 添加换行符
     * 3. 写入输出流
     * 4. 刷新缓冲区，确保数据立即发送
     * 
     * @param data 要发送的字节数组
     * @throws Exception 发送过程中的异常
     */
    public void send(byte[] data) throws Exception {
        // 将字节数组编码为十六进制字符串
        String raw = hexEncode(data);
        // 写入输出流
        writer.write(raw);
        // 刷新缓冲区，确保数据立即发送
        writer.flush();
    }

    /**
     * 接收数据
     * 
     * 执行流程：
     * 1. 从输入流读取一行（以换行符结束）
     * 2. 如果读取到null，说明连接已关闭，关闭连接
     * 3. 将十六进制字符串解码为字节数组
     * 4. 返回字节数组
     * 
     * @return 接收到的字节数组
     * @throws Exception 接收过程中的异常
     */
    public byte[] receive() throws Exception {
        // 读取一行数据
        String line = reader.readLine();
        // 如果读取到null，说明连接已关闭
        if(line == null) {
            close();
        }
        // 将十六进制字符串解码为字节数组
        return hexDecode(line);
    }

    /**
     * 关闭连接
     * 
     * 关闭所有资源：
     * - 关闭输出流
     * - 关闭输入流
     * - 关闭Socket连接
     * 
     * @throws IOException 关闭过程中的异常
     */
    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    /**
     * 将字节数组编码为十六进制字符串
     * 
     * @param buf 要编码的字节数组
     * @return 十六进制字符串（小写）+ 换行符
     */
    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true)+"\n";
    }

    /**
     * 将十六进制字符串解码为字节数组
     * 
     * @param buf 要解码的十六进制字符串
     * @return 解码后的字节数组
     * @throws DecoderException 解码异常（格式错误）
     */
    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
