package top.dhc.mydb.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import top.dhc.mydb.transport.Encoder;
import top.dhc.mydb.transport.Packager;
import top.dhc.mydb.transport.Transporter;

/**
 * 数据库客户端启动器（Launcher）
 * 
 * 这是MYDB数据库系统的客户端入口点，负责：
 * 1. 连接到数据库服务器（默认127.0.0.1:9999）
 * 2. 初始化客户端通信组件（编码器、传输器、打包器）
 * 3. 启动交互式Shell，接收用户输入的SQL语句并执行
 * 
 * 客户端架构：
 * - Socket: 与服务器建立TCP连接
 * - Transporter: 负责底层字节流的传输（使用十六进制编码）
 * - Encoder: 负责数据包的编码和解码（处理错误信息）
 * - Packager: 封装Transporter和Encoder，提供高级数据包接口
 * - Client: 客户端核心类，执行SQL语句并返回结果
 * - Shell: 交互式命令行界面，提供用户交互
 * 
 * 使用方式：
 * 1. 确保数据库服务器已启动（运行backend.Launcher）
 * 2. 运行此客户端程序
 * 3. 在交互式命令行中输入SQL语句
 * 4. 输入"exit"或"quit"退出
 */
public class Launcher {
    /**
     * 客户端主入口方法
     * 
     * 执行流程：
     * 1. 连接到数据库服务器（127.0.0.1:9999）
     * 2. 创建编码器、传输器、打包器
     * 3. 创建客户端和Shell
     * 4. 启动交互式Shell
     * 
     * @param args 命令行参数（当前未使用）
     * @throws UnknownHostException 无法解析服务器地址
     * @throws IOException 网络连接异常
     */
    public static void main(String[] args) throws UnknownHostException, IOException {
        // 连接到数据库服务器（默认本地9999端口）
        Socket socket = new Socket("127.0.0.1", 9999);
        
        // 创建编码器，负责数据包的编码和解码
        Encoder e = new Encoder();
        // 创建传输器，负责底层字节流的传输
        Transporter t = new Transporter(socket);
        // 创建打包器，封装传输器和编码器
        Packager packager = new Packager(t, e);

        // 创建客户端，用于执行SQL语句
        Client client = new Client(packager);
        // 创建Shell，提供交互式命令行界面
        Shell shell = new Shell(client);
        // 启动Shell，开始接收用户输入
        shell.run();
    }
}
