package top.dhc.mydb.backend.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import top.dhc.mydb.backend.tbm.TableManager;
import top.dhc.mydb.transport.Encoder;
import top.dhc.mydb.transport.Package;
import top.dhc.mydb.transport.Packager;
import top.dhc.mydb.transport.Transporter;

public class Server {
    private int port;  // 服务器监听的端口号。
    TableManager tbm;  // 表管理器，用于管理数据库中的表。

    // 构造函数，初始化服务器端口和表管理器。
    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    // start() 方法：启动服务器，监听指定端口并处理客户端连接请求。
    public void start() {
        ServerSocket ss = null;  // 服务器套接字，用于监听客户端的连接请求。
        try {
            ss = new ServerSocket(port);  // 在指定端口启动服务器。
        } catch (IOException e) {
            e.printStackTrace();  // 如果服务器启动失败，打印错误信息。
            return;
        }
        System.out.println("Server listen to port: " + port);  // 打印服务器监听的端口号。

        // 创建一个线程池，最大线程数为 20，最小线程数为 10，使用阻塞队列来管理任务。
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while(true) {
                Socket socket = ss.accept();  // 接受来自客户端的连接请求。
                Runnable worker = new HandleSocket(socket, tbm);  // 创建一个处理连接的任务。
                tpe.execute(worker);  // 将任务提交给线程池执行。
            }
        } catch(IOException e) {
            e.printStackTrace();  // 捕获并打印连接异常。
        } finally {
            try {
                ss.close();  // 关闭服务器套接字。
            } catch (IOException ignored) {}
        }
    }
}

// 处理客户端连接的任务类
class HandleSocket implements Runnable {
    private Socket socket;  // 客户端套接字，用于与客户端通信。
    private TableManager tbm;  // 表管理器，用于数据库操作。

    // 构造函数，初始化客户端套接字和表管理器。
    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    // run() 方法：处理与客户端的通信逻辑。
    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();  // 获取客户端的 IP 地址和端口号。
        System.out.println("Establish connection: " + address.getAddress().getHostAddress() + ":" + address.getPort());  // 打印客户端连接信息。

        Packager packager = null;  // 包装器，用于打包和解包数据包。
        try {
            // 初始化数据传输器和编码器。
            Transporter t = new Transporter(socket);  // 创建数据传输器。
            Encoder e = new Encoder();  // 创建编码器。
            packager = new Packager(t, e);  // 创建数据包包装器。
        } catch(IOException e) {
            e.printStackTrace();  // 捕获初始化错误并打印错误信息。
            try {
                socket.close();  // 关闭客户端套接字。
            } catch (IOException e1) {
                e1.printStackTrace();  // 打印关闭套接字时的错误信息。
            }
            return;  // 返回，结束当前任务。
        }

        // 创建 SQL 执行器，传入表管理器。
        Executor exe = new Executor(tbm);
        while(true) {
            Package pkg = null;
            try {
                pkg = packager.receive();  // 接收客户端发送的数据包。
            } catch(Exception e) {
                break;  // 如果接收数据包时发生异常，跳出循环。
            }
            byte[] sql = pkg.getData();  // 获取 SQL 语句。
            byte[] result = null;
            Exception e = null;
            try {
                result = exe.execute(sql);  // 执行 SQL 语句并获取结果。
            } catch (Exception e1) {
                e = e1;  // 如果执行 SQL 语句时发生异常，捕获异常。
                e.printStackTrace();  // 打印异常信息。
            }
            pkg = new Package(result, e);  // 创建一个新的数据包，包含执行结果和可能的异常。
            try {
                packager.send(pkg);  // 发送执行结果给客户端。
            } catch (Exception e1) {
                e1.printStackTrace();  // 捕获并打印发送错误。
                break;  // 发送错误时跳出循环。
            }
        }

        exe.close();  // 关闭执行器，结束事务。
        try {
            packager.close();  // 关闭数据包包装器。
        } catch (Exception e) {
            e.printStackTrace();  // 打印关闭包装器时的错误信息。
        }
    }
}
