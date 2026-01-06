package top.dhc.mydb.client;

import java.util.Scanner;

/**
 * 交互式命令行Shell
 * 
 * 职责：
 * 1. 提供交互式命令行界面
 * 2. 接收用户输入的SQL语句
 * 3. 将SQL语句发送到服务器执行
 * 4. 显示执行结果或错误信息
 * 
 * 用户交互流程：
 * 1. 显示提示符 ":> "
 * 2. 等待用户输入SQL语句
 * 3. 如果输入"exit"或"quit"，退出程序
 * 4. 否则，将SQL语句发送到服务器执行
 * 5. 显示执行结果或错误信息
 * 6. 重复步骤1-5，直到用户退出
 * 
 * 示例SQL语句：
 * - begin
 * - create table student id int32, name string, (index id)
 * - insert into student values 1 "Alice"
 * - select * from student
 * - commit
 */
public class Shell {
    /** 客户端对象，用于执行SQL语句 */
    private Client client;

    /**
     * 构造函数
     * 
     * @param client 客户端对象
     */
    public Shell(Client client) {
        this.client = client;
    }

    /**
     * 启动交互式Shell
     * 
     * 执行流程：
     * 1. 创建Scanner读取用户输入
     * 2. 进入循环，持续接收用户输入
     * 3. 如果用户输入"exit"或"quit"，退出循环
     * 4. 否则，将SQL语句发送到服务器执行
     * 5. 显示执行结果或错误信息
     * 6. 循环继续，直到用户退出
     * 7. 关闭Scanner和客户端连接
     */
    public void run() {
        // 创建Scanner读取标准输入
        Scanner sc = new Scanner(System.in);
        try {
            // 进入交互循环
            while(true) {
                // 显示提示符
                System.out.print(":> ");
                // 读取用户输入的一行SQL语句
                String statStr = sc.nextLine();
                // 检查是否是退出命令
                if("exit".equals(statStr) || "quit".equals(statStr)) {
                    // 退出循环
                    break;
                }
                try {
                    // 将SQL语句转换为字节数组并执行
                    byte[] res = client.execute(statStr.getBytes());
                    // 显示执行结果
                    System.out.println(new String(res));
                } catch(Exception e) {
                    // 如果执行出错，显示错误信息
                    System.out.println(e.getMessage());
                }

            }
        } finally {
            // 确保资源被释放
            sc.close();      // 关闭Scanner
            client.close();   // 关闭客户端连接
        }
    }
}
