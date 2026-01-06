package top.dhc.mydb.backend;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import top.dhc.mydb.backend.dm.DataManager;
import top.dhc.mydb.backend.server.Server;
import top.dhc.mydb.backend.tbm.TableManager;
import top.dhc.mydb.backend.tm.TransactionManager;
import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.backend.vm.VersionManager;
import top.dhc.mydb.backend.vm.VersionManagerImpl;
import top.dhc.mydb.common.Error;

/**
 * 数据库后端启动器（Launcher）
 * 
 * 这是MYDB数据库系统的后端入口点，负责：
 * 1. 解析命令行参数（创建数据库或打开数据库）
 * 2. 初始化数据库系统的各个核心组件
 * 3. 启动数据库服务器，监听客户端连接
 * 
 * 数据库系统架构层次（自底向上）：
 * - TM (Transaction Manager): 事务管理器，管理事务的生命周期和状态
 * - DM (Data Manager): 数据管理器，管理数据的存储、缓存和恢复
 * - VM (Version Manager): 版本管理器，实现MVCC多版本并发控制
 * - TBM (Table Manager): 表管理器，管理表和字段的元数据
 * - Server: 服务器层，处理客户端连接和SQL执行
 * 
 * 使用示例：
 * - 创建数据库: java Launcher -create /tmp/mydb
 * - 打开数据库: java Launcher -open /tmp/mydb [-mem 64MB]
 */
public class Launcher {

    /** 数据库服务器监听的端口号，默认9999 */
    public static final int port = 9999;

    /** 默认内存大小：64MB = 64 * 1024 * 1024 字节 */
    public static final long DEFALUT_MEM = (1<<20)*64;
    /** 1KB = 1024 字节 */
    public static final long KB = 1 << 10;
    /** 1MB = 1024 * 1024 字节 */
	public static final long MB = 1 << 20;
    /** 1GB = 1024 * 1024 * 1024 字节 */
	public static final long GB = 1 << 30;

    /**
     * 主入口方法
     * 解析命令行参数并执行相应的操作（创建或打开数据库）
     * 
     * @param args 命令行参数
     * @throws ParseException 参数解析异常
     */
    public static void main(String[] args) throws ParseException {
        // 定义命令行选项
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");      // 打开数据库
        options.addOption("create", true, "-create DBPath");  // 创建数据库
        options.addOption("mem", true, "-mem 64MB");         // 指定内存大小
        
        // 解析命令行参数
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        // 根据选项执行相应操作
        if(cmd.hasOption("open")) {
            // 打开已存在的数据库并启动服务器
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            // 创建新的数据库（初始化文件结构）
            createDB(cmd.getOptionValue("create"));
            return;
        }
        // 如果没有提供有效选项，显示使用说明
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    /**
     * 创建新的数据库
     * 
     * 创建流程：
     * 1. 创建事务管理器（TM），初始化XID文件
     * 2. 创建数据管理器（DM），初始化数据库文件和日志
     * 3. 创建版本管理器（VM），实现MVCC功能
     * 4. 创建表管理器（TBM），初始化表管理结构
     * 5. 关闭所有组件，完成初始化
     * 
     * @param path 数据库文件路径（不含后缀）
     */
    private static void createDB(String path) {
        // 创建事务管理器，管理事务的创建、提交、回滚
        TransactionManager tm = TransactionManager.create(path);
        // 创建数据管理器，管理数据的存储和缓存（默认64MB内存）
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        // 创建版本管理器，实现MVCC多版本并发控制
        VersionManager vm = new VersionManagerImpl(tm, dm);
        // 创建表管理器，管理表和字段的元数据
        TableManager.create(path, vm, dm);
        // 关闭所有组件，完成初始化
        tm.close();
        dm.close();
    }

    /**
     * 打开已存在的数据库并启动服务器
     * 
     * 打开流程：
     * 1. 打开事务管理器，加载XID文件
     * 2. 打开数据管理器，加载数据库文件和日志（可能执行崩溃恢复）
     * 3. 创建版本管理器，实现MVCC功能
     * 4. 打开表管理器，加载表和字段的元数据
     * 5. 启动服务器，监听客户端连接
     * 
     * @param path 数据库文件路径（不含后缀）
     * @param mem 分配给页面缓存的内存大小（字节）
     */
    private static void openDB(String path, long mem) {
        // 打开事务管理器
        TransactionManager tm = TransactionManager.open(path);
        // 打开数据管理器（如果数据库异常关闭，会自动执行崩溃恢复）
        DataManager dm = DataManager.open(path, mem, tm);
        // 创建版本管理器
        VersionManager vm = new VersionManagerImpl(tm, dm);
        // 打开表管理器
        TableManager tbm = TableManager.open(path, vm, dm);
        // 启动服务器，开始监听客户端连接
        new Server(port, tbm).start();
    }

    /**
     * 解析内存大小字符串
     * 
     * 支持的格式：
     * - "64KB" -> 64 * 1024 字节
     * - "64MB" -> 64 * 1024 * 1024 字节
     * - "1GB" -> 1 * 1024 * 1024 * 1024 字节
     * 
     * @param memStr 内存大小字符串，如 "64MB"
     * @return 内存大小（字节）
     */
    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
