package top.dhc.mydb.common;

/**
 * 错误异常定义类
 * 
 * 职责：
 * 集中定义数据库系统中所有可能的错误异常
 * 使用单例模式，每个错误类型只有一个异常实例
 * 
 * 设计优势：
 * - 统一管理所有错误类型
 * - 便于错误处理和调试
 * - 使用异常对象比较（==）来判断错误类型，提高性能
 * 
 * 错误分类：
 * - common: 通用错误（缓存、文件操作等）
 * - dm: 数据管理器相关错误
 * - tm: 事务管理器相关错误
 * - vm: 版本管理器相关错误
 * - tbm: 表管理器相关错误
 * - parser: SQL解析器相关错误
 * - transport: 网络传输相关错误
 * - server: 服务器相关错误
 * - launcher: 启动器相关错误
 */
public class Error {
    // ========== 通用错误（common） ==========
    /** 缓存已满异常：当缓存达到最大容量时抛出 */
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    /** 文件已存在异常：创建文件时文件已存在 */
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    /** 文件不存在异常：打开文件时文件不存在 */
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    /** 文件无法读写异常：文件没有读写权限 */
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");

    // ========== 数据管理器错误（dm） ==========
    /** 日志文件损坏异常：日志文件格式不正确 */
    public static final Exception BadLogFileException = new RuntimeException("Bad log file!");
    /** 内存太小异常：分配的内存不足以运行数据库 */
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small!");
    /** 数据太大异常：要插入的数据超过页面大小限制 */
    public static final Exception DataTooLargeException = new RuntimeException("Data too large!");
    /** 数据库忙碌异常：数据库正在执行其他操作 */
    public static final Exception DatabaseBusyException = new RuntimeException("Database is busy!");

    // ========== 事务管理器错误（tm） ==========
    /** XID文件损坏异常：事务状态文件格式不正确 */
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    // ========== 版本管理器错误（vm） ==========
    /** 死锁异常：检测到死锁，事务被回滚 */
    public static final Exception DeadlockException = new RuntimeException("Deadlock!");
    /** 并发更新异常：多个事务同时修改同一数据 */
    public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");
    /** 空Entry异常：要访问的数据项不存在 */
    public static final Exception NullEntryException = new RuntimeException("Null entry!");

    // ========== 表管理器错误（tbm） ==========
    /** 无效字段类型异常：字段类型不支持 */
    public static final Exception InvalidFieldException = new RuntimeException("Invalid field type!");
    /** 字段未找到异常：查询的字段不存在 */
    public static final Exception FieldNotFoundException = new RuntimeException("Field not found!");
    /** 字段未索引异常：字段没有建立索引，无法使用索引查询 */
    public static final Exception FieldNotIndexedException = new RuntimeException("Field not indexed!");
    /** 无效逻辑操作异常：WHERE条件中的逻辑操作符无效 */
    public static final Exception InvalidLogOpException = new RuntimeException("Invalid logic operation!");
    /** 无效值异常：插入或更新的值不符合字段类型要求 */
    public static final Exception InvalidValuesException = new RuntimeException("Invalid values!");
    /** 重复表名异常：创建表时表名已存在 */
    public static final Exception DuplicatedTableException = new RuntimeException("Duplicated table!");
    /** 表未找到异常：查询的表不存在 */
    public static final Exception TableNotFoundException = new RuntimeException("Table not found!");

    // ========== SQL解析器错误（parser） ==========
    /** 无效命令异常：SQL语句格式不正确或不被支持 */
    public static final Exception InvalidCommandException = new RuntimeException("Invalid command!");
    /** 表无索引异常：表没有索引，无法执行索引相关操作 */
    public static final Exception TableNoIndexException = new RuntimeException("Table has no index!");

    // ========== 网络传输错误（transport） ==========
    /** 无效数据包异常：接收到的数据包格式不正确 */
    public static final Exception InvalidPkgDataException = new RuntimeException("Invalid package data!");

    // ========== 服务器错误（server） ==========
    /** 嵌套事务异常：不支持嵌套事务（一个事务内不能开始另一个事务） */
    public static final Exception NestedTransactionException = new RuntimeException("Nested transaction not supported!");
    /** 无事务异常：执行SQL语句时没有活跃的事务 */
    public static final Exception NoTransactionException = new RuntimeException("Not in transaction!");

    // ========== 启动器错误（launcher） ==========
    /** 无效内存异常：内存大小格式不正确（如"64MB"格式错误） */
    public static final Exception InvalidMemException = new RuntimeException("Invalid memory!");
}
