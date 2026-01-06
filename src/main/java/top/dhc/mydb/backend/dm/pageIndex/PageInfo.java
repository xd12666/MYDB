package top.dhc.mydb.backend.dm.pageIndex;

// 页面信息类，用于存储页面的基本信息
// 这是PageIndex索引中存储的数据结构，记录了页面号和该页面的空闲空间大小
// 作为一个简单的数据传输对象DTO，封装了页面的两个关键属性
public class PageInfo {

    // 页面号，标识是哪一个页面，从1开始编号
    public int pgno;

    // 该页面的空闲空间大小，单位是字节
    // 用于PageIndex快速判断该页面是否有足够的空间存储新数据
    public int freeSpace;

    // 构造函数，创建一个页面信息对象
    // pgno: 页面号
    // freeSpace: 页面的空闲空间大小（字节）
    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}