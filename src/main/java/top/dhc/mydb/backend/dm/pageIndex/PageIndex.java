package top.dhc.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.dhc.mydb.backend.dm.pageCache.PageCache;
import top.dhc.mydb.backend.dm.pageIndex.PageInfo;

// 页面索引类，用于快速查找有足够空闲空间的页面
// 采用分桶策略：将页面按空闲空间大小分成40个区间管理
// 插入数据时可以快速找到合适的页面，避免遍历所有页面
public class PageIndex {

    // 将一页划分成40个区间，这是一个经验值，平衡了索引精度和管理开销
    private static final int INTERVALS_NO = 40;

    // 每个区间的阈值：8192字节 / 40 = 204字节
    // 区间0存储0-204字节空闲的页面，区间1存储205-409字节空闲的页面，以此类推
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    // 保证多线程环境下的线程安全
    private Lock lock;

    // 页面信息列表数组，共41个列表，对应区间0到区间40
    // lists[i]存储空闲空间在i*THRESHOLD到(i+1)*THRESHOLD之间的页面
    private List<PageInfo>[] lists;

    // 构造函数：初始化41个空闲列表和锁
    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    // 将页面添加到索引中
    // 根据页面的空闲空间大小计算区间编号，将其放入对应的区间列表
    // 例如：300字节空闲的页面放入区间1（300/204=1）
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;  // 计算区间编号
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    // 选择一个有足够空闲空间的页面
    // 从需要的空间大小对应的区间开始，向更大的区间查找
    // 找到第一个非空列表，取出第一个页面返回（同时从索引中移除）
    // 如果所有区间都没有合适的页面，返回null
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;  // 计算最小需要的区间
            if(number < INTERVALS_NO) number ++;  // 从下一个区间开始查找更安全

            // 从计算的区间开始向上查找
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {  // 当前区间为空，继续下一个
                    number ++;
                    continue;
                }
                return lists[number].remove(0);  // 找到非空区间，取出第一个页面
            }
            return null;  // 所有区间都没有合适的页面
        } finally {
            lock.unlock();
        }
    }

}