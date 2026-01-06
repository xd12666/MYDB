package top.dhc.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.dhc.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 *
 * 设计思路：
 * 1. 使用引用计数来管理缓存资源的生命周期，只有引用计数为0时才真正释放资源
 * 2. 通过 getting 映射表避免多个线程同时加载同一个资源，提高效率
 * 3. 支持最大缓存数限制，防止内存溢出
 * 4. 使用 ReentrantLock 保证线程安全
 * 5. 采用模板方法模式，由子类实现具体的资源加载和释放逻辑
 */
public abstract class AbstractCache<T> {

    // 实际缓存的数据，key为资源标识，value为缓存的资源对象
    private HashMap<Long, T> cache;

    // 元素的引用个数，记录每个资源被引用的次数
    // 当引用计数降为0时，资源才会被真正释放
    private HashMap<Long, Integer> references;

    // 正在获取某资源的线程标记
    // 用于避免多个线程同时从数据源加载同一个资源，造成资源浪费
    // 如果某个key存在于此map中，说明已有线程正在加载该资源，其他线程需要等待
    private HashMap<Long, Boolean> getting;

    // 缓存的最大资源数，用于限制缓存大小，防止内存溢出
    // 如果设置为0或负数，则表示不限制缓存大小
    private int maxResource;

    // 缓存中元素的个数，用于判断是否达到最大缓存限制
    private int count = 0;

    // 全局锁，用于保护 cache、references、getting、count 等共享数据的并发访问
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 获取指定key的资源
     *
     * 该方法实现了复杂的并发控制逻辑：
     * 1. 如果资源正在被其他线程加载，当前线程等待
     * 2. 如果资源已在缓存中，直接返回并增加引用计数
     * 3. 如果资源不在缓存中，检查缓存是否已满
     * 4. 如果缓存未满，标记正在获取，释放锁后调用getForCache加载资源
     * 5. 加载完成后，将资源放入缓存，设置引用计数为1
     *
     * @param key 资源的唯一标识
     * @return 资源对象
     * @throws Exception 如果缓存已满或加载资源失败
     */
    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();

            // 情况1：请求的资源正在被其他线程获取
            // 此时不应该重复获取，应该等待其他线程获取完成
            if(getting.containsKey(key)) {
                lock.unlock();
                try {
                    // 短暂休眠后重试，避免busy-waiting消耗CPU
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // 情况2：资源在缓存中，直接返回
            if(cache.containsKey(key)) {
                T obj = cache.get(key);
                // 增加引用计数，表示又有一个地方在使用这个资源
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 情况3：资源不在缓存中，需要从数据源加载
            // 首先检查缓存是否已满
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }

            // 缓存未满，准备加载资源
            count ++;  // 预先增加计数，占据一个缓存位置
            getting.put(key, true);  // 标记正在获取，防止其他线程重复加载
            lock.unlock();
            break;  // 跳出循环，开始加载资源
        }

        // 在锁外调用 getForCache，避免长时间持有锁
        // getForCache 可能涉及IO操作，耗时较长
        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            // 加载失败，需要清理之前的状态
            lock.lock();
            count --;  // 恢复计数
            getting.remove(key);  // 移除获取标记，允许其他线程重试
            lock.unlock();
            throw e;
        }

        // 加载成功，将资源放入缓存
        lock.lock();
        getting.remove(key);  // 移除获取标记
        cache.put(key, obj);  // 放入缓存
        references.put(key, 1);  // 设置初始引用计数为1
        lock.unlock();

        return obj;
    }

    /**
     * 释放一个缓存资源的引用
     *
     * 这个方法会将指定资源的引用计数减1
     * 如果引用计数降为0，说明没有任何地方在使用该资源了
     * 此时会调用 releaseForCache 将资源写回并从缓存中移除
     *
     * @param key 要释放的资源标识
     */
    protected void release(long key) {
        lock.lock();
        try {
            // 引用计数减1
            int ref = references.get(key)-1;

            if(ref == 0) {
                // 引用计数为0，资源不再被使用，可以释放
                T obj = cache.get(key);
                releaseForCache(obj);  // 调用子类实现的释放方法，可能涉及写回磁盘等操作
                references.remove(key);
                cache.remove(key);
                count --;  // 缓存计数减1
            } else {
                // 引用计数还不为0，只更新计数
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     *
     * 该方法通常在系统关闭时调用
     * 会将缓存中的所有资源都释放（写回），确保数据持久化
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);  // 释放每个缓存的资源
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     *
     * 这是一个模板方法，由子类实现具体的资源加载逻辑
     * 例如：从磁盘读取数据页、从数据库加载记录等
     *
     * @param key 资源的唯一标识
     * @return 加载的资源对象
     * @throws Exception 加载失败时抛出异常
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     *
     * 这是一个模板方法，由子类实现具体的资源释放逻辑
     * 例如：将脏页写回磁盘、关闭文件句柄等
     *
     * @param obj 要释放的资源对象
     */
    protected abstract void releaseForCache(T obj);
}