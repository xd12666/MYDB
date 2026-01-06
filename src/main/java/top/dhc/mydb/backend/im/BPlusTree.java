package top.dhc.mydb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.dhc.mydb.backend.common.SubArray;
import top.dhc.mydb.backend.dm.DataManager;
import top.dhc.mydb.backend.dm.dataItem.DataItem;
import top.dhc.mydb.backend.im.Node.InsertAndSplitRes;
import top.dhc.mydb.backend.im.Node.LeafSearchRangeRes;
import top.dhc.mydb.backend.im.Node.SearchNextRes;
import top.dhc.mydb.backend.tm.TransactionManagerImpl;
import top.dhc.mydb.backend.utils.Parser;

// B+树索引实现类
// B+树是数据库中常用的索引结构，具有以下特点：
// 1. 所有数据存储在叶子节点，内部节点只存储键值和指针
// 2. 叶子节点之间有链接，支持高效的范围查询
// 3. 树的高度较低，减少磁盘I/O次数
// 4. 支持插入时的自动分裂，保持树的平衡
//
// 本实现中：
// - 使用DataManager存储节点数据
// - 使用UID作为节点指针
// - 使用SUPER_XID（超级事务）来修改树结构
public class BPlusTree {

    // 数据管理器，用于读写节点数据
    DataManager dm;

    // 启动节点的UID，这个节点存储根节点的UID
    // 为什么需要这一层间接？因为根节点可能会变化（分裂时），需要一个固定的位置存储当前根节点的UID
    long bootUid;

    // 启动节点的DataItem对象，用于读写根节点的UID
    DataItem bootDataItem;

    // 启动节点的锁，保护根节点UID的并发访问
    Lock bootLock;

    // 创建一个新的B+树
    // 步骤：
    // 1. 创建一个空的根节点（NilRoot）
    // 2. 将根节点写入数据库，获得rootUid
    // 3. 创建启动节点，存储rootUid
    // 4. 返回启动节点的UID（bootUid）
    // 返回值bootUid就是这棵B+树的标识符
    public static long create(DataManager dm) throws Exception {
        // 创建一个空的根节点
        byte[] rawRoot = Node.newNilRootRaw();
        // 使用超级事务插入根节点
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        // 创建启动节点，存储根节点的UID
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    // 加载一个已存在的B+树
    // 根据bootUid加载启动节点，初始化B+树对象
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        // 读取启动节点
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;

        // 创建B+树对象
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    // 获取当前根节点的UID
    // 从启动节点中读取根节点的UID
    // 需要加锁，因为根节点UID可能被并发更新
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            // 启动节点只存储一个long值（8字节），就是根节点的UID
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    // 更新根节点UID
    // 当根节点分裂时调用，需要创建新的根节点
    // left: 原根节点的UID
    // right: 分裂出的新节点的UID
    // rightKey: 新节点的最小键值
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            // 创建新的根节点，包含两个子节点
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);

            // 更新启动节点，将其指向新的根节点
            bootDataItem.before();  // 记录修改前的数据，用于回滚
            SubArray diRaw = bootDataItem.data();
            // 将新根节点的UID写入启动节点
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);  // 记录日志
        } finally {
            bootLock.unlock();
        }
    }

    // 查找包含指定键的叶子节点
    // 从给定节点开始，递归向下查找，直到找到叶子节点
    // nodeUid: 开始查找的节点UID
    // key: 要查找的键值
    // 返回包含该键的叶子节点的UID
    private long searchLeaf(long nodeUid, long key) throws Exception {
        // 加载节点
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            // 已经是叶子节点，直接返回
            return nodeUid;
        } else {
            // 内部节点，查找下一层的子节点
            long next = searchNext(nodeUid, key);
            // 递归查找
            return searchLeaf(next, key);
        }
    }

    // 在内部节点中查找下一个应该访问的子节点
    // nodeUid: 内部节点的UID
    // key: 要查找的键值
    // 返回下一层应该访问的子节点UID
    //
    // 为什么需要while循环？因为可能遇到节点正在分裂的情况
    // 此时需要通过siblingUid找到兄弟节点继续查找
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();

            if(res.uid != 0) return res.uid;  // 找到了下一个节点
            nodeUid = res.siblingUid;  // 需要到兄弟节点查找
        }
    }

    // 查找指定键对应的所有UID
    // 实际上调用searchRange(key, key)，查找单个键值
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    // 范围查询：查找[leftKey, rightKey]区间内所有键对应的UID
    // B+树的优势：叶子节点有链接，可以高效地进行范围查询
    // 步骤：
    // 1. 找到包含leftKey的叶子节点
    // 2. 从该叶子节点开始，沿着叶子节点链表向右遍历
    // 3. 收集所有在范围内的UID
    // 4. 直到遇到叶子节点的siblingUid为0（链表结束）
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        // 找到包含leftKey的叶子节点
        long leafUid = searchLeaf(rootUid, leftKey);

        List<Long> uids = new ArrayList<>();
        // 沿着叶子节点链表遍历
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            // 在当前叶子节点中查找范围内的UID
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);

            if(res.siblingUid == 0) {
                // 没有下一个叶子节点，查找结束
                break;
            } else {
                // 继续查找下一个叶子节点
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    // 插入键值对到B+树
    // key: 索引键
    // uid: 数据项的UID
    // 步骤：
    // 1. 从根节点开始递归插入
    // 2. 如果插入导致节点分裂，返回新节点信息
    // 3. 如果根节点分裂，需要创建新的根节点
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;

        // 如果根节点分裂，创建新的根节点
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    // 插入结果类
    // 记录插入操作是否导致节点分裂
    class InsertRes {
        long newNode;  // 分裂出的新节点UID，0表示没有分裂
        long newKey;   // 新节点的最小键值
    }

    // 递归插入操作
    // nodeUid: 当前节点的UID
    // uid: 要插入的数据项UID
    // key: 要插入的键值
    // 返回插入结果，包含是否分裂的信息
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            // 叶子节点，直接插入
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            // 内部节点，先找到应该插入的子节点
            long next = searchNext(nodeUid, key);
            // 递归插入到子节点
            InsertRes ir = insert(next, uid, key);

            if(ir.newNode != 0) {
                // 子节点分裂了，需要在当前节点插入新的子节点指针
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                // 子节点没有分裂，插入成功
                res = new InsertRes();
            }
        }
        return res;
    }

    // 插入并处理分裂
    // 尝试在节点中插入键值对，如果节点满了则分裂
    //
    // 为什么需要while循环？
    // 因为在并发环境下，节点可能正在分裂
    // 此时需要通过siblingUid找到正确的节点继续插入
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();

            if(iasr.siblingUid != 0) {
                // 需要到兄弟节点插入
                nodeUid = iasr.siblingUid;
            } else {
                // 插入完成
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;  // 可能分裂出的新节点
                res.newKey = iasr.newKey;   // 新节点的最小键值
                return res;
            }
        }
    }

    // 关闭B+树，释放启动节点的引用
    public void close() {
        bootDataItem.release();
    }
}