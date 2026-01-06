package top.dhc.mydb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import top.dhc.mydb.backend.common.SubArray;
import top.dhc.mydb.backend.dm.dataItem.DataItem;
import top.dhc.mydb.backend.tm.TransactionManagerImpl;
import top.dhc.mydb.backend.utils.Parser;

// B+树节点类
//
// 节点的二进制结构：
// [LeafFlag(1字节)][KeyNumber(2字节)][SiblingUid(8字节)][Son0(8字节)][Key0(8字节)][Son1(8字节)][Key1(8字节)]...[SonN(8字节)][KeyN(8字节)]
//
// 字段说明：
// - LeafFlag: 标识是否为叶子节点，1表示叶子，0表示内部节点
// - KeyNumber: 当前节点包含的键值对数量
// - SiblingUid: 右兄弟节点的UID，用于叶子节点的链表连接，0表示没有右兄弟
// - Son/Key对: 子节点UID和对应的键值，交替存储
//
// 节点类型：
// 1. 叶子节点：Son存储数据项的UID，Key存储索引键
// 2. 内部节点：Son存储子节点的UID，Key存储子节点的最小键值
public class Node {

    // LeafFlag字段的偏移位置
    static final int IS_LEAF_OFFSET = 0;

    // KeyNumber字段的偏移位置
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;

    // SiblingUid字段的偏移位置
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;

    // 节点头部大小：1字节LeafFlag + 2字节KeyNumber + 8字节SiblingUid = 11字节
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    // 平衡数：节点最多包含64个键值对（32*2）
    // 当键值对数量达到64时触发分裂，分成两个各32个键值对的节点
    static final int BALANCE_NUMBER = 32;

    // 节点总大小：头部11字节 + (8字节Son + 8字节Key) * (32*2+2) = 11 + 16*66 = 1067字节
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    // 所属的B+树
    BPlusTree tree;

    // 节点的DataItem对象，用于访问节点数据
    DataItem dataItem;

    // 节点的原始字节数组，指向DataItem的数据
    SubArray raw;

    // 节点的UID
    long uid;

    // 设置节点的LeafFlag字段
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    // 获取节点是否为叶子节点
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    // 设置节点的键值对数量
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    // 获取节点的键值对数量
    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    // 设置节点的右兄弟UID
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    // 获取节点的右兄弟UID
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    // 设置第kth个Son（子节点或数据项的UID）
    // kth从0开始
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    // 获取第kth个Son
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    // 设置第kth个Key（索引键值）
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    // 获取第kth个Key
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    // 从源节点的第kth个位置开始，复制数据到目标节点
    // 用于节点分裂时复制后半部分数据
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    // 将从第kth+1个位置开始的所有数据向右移动一个位置
    // 用于在第kth位置插入新数据前腾出空间
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    // 创建一个新的根节点
    // 根节点包含两个子节点：left和right
    // key是right子节点的最小键值
    // 这个方法在根节点分裂时调用
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);  // 根节点是内部节点
        setRawNoKeys(raw, 2);      // 包含2个键值对
        setRawSibling(raw, 0);     // 根节点没有兄弟

        // 第0个键值对：Son0指向left，Key0是right的最小键
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);

        // 第1个键值对：Son1指向right，Key1是无穷大（表示右边界）
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    // 创建一个空的根节点
    // 用于创建新B+树时的初始根节点
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);   // 空树的根节点是叶子节点
        setRawNoKeys(raw, 0);      // 没有键值对
        setRawSibling(raw, 0);     // 没有兄弟节点

        return raw.raw;
    }

    // 加载一个节点
    // 从DataManager读取节点数据，创建Node对象
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    // 释放节点，减少DataItem的引用计数
    public void release() {
        dataItem.release();
    }

    // 判断是否为叶子节点
    // 需要加读锁保证并发安全
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    // 搜索下一个节点的结果类
    // 用于内部节点查找应该访问的子节点
    class SearchNextRes {
        long uid;         // 下一个应该访问的子节点UID
        long siblingUid;  // 如果在当前节点找不到，需要访问的兄弟节点UID
    }

    // 在内部节点中搜索下一个应该访问的子节点
    // 遍历节点的所有Key，找到第一个大于目标key的位置
    // 返回该位置对应的Son作为下一个节点
    // 如果所有Key都小于目标key，说明应该到兄弟节点查找
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);

            // 遍历所有键值对
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);
                if(key < ik) {
                    // 找到第一个大于key的位置
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }

            // 所有Key都小于目标key，需要到兄弟节点查找
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    // 叶子节点范围查询的结果类
    class LeafSearchRangeRes {
        List<Long> uids;  // 范围内的所有UID
        long siblingUid;  // 如果范围跨节点，需要继续查找的兄弟节点UID
    }

    // 在叶子节点中进行范围查询
    // 查找[leftKey, rightKey]范围内的所有UID
    // 步骤：
    // 1. 找到第一个大于等于leftKey的位置
    // 2. 从该位置开始收集所有小于等于rightKey的UID
    // 3. 如果到达节点末尾还没结束，返回兄弟节点UID继续查找
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;

            // 找到第一个大于等于leftKey的位置
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }

            // 收集范围内的所有UID
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }

            // 如果遍历到节点末尾，可能需要继续查找兄弟节点
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }

            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    // 插入并分裂的结果类
    class InsertAndSplitRes {
        long siblingUid;  // 如果插入失败，需要到兄弟节点插入的UID
        long newSon;      // 如果发生分裂，新节点的UID
        long newKey;      // 如果发生分裂，新节点的最小键值
    }

    // 插入键值对，如果节点满了则分裂
    // 步骤：
    // 1. 记录修改前的数据（用于回滚）
    // 2. 尝试插入键值对
    // 3. 如果插入失败（键值应该在兄弟节点），返回兄弟节点UID
    // 4. 如果插入成功但节点满了，执行分裂
    // 5. 记录日志
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();  // 记录修改前的数据
        try {
            success = insert(uid, key);
            if(!success) {
                // 插入失败，需要到兄弟节点插入
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                // 节点满了，需要分裂
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                // 插入成功，记录日志
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                // 插入失败或发生异常，回滚
                dataItem.unBefore();
            }
        }
    }

    // 在节点中插入键值对
    // 找到合适的位置插入，保持键值有序
    // 返回是否插入成功，失败说明应该插入到兄弟节点
    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;

        // 找到第一个大于等于key的位置
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }

        // 如果到达节点末尾且存在兄弟节点，说明应该插入到兄弟节点
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        if(getRawIfLeaf(raw)) {
            // 叶子节点：直接插入
            shiftRawKth(raw, kth);         // 腾出空间
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        } else {
            // 内部节点：插入后需要调整
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }

    // 判断节点是否需要分裂
    // 当键值对数量达到BALANCE_NUMBER*2（64个）时分裂
    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    // 分裂结果类
    class SplitRes {
        long newSon;  // 新节点的UID
        long newKey;  // 新节点的最小键值
    }

    // 执行节点分裂
    // 将当前节点分成两个节点，各包含BALANCE_NUMBER（32个）键值对
    // 步骤：
    // 1. 创建新节点，复制后半部分数据
    // 2. 将新节点插入数据库
    // 3. 更新当前节点：设置键值对数量为32，兄弟指向新节点
    // 4. 返回新节点的UID和最小键值
    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));

        // 复制后半部分数据到新节点
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);

        // 插入新节点到数据库
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);

        // 更新当前节点
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);  // 新节点的最小键值
        return res;
    }

    // 将节点转换为字符串，用于调试
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}