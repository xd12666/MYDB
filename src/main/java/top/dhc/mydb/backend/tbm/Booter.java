package top.dhc.mydb.backend.tbm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import top.dhc.mydb.backend.utils.Panic;
import top.dhc.mydb.common.Error;

// 记录第一个表的uid
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";  // 表示数据库引导文件的后缀。
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";  // 临时引导文件后缀，用于更新引导文件。

    String path;  // 存储引导文件的路径。
    File file;  // 引导文件对象。

    // 创建一个新的 Booter 文件，如果文件已经存在则会抛出错误。
    public static Booter create(String path) {
        removeBadTmp(path);  // 删除任何不完整的临时引导文件。
        File f = new File(path + BOOTER_SUFFIX);  // 根据路径创建引导文件。
        try {
            if (!f.createNewFile()) {  // 尝试创建新文件，如果文件已经存在，触发 panic。
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);  // 捕获异常并触发 panic。
        }
        if (!f.canRead() || !f.canWrite()) {  // 如果文件不可读写，触发 panic。
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);  // 返回 Booter 对象。
    }

    // 打开已存在的引导文件。
    public static Booter open(String path) {
        removeBadTmp(path);  // 删除任何不完整的临时引导文件。
        File f = new File(path + BOOTER_SUFFIX);  // 根据路径打开引导文件。
        if (!f.exists()) {  // 如果文件不存在，触发 panic。
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {  // 如果文件不可读写，触发 panic。
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);  // 返回 Booter 对象。
    }

    // 删除任何损坏的临时引导文件。
    private static void removeBadTmp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();  // 删除临时文件。
    }

    // 构造函数，用于创建 Booter 对象并初始化文件路径。
    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    // 加载引导文件的数据。
    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());  // 读取引导文件的内容到字节数组。
        } catch (IOException e) {
            Panic.panic(e);  // 捕获异常并触发 panic。
        }
        return buf;  // 返回文件内容。
    }

    // 更新引导文件的内容。
    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);  // 创建临时引导文件。
        try {
            tmp.createNewFile();  // 创建新的临时文件。
        } catch (Exception e) {
            Panic.panic(e);  // 捕获异常并触发 panic。
        }
        if (!tmp.canRead() || !tmp.canWrite()) {  // 如果临时文件不可读写，触发 panic。
            Panic.panic(Error.FileCannotRWException);
        }
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);  // 将数据写入临时文件。
            out.flush();  // 刷新文件输出流。
        } catch (IOException e) {
            Panic.panic(e);  // 捕获异常并触发 panic。
        }
        try {
            Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);  // 将临时文件替换为正式引导文件。
        } catch (IOException e) {
            Panic.panic(e);  // 捕获异常并触发 panic。
        }
        file = new File(path + BOOTER_SUFFIX);  // 更新正式文件对象。
        if (!file.canRead() || !file.canWrite()) {  // 如果文件不可读写，触发 panic。
            Panic.panic(Error.FileCannotRWException);
        }
    }
}
