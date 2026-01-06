// 定义一个名为 Tokenizer 的类，该类用于将字节数组形式的 SQL 语句分解成标记（token）。
// Tokenizer 会逐个读取字节，识别出其中的符号、字符串、关键字等部分，供后续的 SQL 解析使用。
// 该类能够处理空白符、符号、字母、数字等各种情况。

package top.dhc.mydb.backend.parser;

// 导入需要的类，其中 Error 类包含一些标准错误处理。
import top.dhc.mydb.common.Error;

public class Tokenizer {
    // 存储传入的 SQL 语句（字节数组）。
    private byte[] stat;
    // 记录当前读取的位置（字节位置）。
    private int pos;
    // 当前解析的标记（token）。
    private String currentToken;
    // 是否需要重新读取标记。
    private boolean flushToken;
    // 记录错误信息（如果有）。
    private Exception err;

    // Tokenizer 构造函数，初始化状态。
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    // peek() 方法：获取下一个标记，但不会消费它。用于查看当前的标记。
    public String peek() throws Exception {
        // 如果存在错误，则抛出异常。
        if(err != null) {
            throw err;
        }
        // 如果需要刷新标记，获取下一个标记。
        if(flushToken) {
            String token = null;
            try {
                token = next();  // 获取下一个标记。
            } catch(Exception e) {
                err = e;  // 记录错误信息。
                throw e;  // 抛出异常。
            }
            currentToken = token;
            flushToken = false;  // 标记已刷新。
        }
        return currentToken;
    }

    // pop() 方法：表示当前标记已处理，刷新当前标记。
    public void pop() {
        flushToken = true;
    }

    // errStat() 方法：返回当前状态的字节数组，并在当前位置插入 " << " 来标记错误发生的位置。
    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);  // 将当前已读取的字节复制到新数组。
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);  // 在当前位置插入错误标记。
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);  // 将剩余部分复制到新数组。
        return res;
    }

    // popByte() 方法：推进当前读取位置。
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    // peekByte() 方法：查看当前字节位置的字节。
    private Byte peekByte() {
        if(pos == stat.length) {
            return null;  // 如果已到达结尾，返回 null。
        }
        return stat[pos];
    }

    // next() 方法：获取下一个标记，处理整个标记的解析过程。
    private String next() throws Exception {
        if(err != null) {
            throw err;  // 如果存在错误，抛出异常。
        }
        return nextMetaState();  // 处理标记的元状态（决定下一步如何解析标记）。
    }

    // nextMetaState() 方法：通过循环逐字节地读取标记，跳过空白符，直到找到有效的标记。
    private String nextMetaState() throws Exception {
        while(true) {
            Byte b = peekByte();  // 查看当前字节。
            if(b == null) {
                return "";  // 如果没有更多字节，返回空字符串。
            }
            if(!isBlank(b)) {
                break;  // 如果遇到非空白符，停止跳过空白。
            }
            popByte();  // 跳过空白符。
        }
        byte b = peekByte();  // 获取当前字节。
        if(isSymbol(b)) {
            popByte();  // 如果是符号，返回该符号并推进读取位置。
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            return nextQuoteState();  // 如果是引号，进入引号状态处理字符串。
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();  // 如果是字母或数字，进入标记状态。
        } else {
            err = Error.InvalidCommandException;  // 如果是无效的字符，抛出错误。
            throw err;
        }
    }

    // nextTokenState() 方法：用于处理由字母、数字或下划线组成的标记（例如表名、字段名等）。
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();  // 用于构建标记字符串。
        while(true) {
            Byte b = peekByte();  // 查看当前字节。
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if(b != null && isBlank(b)) {
                    popByte();  // 如果是空白符，跳过。
                }
                return sb.toString();  // 返回构建的标记。
            }
            sb.append(new String(new byte[]{b}));  // 将字节追加到标记字符串。
            popByte();  // 推进读取位置。
        }
    }

    // isDigit() 方法：检查字节是否是数字字符。
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    // isAlphaBeta() 方法：检查字节是否是字母字符。
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    // nextQuoteState() 方法：处理引号包围的字符串。
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();  // 获取引号字符。
        popByte();  // 跳过引号。
        StringBuilder sb = new StringBuilder();  // 用于构建字符串内容。
        while(true) {
            Byte b = peekByte();  // 查看当前字节。
            if(b == null) {
                err = Error.InvalidCommandException;  // 如果遇到结尾没有匹配的引号，抛出错误。
                throw err;
            }
            if(b == quote) {
                popByte();  // 如果遇到匹配的引号，跳过引号并结束。
                break;
            }
            sb.append(new String(new byte[]{b}));  // 将字节追加到字符串内容中。
            popByte();  // 推进读取位置。
        }
        return sb.toString();  // 返回引号内的字符串内容。
    }

    // isSymbol() 方法：检查字节是否是有效的符号（例如 =, >, <, *, , 等）。
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    // isBlank() 方法：检查字节是否是空白字符（例如空格、制表符、换行符）。
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
