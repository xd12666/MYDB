package top.dhc.mydb.transport;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import top.dhc.mydb.common.Error;

/**
 * 数据包编码器（Encoder）
 * 
 * 职责：
 * 负责数据包的编码和解码
 * 区分正常数据和错误信息
 * 
 * 数据包格式：
 * - 第一个字节：标志位
 *   - 0: 正常数据包，后面跟着数据内容
 *   - 1: 错误数据包，后面跟着错误信息（字符串）
 * 
 * 编码规则：
 * - 正常数据包: [0] + data
 * - 错误数据包: [1] + error_message_bytes
 * 
 * 使用场景：
 * - 服务器执行SQL成功时，发送 [0] + result_data
 * - 服务器执行SQL失败时，发送 [1] + error_message
 */
public class Encoder {

    /**
     * 编码数据包
     * 
     * 编码规则：
     * - 如果包中包含错误，编码为 [1] + 错误信息
     * - 如果包中只有数据，编码为 [0] + 数据内容
     * 
     * @param pkg 要编码的数据包
     * @return 编码后的字节数组
     */
    public byte[] encode(Package pkg) {
        // 检查是否包含错误
        if(pkg.getErr() != null) {
            // 获取错误信息
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            // 编码为 [1] + 错误信息
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            // 编码为 [0] + 数据内容
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    /**
     * 解码数据包
     * 
     * 解码规则：
     * - 如果第一个字节是0，解码为正常数据包
     * - 如果第一个字节是1，解码为错误数据包
     * 
     * @param data 要解码的字节数组
     * @return 解码后的数据包
     * @throws Exception 如果数据格式无效
     */
    public Package decode(byte[] data) throws Exception {
        // 检查数据长度
        if(data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        // 根据第一个字节判断包类型
        if(data[0] == 0) {
            // 正常数据包：提取数据部分（跳过第一个字节）
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            // 错误数据包：提取错误信息并创建异常
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            // 无效的标志位
            throw Error.InvalidPkgDataException;
        }
    }

}
