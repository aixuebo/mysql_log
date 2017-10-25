package com.alibaba.otter.canal.common.zookeeper;

import java.io.UnsupportedEncodingException;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * 基于string的序列化方式
 * 将字符串或者字节数组进行序列化成字节数组的过程
 * @author jianghang 2012-7-11 下午02:57:09
 * @version 1.0.0
 */
public class ByteSerializer implements ZkSerializer {

    //反序列化---因为反序列化的结果就是字节数组,因此直接返回即可
    public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
        return bytes;
    }

    //序列化
    public byte[] serialize(final Object data) throws ZkMarshallingError {
        try {
            if (data instanceof byte[]) {//如果对象本身就是字节数组,直接返回即可
                return (byte[]) data;
            } else {
                return ((String) data).getBytes("utf-8");
            }
        } catch (final UnsupportedEncodingException e) {
            throw new ZkMarshallingError(e);
        }
    }

}
