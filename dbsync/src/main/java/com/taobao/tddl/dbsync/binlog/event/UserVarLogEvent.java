package com.taobao.tddl.dbsync.binlog.event;

import java.io.IOException;
import java.io.Serializable;

import com.taobao.tddl.dbsync.binlog.CharsetConversion;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * User_var_log_event. Every time a query uses the value of a user variable, a
 * User_var_log_event is written before the Query_log_event, to set the user
 * variable.
 *
 * 用户自定义变量事件
 * 查询之前,用户会自定义变量,因此在query事件前,先有UserVarLogEvent事件
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 *
 * 4个字节,表示变量name的长度,然后追加若干个字符,表示name的内容
 * 1个字节,非0,则表示value是null,0则表示value有值,要接下来继续解析
 * 1个字节,表示属性值的类型,
 * 4个字节,表示字符串的时候,.使用什么编码集
 * 4个字节,表示属性值的占用的大小,比如String类型,因此是记录字节占用内容 + 追加若干个字符串内容字节数组
 * 如果此时是int 那就直接读取int的值,如果是disimal,就是具体的内容值
 *
 */
public final class UserVarLogEvent extends LogEvent {

    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>4 bytes. the size of the user variable name.</li>
     * <li>The user variable name.</li>
     * <li>1 byte. Non-zero if the variable value is the SQL NULL value, 0
     * otherwise. If this byte is 0, the following parts exist in the event.</li>
     * <li>1 byte. The user variable type. The value corresponds to elements of
     * enum Item_result defined in include/mysql_com.h.</li>
     * <li>4 bytes. The number of the character set for the user variable
     * (needed for a string variable). The character set number is really a
     * collation number that indicates a character set/collation pair.</li>
     * <li>4 bytes. The size of the user variable value (corresponds to member
     * val_len of class Item_string).</li>
     * <li>Variable-sized. For a string variable, this is the string. For a
     * float or integer variable, this is its value in 8 bytes.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    private final String       name;
    private final Serializable value;//属性值内容
    private final int          type;//属性值类型
    private final int          charsetNumber;//属性值用什么编码
    private final boolean      isNull;//属性值是否是空

    /**
     * The following is for user defined functions
     * 
     * @see mysql-5.1.60//include/mysql_com.h
     */
    public static final int    STRING_RESULT          = 0;//说明数据是字符串形式的
    public static final int    REAL_RESULT            = 1;//double结果集
    public static final int    INT_RESULT             = 2;//int结果集
    public static final int    ROW_RESULT             = 3;
    public static final int    DECIMAL_RESULT         = 4;//小数结果集

    /* User_var event data */
    public static final int    UV_VAL_LEN_SIZE        = 4;
    public static final int    UV_VAL_IS_NULL         = 1;
    public static final int    UV_VAL_TYPE_SIZE       = 1;
    public static final int    UV_NAME_LEN_SIZE       = 4;
    public static final int    UV_CHARSET_NUMBER_SIZE = 4;

    public UserVarLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent)
                                                                                                          throws IOException{
        super(header);

        /* The Post-Header is empty. The Variable Data part begins immediately. */
        buffer.position(descriptionEvent.commonHeaderLen + descriptionEvent.postHeaderLen[USER_VAR_EVENT - 1]);
        final int nameLen = (int) buffer.getUint32();//4个字节,表示name的长度
        name = buffer.getFixString(nameLen); // UV_NAME_LEN_SIZE
        isNull = (0 != buffer.getInt8());//1个字节,表示该字段是否是null

        if (isNull) {
            type = STRING_RESULT;
            charsetNumber = 63; /* binary */
            value = null;
        } else {
            type = buffer.getInt8(); // UV_VAL_IS_NULL 获取类型
            charsetNumber = (int) buffer.getUint32(); // buf + UV_VAL_TYPE_SIZE 获取编码
            final int valueLen = (int) buffer.getUint32(); // buf +
                                                           // UV_CHARSET_NUMBER_SIZE 字符存储的长度
            final int limit = buffer.limit(); /* for restore *///先找到limit,为了处理完成后,还原limit位置,因此先临时存储一下
            buffer.limit(buffer.position() + valueLen);

            /* @see User_var_log_event::print */
            switch (type) {
                case REAL_RESULT:
                    value = Double.valueOf(buffer.getDouble64()); // float8get
                    break;
                case INT_RESULT:
                    if (valueLen == 8) value = Long.valueOf(buffer.getLong64()); // !uint8korr
                    else if (valueLen == 4) value = Long.valueOf(buffer.getUint32());
                    else throw new IOException("Error INT_RESULT length: " + valueLen);
                    break;
                case DECIMAL_RESULT:
                    final int precision = buffer.getInt8();
                    final int scale = buffer.getInt8();
                    value = buffer.getDecimal(precision, scale); // bin2decimal
                    break;
                case STRING_RESULT:
                    String charsetName = CharsetConversion.getJavaCharset(charsetNumber);
                    value = buffer.getFixString(valueLen, charsetName);//对数据进行编码后返回
                    break;
                case ROW_RESULT:
                    // this seems to be banned in MySQL altogether
                    throw new IOException("ROW_RESULT is unsupported");
                default:
                    value = null;
                    break;
            }
            buffer.limit(limit);
        }
    }

    public final String getQuery() {
        if (value == null) {
            return "SET @" + name + " := NULL";
        } else if (type == STRING_RESULT) {//说明数据是字符串形式的
            // TODO: do escaping !?
            return "SET @" + name + " := \'" + value + '\'';
        } else {//说明数据是整数形式的 或者其他形式
            return "SET @" + name + " := " + String.valueOf(value);
        }
    }
}
