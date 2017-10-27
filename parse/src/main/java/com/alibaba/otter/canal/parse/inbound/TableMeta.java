package com.alibaba.otter.canal.parse.inbound;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;

/**
 * 描述数据meta对象,mysql binlog中对应的{@linkplain TableMapLogEvent}包含的信息不全
 * 
 * <pre>
 * 1. 主键信息
 * 2. column name
 * 3. unsigned字段
 * </pre>
 * 
 * @author jianghang 2013-1-18 下午12:24:59
 *
 * 使用desc 数据库.table命令返回具体的信息就是该表数据
 * @version 1.0.0
 */
public class TableMeta {

    private String          fullName; // schema.table 数据库.table表示表的路径名字
    private List<FieldMeta> fileds;//该表有哪些字段

    public TableMeta(String fullName, List<FieldMeta> fileds){
        this.fullName = fullName;
        this.fileds = fileds;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public List<FieldMeta> getFileds() {
        return fileds;
    }

    public void setFileds(List<FieldMeta> fileds) {
        this.fileds = fileds;
    }

    //表示一个字段
    public static class FieldMeta {

        private String columnName;//字段name
        private String columnType;//字段类型
        private String isNullable;//字段是否允许为空
        private String iskey;//字段是否是主键,比如PRI 或者 MUL这样的信息
        private String defaultValue;//字段的默认值
        private String extra;//额外信息---比如auto_increment

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
        }

        public String getIsNullable() {
            return isNullable;
        }

        public void setIsNullable(String isNullable) {
            this.isNullable = isNullable;
        }

        public String getIskey() {
            return iskey;
        }

        public void setIskey(String iskey) {
            this.iskey = iskey;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(String extra) {
            this.extra = extra;
        }

        public boolean isUnsigned() {
            return StringUtils.containsIgnoreCase(columnType, "unsigned");
        }

        public boolean isKey() {
            return StringUtils.equalsIgnoreCase(iskey, "PRI");
        }

        public boolean isNullable() {
            return StringUtils.equalsIgnoreCase(isNullable, "YES");
        }

        public String toString() {
            return "FieldMeta [columnName=" + columnName + ", columnType=" + columnType + ", defaultValue="
                   + defaultValue + ", extra=" + extra + ", isNullable=" + isNullable + ", iskey=" + iskey + "]";
        }

    }
}
