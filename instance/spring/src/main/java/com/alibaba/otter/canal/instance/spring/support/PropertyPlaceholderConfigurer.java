package com.alibaba.otter.canal.instance.spring.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.Assert;

/**
 * 扩展Spring的
 * {@linkplain org.springframework.beans.factory.config.PropertyPlaceholderConfigurer}
 * ，增加默认值的功能。 例如：${placeholder:defaultValue}，假如placeholder的值不存在，则默认取得
 * defaultValue。
 * 
 * @author jianghang 2013-1-24 下午03:37:56
 * @version 1.0.0
 */
public class PropertyPlaceholderConfigurer extends org.springframework.beans.factory.config.PropertyPlaceholderConfigurer implements ResourceLoaderAware, InitializingBean {

    private static final String PLACEHOLDER_PREFIX = "${";
    private static final String PLACEHOLDER_SUFFIX = "}";
    private ResourceLoader      loader;
    private String[]            locationNames;

    public PropertyPlaceholderConfigurer(){
        setIgnoreUnresolvablePlaceholders(true);
    }

    public void setResourceLoader(ResourceLoader loader) {
        this.loader = loader;
    }

    public void setLocationNames(String[] locations) {
        this.locationNames = locations;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(loader, "no resourceLoader");

        if (locationNames != null) {
            for (int i = 0; i < locationNames.length; i++) {
                locationNames[i] = resolveSystemPropertyPlaceholders(locationNames[i]);//替换成具体的值
            }
        }

        if (locationNames != null) {
            List<Resource> resources = new ArrayList<Resource>(locationNames.length);

            for (String location : locationNames) {
                location = trimToNull(location);

                if (location != null) {
                    resources.add(loader.getResource(location));
                }
            }

            super.setLocations(resources.toArray(new Resource[resources.size()]));
        }
    }

    //将${}内容替换成具体的值
    private String resolveSystemPropertyPlaceholders(String text) {
        StringBuilder buf = new StringBuilder(text);

        for (int startIndex = buf.indexOf(PLACEHOLDER_PREFIX); startIndex >= 0;) {//从${位置开始循环
            int endIndex = buf.indexOf(PLACEHOLDER_SUFFIX, startIndex + PLACEHOLDER_PREFIX.length());//找到}位置

            if (endIndex != -1) {
                String placeholder = buf.substring(startIndex + PLACEHOLDER_PREFIX.length(), endIndex);//找到${}之间的内容
                int nextIndex = endIndex + PLACEHOLDER_SUFFIX.length();

                try {
                    String value = resolveSystemPropertyPlaceholder(placeholder);//替换成具体的value值

                    if (value != null) {
                        buf.replace(startIndex, endIndex + PLACEHOLDER_SUFFIX.length(), value);//将${}的内容替换成value
                        nextIndex = startIndex + value.length();
                    } else {
                        System.err.println("Could not resolve placeholder '"
                                           + placeholder
                                           + "' in ["
                                           + text
                                           + "] as system property: neither system property nor environment variable found");
                    }
                } catch (Throwable ex) {
                    System.err.println("Could not resolve placeholder '" + placeholder + "' in [" + text
                                       + "] as system property: " + ex);
                }

                startIndex = buf.indexOf(PLACEHOLDER_PREFIX, nextIndex);
            } else {
                startIndex = -1;
            }
        }

        return buf.toString();
    }

    //从系统中找到key对应的值,然后返回
    private String resolveSystemPropertyPlaceholder(String placeholder) {
        DefaultablePlaceholder dp = new DefaultablePlaceholder(placeholder);
        String value = System.getProperty(dp.placeholder);

        if (value == null) {
            value = System.getenv(dp.placeholder);
        }

        if (value == null) {
            value = dp.defaultValue;
        }

        return value;
    }

    @Override
    protected String resolvePlaceholder(String placeholder, Properties props, int systemPropertiesMode) {
        DefaultablePlaceholder dp = new DefaultablePlaceholder(placeholder);
        String value = super.resolvePlaceholder(dp.placeholder, props, systemPropertiesMode);

        if (value == null) {
            value = dp.defaultValue;
        }

        return trimToEmpty(value);
    }

    //demo ${key:default},将key:default拆分成key和default,后者表示默认值,前者表示要去环境中替换成具体的值
    private static class DefaultablePlaceholder {

        private final String defaultValue;//默认值
        private final String placeholder;//key要被替换的值

        public DefaultablePlaceholder(String placeholder){
            int commaIndex = placeholder.indexOf(":");
            String defaultValue = null;

            if (commaIndex >= 0) {
                defaultValue = trimToEmpty(placeholder.substring(commaIndex + 1));
                placeholder = trimToEmpty(placeholder.substring(0, commaIndex));
            }

            this.placeholder = placeholder;
            this.defaultValue = defaultValue;
        }
    }

    //将空字符串转换成null
    private String trimToNull(String str) {
        if (str == null) {
            return null;
        }

        String result = str.trim();

        if (result == null || result.length() == 0) {
            return null;
        }

        return result;
    }

    //将空字符串转换成""
    public static String trimToEmpty(String str) {
        if (str == null) {
            return "";
        }

        return str.trim();
    }
}
