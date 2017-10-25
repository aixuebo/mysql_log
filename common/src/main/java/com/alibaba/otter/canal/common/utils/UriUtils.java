package com.alibaba.otter.canal.common.utils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;

/**
 * @author zebin.xuzb 2012-11-15 下午3:53:09
 * @since 1.0.0
 * 对url的解析的query转换成Map<String,String>形式
 */
public final class UriUtils {

    private final static String SPLIT            = "&";
    private final static String EQUAL            = "=";
    private final static String DEFAULT_ENCODING = "ISO_8859_1";

    private UriUtils(){
    }

    public static Map<String, String> parseQuery(final String uriString) {
        URI uri = null;
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return parseQuery(uri);
    }

    public static Map<String, String> parseQuery(final String uriString, final String encoding) {
        URI uri = null;
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return parseQuery(uri, encoding);
    }

    public static Map<String, String> parseQuery(final URI uri) {
        return parseQuery(uri, DEFAULT_ENCODING);
    }

    public static Map<String, String> parseQuery(final URI uri, final String encoding) {
        //接续query,即url中xx=xx&yy=yy部分
        if (uri == null || StringUtils.isBlank(uri.getQuery())) {//没有query,则返回空的集合
            return Collections.EMPTY_MAP;
        }
        String query = uri.getRawQuery();
        HashMap<String, String> params = new HashMap<String, String>();
        @SuppressWarnings("resource")
        Scanner scan = new Scanner(query);
        scan.useDelimiter(SPLIT);//按照&拆分字符串
        while (scan.hasNext()) {
        	//解析key=value
            String token = scan.next().trim();
            String[] pair = token.split(EQUAL);
            String key = decode(pair[0], encoding);
            String value = null;
            if (pair.length == 2) {
                value = decode(pair[1], encoding);
            }
            params.put(key, value);
        }
        return params;
    }

    private static String decode(final String content, final String encoding) {
        try {
            return URLDecoder.decode(content, encoding != null ? encoding : DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
