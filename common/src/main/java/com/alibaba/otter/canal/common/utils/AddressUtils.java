package com.alibaba.otter.canal.common.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.Enumeration;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddressUtils {

    private static final Logger  logger       = LoggerFactory.getLogger(AddressUtils.class);
    private static final String  LOCALHOST_IP = "127.0.0.1";
    private static final String  EMPTY_IP     = "0.0.0.0";
    private static final Pattern IP_PATTERN   = Pattern.compile("[0-9]{1,3}(\\.[0-9]{1,3}){3,}");//ip的正则表达式

    //true表示该端口可用
    public static boolean isAvailablePort(int port) {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
            ss.bind(null);
            return true;
        } catch (IOException e) {//说明端口被占用等情况发生
            return false;
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                }
            }
        }
    }

    //true表示提供的ip或者host是有效可用的
    private static boolean isValidHostAddress(InetAddress address) {
        if (address == null || address.isLoopbackAddress()) return false;
        String name = address.getHostAddress();
        return (name != null && !EMPTY_IP.equals(name) && !LOCALHOST_IP.equals(name) && IP_PATTERN.matcher(name).matches());
    }

    //获取本地内网ip
    public static String getHostIp() {
        InetAddress address = getHostAddress();
        return address == null ? null : address.getHostAddress();
    }

    //获取本地host
    public static String getHostName() {
        InetAddress address = getHostAddress();
        return address == null ? null : address.getHostName();
    }

    public static InetAddress getHostAddress() {
        InetAddress localAddress = null;
        try {
            localAddress = InetAddress.getLocalHost();//获取本地IP
            if (isValidHostAddress(localAddress)) {//是有效的,则返回
                return localAddress;
            }
        } catch (Throwable e) {
            logger.warn("Failed to retriving local host ip address, try scan network card ip address. cause: "
                        + e.getMessage());
        }
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    try {
                        NetworkInterface network = interfaces.nextElement();
                        Enumeration<InetAddress> addresses = network.getInetAddresses();
                        if (addresses != null) {
                            while (addresses.hasMoreElements()) {
                                try {
                                    InetAddress address = addresses.nextElement();
                                    if (isValidHostAddress(address)) {
                                        return address;
                                    }
                                } catch (Throwable e) {
                                    logger.warn("Failed to retriving network card ip address. cause:" + e.getMessage());
                                }
                            }
                        }
                    } catch (Throwable e) {
                        logger.warn("Failed to retriving network card ip address. cause:" + e.getMessage());
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn("Failed to retriving network card ip address. cause:" + e.getMessage());
        }
        logger.error("Could not get local host ip address, will use 127.0.0.1 instead.");
        return localAddress;
    }
    
    public static void main(String[] args) {
    	System.out.println(AddressUtils.getHostIp());
    	System.out.println(AddressUtils.getHostName());
	}
    
}
