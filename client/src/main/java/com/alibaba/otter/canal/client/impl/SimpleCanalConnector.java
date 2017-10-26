package com.alibaba.otter.canal.client.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.impl.running.ClientRunningData;
import com.alibaba.otter.canal.client.impl.running.ClientRunningListener;
import com.alibaba.otter.canal.client.impl.running.ClientRunningMonitor;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalPacket.Ack;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientAck;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientAuth;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientRollback;
import com.alibaba.otter.canal.protocol.CanalPacket.Compression;
import com.alibaba.otter.canal.protocol.CanalPacket.Get;
import com.alibaba.otter.canal.protocol.CanalPacket.Handshake;
import com.alibaba.otter.canal.protocol.CanalPacket.Messages;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.protocol.CanalPacket.PacketType;
import com.alibaba.otter.canal.protocol.CanalPacket.Sub;
import com.alibaba.otter.canal.protocol.CanalPacket.Unsub;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 基于{@linkplain CanalServerWithNetty}定义的网络协议接口，对于canal数据进行get/rollback/ack等操作
 * 
 * @author jianghang 2012-10-24 下午05:37:20
 * @version 1.0.0
 */
public class SimpleCanalConnector implements CanalConnector {

    private static final Logger  logger                = LoggerFactory.getLogger(SimpleCanalConnector.class);
    private SocketAddress        address;
    private String               username;
    private String               password;
    private int                  soTimeout             = 60000;                                              // milliseconds
    private String               filter;                                                                     // 记录上一次的filter提交值,便于自动重试时提交

    private final ByteBuffer     readHeader            = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);//头字节---因此是4个字节
    private final ByteBuffer     writeHeader           = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);//头字节---因此是4个字节
    private SocketChannel        channel;//连接服务器的通道--本地属于客户端
    private List<Compression>    supportedCompressions = new ArrayList<Compression>();//客户端支持的压缩方式
    private ClientIdentity       clientIdentity;
    private ClientRunningMonitor runningMonitor;                                                             // 运行控制
    private ZkClientx            zkClientx;
    private BooleanMutex         mutex                 = new BooleanMutex(false);
    private volatile boolean     connected             = false;                                              // 代表connected是否已正常执行，因为有HA，不代表在工作中
    private boolean              rollbackOnConnect     = true;                                               // 是否在connect链接成功后，自动执行rollback操作
    private boolean              rollbackOnDisConnect  = false;                                              // 是否在connect链接成功后，自动执行rollback操作

    // 读写数据分别使用不同的锁进行控制，减小锁粒度,读也需要排他锁，并发度容易造成数据包混乱，反序列化失败
    private Object               readDataLock          = new Object();
    private Object               writeDataLock         = new Object();

    public SimpleCanalConnector(SocketAddress address, String username, String password, String destination){
        this(address, username, password, destination, 60000);
    }

    public SimpleCanalConnector(SocketAddress address, String username, String password, String destination,
                                int soTimeout){
        this.address = address;
        this.username = username;
        this.password = password;
        this.soTimeout = soTimeout;
        this.clientIdentity = new ClientIdentity(destination, (short) 1001);
    }

    public void connect() throws CanalClientException {
        if (connected) {
            return;
        }

        if (runningMonitor != null) {
            if (!runningMonitor.isStart()) {
                runningMonitor.start();
            }
        } else {
            waitClientRunning();
            doConnect();
            if (filter != null) { // 如果存在条件，说明是自动切换，基于上一次的条件订阅一次
                subscribe(filter);
            }
            if (rollbackOnConnect) {
                rollback();
            }
        }

        connected = true;
    }

    public void disconnect() throws CanalClientException {
        if (rollbackOnDisConnect && channel.isConnected()) {
            rollback();
        }

        connected = false;
        if (runningMonitor != null) {
            if (runningMonitor.isStart()) {
                runningMonitor.stop();
            }
        } else {
            doDisconnnect();
        }
    }

    private InetSocketAddress doConnect() throws CanalClientException {
        try {
            channel = SocketChannel.open();
            channel.socket().setSoTimeout(soTimeout);
            SocketAddress address = getAddress();//获取服务器地址
            if (address == null) {
                address = getNextAddress();
            }
            channel.connect(address);
            Packet p = Packet.parseFrom(readNextPacket(channel));//将数据还原成Packet对象
            if (p.getVersion() != 1) {
                throw new CanalClientException("unsupported version at this client.");
            }

            if (p.getType() != PacketType.HANDSHAKE) {//一定是握手服务
                throw new CanalClientException("expect handshake but found other type.");
            }
            //
            Handshake handshake = Handshake.parseFrom(p.getBody());
            supportedCompressions.addAll(handshake.getSupportedCompressionsList());//说明压缩方式通过
            //
            ClientAuth ca = ClientAuth.newBuilder()
                .setUsername(username != null ? username : "")
                .setNetReadTimeout(soTimeout)
                .setNetWriteTimeout(soTimeout)
                .build();

            //向服务器发送一个包,包的内容就是ClientAuth权限信息
            writeWithHeader(channel,
                Packet.newBuilder()
                    .setType(PacketType.CLIENTAUTHENTICATION)
                    .setBody(ca.toByteString())
                    .build()
                    .toByteArray());

            Packet ack = Packet.parseFrom(readNextPacket(channel));//获取下一个事件包
            if (ack.getType() != PacketType.ACK) {//一定是ack事件
                throw new CanalClientException("unexpected packet type when ack is expected");
            }

            Ack ackBody = Ack.parseFrom(ack.getBody());//解析内容
            if (ackBody.getErrorCode() > 0) {
                throw new CanalClientException("something goes wrong when doing authentication: "
                                               + ackBody.getErrorMessage());
            }

            connected = true;//连接成功
            return new InetSocketAddress(channel.socket().getLocalAddress(), channel.socket().getLocalPort());
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    private void doDisconnnect() throws CanalClientException {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                logger.warn("exception on closing channel:{} \n {}", channel, e);
            }
            channel = null;
        }
    }

    public void subscribe() throws CanalClientException {
        subscribe(""); // 传递空字符即可
    }

    public void subscribe(String filter) throws CanalClientException {
        waitClientRunning();
        try {
            //发送客户端订阅的filter信息
            writeWithHeader(channel,
                    //组成一个数据包
                Packet.newBuilder()
                    .setType(PacketType.SUBSCRIPTION)
                    .setBody(Sub.newBuilder()
                        .setDestination(clientIdentity.getDestination())
                        .setClientId(String.valueOf(clientIdentity.getClientId()))
                        .setFilter(filter != null ? filter : "")
                        .build()
                        .toByteString())
                    .build()
                    .toByteArray());
            //
            Packet p = Packet.parseFrom(readNextPacket(channel));//收到服务器的返回值
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getErrorCode() > 0) {
                throw new CanalClientException("failed to subscribe with reason: " + ack.getErrorMessage());
            }

            clientIdentity.setFilter(filter);
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    public void unsubscribe() throws CanalClientException {
        waitClientRunning();
        try {
            writeWithHeader(channel,
                Packet.newBuilder()
                    .setType(PacketType.UNSUBSCRIPTION)
                    .setBody(Unsub.newBuilder()
                        .setDestination(clientIdentity.getDestination())
                        .setClientId(String.valueOf(clientIdentity.getClientId()))
                        .build()
                        .toByteString())
                    .build()
                    .toByteArray());
            //
            Packet p = Packet.parseFrom(readNextPacket(channel));
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getErrorCode() > 0) {
                throw new CanalClientException("failed to unSubscribe with reason: " + ack.getErrorMessage());
            }
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    //需要ack
    public Message get(int batchSize) throws CanalClientException {
        return get(batchSize, null, null);
    }

    //需要ack
    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        Message message = getWithoutAck(batchSize, timeout, unit);
        ack(message.getId());
        return message;
    }

    //不需要ack
    public Message getWithoutAck(int batchSize) throws CanalClientException {
        return getWithoutAck(batchSize, null, null);
    }

    //不需要ack
    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        waitClientRunning();
        try {
            int size = (batchSize <= 0) ? 1000 : batchSize;
            long time = (timeout == null || timeout < 0) ? -1 : timeout; // -1代表不做timeout控制
            if (unit == null) {
                unit = TimeUnit.MILLISECONDS;
            }

            //发送get请求
            writeWithHeader(channel,
                Packet.newBuilder()
                    .setType(PacketType.GET)
                    .setBody(Get.newBuilder()
                        .setAutoAck(false)
                        .setDestination(clientIdentity.getDestination())
                        .setClientId(String.valueOf(clientIdentity.getClientId()))
                        .setFetchSize(size)
                        .setTimeout(time)
                        .setUnit(unit.ordinal())
                        .build()
                        .toByteString())
                    .build()
                    .toByteArray());

            return receiveMessages();//接收返回值
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    private Message receiveMessages() throws InvalidProtocolBufferException, IOException {
        Packet p = Packet.parseFrom(readNextPacket(channel));//接收返回值
        switch (p.getType()) {
            case MESSAGES: {
                if (!p.getCompression().equals(Compression.NONE)) {
                    throw new CanalClientException("compression is not supported in this connector");
                }

                Messages messages = Messages.parseFrom(p.getBody());
                Message result = new Message(messages.getBatchId());
                for (ByteString byteString : messages.getMessagesList()) {//循环每一个事件,转换成Entry对象
                    result.addEntry(Entry.parseFrom(byteString));
                }
                return result;
            }
            case ACK: {//肯定是失败了,因为没有拿到信息
                Ack ack = Ack.parseFrom(p.getBody());
                throw new CanalClientException("something goes wrong with reason: " + ack.getErrorMessage());
            }
            default: {
                throw new CanalClientException("unexpected packet type: " + p.getType());
            }
        }
    }

    //发送给服务器说明这个批处理已经消费完成了
    public void ack(long batchId) throws CanalClientException {
        waitClientRunning();
        //通知服务器哪个客户端,对哪个目的地队列的哪个批处理ID消费成功
        ClientAck ca = ClientAck.newBuilder()
            .setDestination(clientIdentity.getDestination())
            .setClientId(String.valueOf(clientIdentity.getClientId()))
            .setBatchId(batchId)
            .build();
        try {
            //发送数据
            writeWithHeader(channel, Packet.newBuilder()
                .setType(PacketType.CLIENTACK)
                .setBody(ca.toByteString())
                .build()
                .toByteArray());
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    //向服务器发送批处理的回滚信息
    public void rollback(long batchId) throws CanalClientException {
        waitClientRunning();
        //客户端回滚请求实体,通知服务器该客户端要回滚哪一个队列的数据,该数据是批处理ID指代的数据
        ClientRollback ca = ClientRollback.newBuilder()
            .setDestination(clientIdentity.getDestination())
            .setClientId(String.valueOf(clientIdentity.getClientId()))
            .setBatchId(batchId)
            .build();
        try {
            writeWithHeader(channel, Packet.newBuilder()
                .setType(PacketType.CLIENTROLLBACK)
                .setBody(ca.toByteString())
                .build()
                .toByteArray());
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    public void rollback() throws CanalClientException {
        waitClientRunning();
        rollback(0);// 0代笔未设置
    }

    // ==================== helper method ====================
    //向服务器发送请求头内容
    private void writeWithHeader(SocketChannel channel, byte[] body) throws IOException {
        synchronized (writeDataLock) {
            writeHeader.clear();
            writeHeader.putInt(body.length);//写入多少个字节的body
            writeHeader.flip();
            channel.write(writeHeader);
            channel.write(ByteBuffer.wrap(body));//写入具体body内容
        }
    }

    //读取下一个包的内容
    private byte[] readNextPacket(SocketChannel channel) throws IOException {
        synchronized (readDataLock) {
            readHeader.clear();
            read(channel, readHeader);//填充头信息
            int bodyLen = readHeader.getInt(0);
            ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen).order(ByteOrder.BIG_ENDIAN);
            read(channel, bodyBuf);
            return bodyBuf.array();
        }
    }

    //读满buffer数据
    private void read(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int r = channel.read(buffer);
            if (r == -1) {
                throw new IOException("end of stream when reading header");
            }
        }
    }

    //初始化客户端监听器
    private synchronized void initClientRunningMonitor(ClientIdentity clientIdentity) {
        if (zkClientx != null && clientIdentity != null && runningMonitor == null) {
            ClientRunningData clientData = new ClientRunningData();
            clientData.setClientId(clientIdentity.getClientId());
            clientData.setAddress(AddressUtils.getHostIp());//设置客户端本地机器的IP

            runningMonitor = new ClientRunningMonitor();
            runningMonitor.setDestination(clientIdentity.getDestination());
            runningMonitor.setZkClient(zkClientx);
            runningMonitor.setClientData(clientData);
            runningMonitor.setListener(new ClientRunningListener() {//如何处理监听器

                public InetSocketAddress processActiveEnter() {
                    InetSocketAddress address = doConnect();//重新连接
                    mutex.set(true);
                    if (filter != null) { // 如果存在条件，说明是自动切换，基于上一次的条件订阅一次
                        subscribe(filter);
                    }

                    if (rollbackOnConnect) {//是否先执行回滚
                        rollback();
                    }

                    return address;
                }

                public void processActiveExit() {
                    mutex.set(false);
                    doDisconnnect();
                }

            });
        }
    }

    //阻塞等待,一直等待没有请求了,可以继续请求为止
    private void waitClientRunning() {
        try {
            if (zkClientx != null) {
                if (!connected) {// 未调用connect
                    throw new CanalClientException("should connect first");
                }

                mutex.get();// 阻塞等待
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CanalClientException(e);
        }
    }

    public boolean checkValid() {
        if (zkClientx != null) {
            return mutex.state();
        } else {
            return true;// 默认都放过
        }
    }

    public SocketAddress getNextAddress() {
        return null;
    }

    public SocketAddress getAddress() {
        return address;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public void setZkClientx(ZkClientx zkClientx) {
        this.zkClientx = zkClientx;
        initClientRunningMonitor(this.clientIdentity);
    }

    public void setRollbackOnConnect(boolean rollbackOnConnect) {
        this.rollbackOnConnect = rollbackOnConnect;
    }

    public void setRollbackOnDisConnect(boolean rollbackOnDisConnect) {
        this.rollbackOnDisConnect = rollbackOnDisConnect;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

}
