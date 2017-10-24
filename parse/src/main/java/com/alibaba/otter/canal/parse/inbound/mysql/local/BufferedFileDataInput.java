package com.alibaba.otter.canal.parse.inbound.mysql.local;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jianghang 2012-7-7 下午03:10:47
 * @version 1.0.0
 * 文件进行包装  DataInputStream<BufferedInputStream<FileInputStream<File>>>
 */
public class BufferedFileDataInput {

    private static final Logger logger = LoggerFactory.getLogger(BufferedFileDataInput.class);
    // Read parameters.
    private File                file;//读取该文件
    private int                 size;//buffer缓冲区大小

    // Variables to control reading.
    private FileInputStream     fileInput;//文件流
    private BufferedInputStream bufferedInput;//文件缓冲流
    private DataInputStream     dataInput;//对bufferedInput进一步包装,即想从文件中读取的都是int long 这种基础类型
    private long                offset;//当前的位移位置
    private FileChannel         fileChannel;//读取file的文件流

    public BufferedFileDataInput(File file, int size) throws FileNotFoundException, IOException, InterruptedException{
        this.file = file;
        this.size = size;
    }

    public BufferedFileDataInput(File file) throws FileNotFoundException, IOException, InterruptedException{
        this(file, 1024);
    }

    //剩余字节数
    public long available() throws IOException {
        return fileChannel.size() - offset;
    }

    //跳过若干个字节
    public long skip(long bytes) throws IOException {
        long bytesSkipped = bufferedInput.skip(bytes);
        offset += bytesSkipped;
        return bytesSkipped;
    }

    //定位到参数的位置
    public void seek(long seekBytes) throws FileNotFoundException, IOException, InterruptedException {
        fileInput = new FileInputStream(file);
        fileChannel = fileInput.getChannel();

        try {
            fileChannel.position(seekBytes);
        } catch (ClosedByInterruptException e) {
            throw new InterruptedException();
        }
        bufferedInput = new BufferedInputStream(fileInput, size);
        dataInput = new DataInputStream(bufferedInput);
        offset = seekBytes;
    }


    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    //读取数据内容,读取到bytes参数里面
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        dataInput.readFully(bytes, start, len);
        offset += len;
    }

    public void close() {
        try {
            if (fileChannel != null) {
                fileChannel.close();
                fileInput.close();
            }
        } catch (IOException e) {
            logger.warn("Unable to close buffered file reader: file=" + file.getName() + " exception=" + e.getMessage());
        }

        fileChannel = null;
        fileInput = null;
        bufferedInput = null;
        dataInput = null;
        offset = -1;
    }

}
