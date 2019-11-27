package org.apache.rocketmq.acl.common;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * 类描述：
 *
 * @author qying
 * @since 2019/11/26 17:33
 */
public class Test {
    public static void main(String[] args) throws Exception {
        ServerSocketChannel channel = ServerSocketChannel.open();
        channel.configureBlocking(false);
        ServerSocket serverSocket = channel.socket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress("0.0.0.0", 8080));
        Selector selector = Selector.open();
        channel.register(selector, SelectionKey.OP_ACCEPT);
        while (true) {
            int s = selector.select(1000);
            if (s <= 0) {
            } else {
                Iterator<SelectionKey> selecionKeys = selector.selectedKeys().iterator();
//                LOGGER.info("keys:" + selector.selectedKeys().size());
                int accepts = 0;
                while (selecionKeys.hasNext()) {
                    SelectionKey key = selecionKeys.next();
                    if (key.isValid() && key.isAcceptable()) {
                        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        socketChannel.configureBlocking(false);
                        socketChannel.register(selector, SelectionKey.OP_READ);
                        accepts++;
                    }
                    if (key.isValid() && key.isReadable()) {
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int count = socketChannel.read(buffer);
                        if (count >= 0) {
                            byte[] bytes = new byte[1024];
                            StringBuilder sb = new StringBuilder();
                            buffer.flip();
                            while (buffer.hasRemaining()) {
                                buffer.get(bytes, 0, count);
                            }
                            buffer.clear();
                            buffer.put("server received".getBytes());
                            socketChannel.write(buffer);
                            sb.append(bytes);
                            System.out.println(new String(bytes, 0, count));
                        } else {
                            key.cancel();
                        }
                    }
                    selecionKeys.remove();
                }
//                LOGGER.info("accepts:" + accepts);
            }
        }

    }
}
