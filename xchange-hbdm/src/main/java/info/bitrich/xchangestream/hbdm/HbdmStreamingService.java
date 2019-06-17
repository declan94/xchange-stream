package info.bitrich.xchangestream.hbdm;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import info.bitrich.xchangestream.service.netty.NettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.reactivex.ObservableEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public abstract class HbdmStreamingService extends JsonNettyStreamingService {

    protected final ObjectMapper mapper = new ObjectMapper();

    protected List<ObservableEmitter<Long>> delayEmitters = new LinkedList<>();

    public HbdmStreamingService(String apiUrl) {
        super(apiUrl);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public void addDelayEmitter(ObservableEmitter<Long> delayEmitter) {
        delayEmitters.add(delayEmitter);
    }

    @Override
    protected WebSocketClientHandler getWebSocketClientHandler(WebSocketClientHandshaker handshaker, WebSocketClientHandler.WebSocketMessageHandler handler) {
        return new OkCoinNettyWebSocketClientHandler(handshaker, handler);
    }

    protected class OkCoinNettyWebSocketClientHandler extends NettyStreamingService.NettyWebSocketClientHandler {

        private final Logger LOG = LoggerFactory.getLogger(OkCoinNettyWebSocketClientHandler.class);

        protected OkCoinNettyWebSocketClientHandler(WebSocketClientHandshaker handshaker, WebSocketMessageHandler handler) {
            super(handshaker, handler);
        }

        private byte[] decode(byte[] data) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            decompress(bais, baos);
            baos.flush();
            baos.close();
            bais.close();
            return baos.toByteArray();
        }

        private void decompress(InputStream is, OutputStream os) throws IOException {
            GZIPInputStream gis = new GZIPInputStream(is);
            int count;
            byte[] data = new byte[1024];
            while ((count = gis.read(data, 0, 1024)) != -1) {
                os.write(data, 0, count);
            }
            gis.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            super.channelInactive(ctx);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

            if (!handshaker.isHandshakeComplete()) {
                super.channelRead0(ctx, msg);
                return;
            }

            super.channelRead0(ctx, msg);

            WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof BinaryWebSocketFrame) {
                BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
                ByteBuf byteBuf = binaryFrame.content();
                byte[] data = new byte[byteBuf.readableBytes()];
                ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
                try {
                    bis.read(data);
                    bis.close();
                    handler.onMessage(new String(decode(data), "UTF-8"));
                } catch (Exception e) {
                    LOG.trace("Error when inflate websocket binary message");
                }
            }

        }

    }

}
