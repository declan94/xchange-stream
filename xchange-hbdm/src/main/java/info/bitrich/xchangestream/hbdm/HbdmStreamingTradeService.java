package info.bitrich.xchangestream.hbdm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import org.knowm.xchange.ExchangeSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class HbdmStreamingTradeService extends HbdmStreamingService {

    private final static Logger logger = LoggerFactory.getLogger(HbdmStreamingTradeService.class);

    private String requestHost = "";

    private String requestPath = "";

    protected ExchangeSpecification exchangeSpecification;

    public HbdmStreamingTradeService(String apiUrl) {
        super(apiUrl);
        try {
            URI uri = new URI(apiUrl);
            requestHost = uri.getHost();
            requestPath = uri.getPath();
        } catch (URISyntaxException e) {
        }
    }

    public void setExchangeSpecification(ExchangeSpecification exchangeSpecification) {
        this.exchangeSpecification = exchangeSpecification;
    }

    @Override
    public Completable connect() {
        Completable conn = super.connect();
        String apiKey = exchangeSpecification.getApiKey();
        String apiSecret = exchangeSpecification.getSecretKey();
        if (apiKey == null || apiKey.isEmpty()) {
            return conn;
        }
        return conn.andThen((CompletableSource) (completable) -> {
            // login
            try {
                Map<String, Object> authMsg = HbdmAuthenticator.authenticateMessage(apiKey, apiSecret, requestHost, requestPath);
                sendMessage(mapper.writeValueAsString(authMsg));
                Thread.sleep(100);
            } catch (UnsupportedEncodingException | InvalidKeyException | NoSuchAlgorithmException | JsonProcessingException e) {
                completable.onError(e);
            } catch (InterruptedException e) {
            }
            completable.onComplete();
        });
    }

    @Override
    protected String getChannelNameFromMessage(JsonNode message) throws IOException {
        return message.has("topic") ? message.get("topic").asText() : "";
    }

    @Override
    public String getSubscribeMessage(String channelName, Object... args) throws IOException {
        Map<String, String> msg = new HashMap<>();
        msg.put("op", "sub");
        msg.put("topic", channelName);
        return mapper.writeValueAsString(msg);
    }

    @Override
    public String getUnsubscribeMessage(String channelName) throws IOException {
        Map<String, String> msg = new HashMap<>();
        msg.put("op", "unsub");
        msg.put("topic", channelName);
        return mapper.writeValueAsString(msg);
    }

    @Override
    protected void handleMessage(JsonNode message) {
        if ("ping".equals(message.get("op").asText())) {
            Map<String, Object> pong = new HashMap<>();
            pong.put("op", "pong");
            pong.put("ts", message.get("ts").asLong());
            try {
                sendMessage(mapper.writeValueAsString(pong));
            } catch (JsonProcessingException e) {
                logger.error("Convert pong message to json failed", e);
            }
            return;
        }
        if (message.has("err-code")) {
            logger.info("Response message: {}", message);
            return;
        }
        super.handleMessage(message);
    }

}
