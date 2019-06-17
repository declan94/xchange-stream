package info.bitrich.xchangestream.hbdm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.hbdm.HbdmAdapters;
import org.knowm.xchange.hbdm.HbdmPrompt;
import org.knowm.xchange.hbdm.dto.market.HbdmDepth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HbdmStreamingMarketService extends HbdmStreamingService implements StreamingMarketDataService {

    private final static Logger logger = LoggerFactory.getLogger(HbdmStreamingMarketService.class);

    private static final String MARKET_API_URI = "wss://www.hbdm.com/ws";

    public HbdmStreamingMarketService(String apiUrl) {
        super(apiUrl);
    }

    public HbdmStreamingMarketService() {
        this(MARKET_API_URI);
    }

    @Override
    protected String getChannelNameFromMessage(JsonNode message) throws IOException {
        return message.has("ch") ? message.get("ch").asText() : "";
    }

    @Override
    public String getSubscribeMessage(String channelName, Object... args) throws IOException {
        Map<String, String> msg = new HashMap<>();
        msg.put("sub", channelName);
        return mapper.writeValueAsString(msg);
    }

    @Override
    public String getUnsubscribeMessage(String channelName) throws IOException {
        return "";
    }

    @Override
    protected void handleMessage(JsonNode message) {
        if (message.has("ping")) {
            Map<String, Long> pong = new HashMap<>();
            pong.put("pong", message.get("ping").asLong());
            try {
                sendMessage(mapper.writeValueAsString(pong));
            } catch (JsonProcessingException e) {
                logger.error("Convert pong message to json failed", e);
            }
            return;
        }
        if (message.has("ts")) {
            for (ObservableEmitter<Long> emitter : delayEmitters) {
                emitter.onNext(System.currentTimeMillis() - message.get("ts").longValue());
            }
        }
        if (message.has("status")) {
            logger.info("Response message: {}", message);
            return;
        }
        super.handleMessage(message);
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        assert args.length > 0;
        assert args[0] instanceof HbdmPrompt;
        String contract = null;
        switch ((HbdmPrompt) args[0]) {
            case QUARTER:
                contract = "CQ";
                break;
            case NEXT_WEEK:
                contract = "NW";
                break;
            case THIS_WEEK:
                contract = "CW";
                break;
        }
        String type = "step6";
        if (args.length > 1) {
            type = (String) args[1];
        }
        String channel = String.format("market.%s_%s.depth.%s", currencyPair.base, contract, type);
        return subscribeChannel(channel).map(jsonNode -> {
            HbdmDepth depth = mapper.treeToValue(jsonNode.get("tick"), HbdmDepth.class);
            return HbdmAdapters.adaptOrderBook(depth, currencyPair);
        });
    }

    @Override
    public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
        throw new NotYetImplementedForExchangeException();
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        throw new NotYetImplementedForExchangeException();
    }

}
