package info.bitrich.xchangestream.hbdm;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Completable;
import org.knowm.xchange.hbdm.HbdmExchange;

public class HbdmStreamingExchange extends HbdmExchange implements StreamingExchange {

    private static final String MARKET_API_URI = "wss://www.hbdm.com/ws";

    private static final String TRADE_API_URI = "wss://api.hbdm.com/notification";

    private HbdmStreamingTradeService streamingTradeService;

    private HbdmStreamingMarketService streamingMarketService;

    @Override
    protected void initServices() {
        super.initServices();
        streamingMarketService = new HbdmStreamingMarketService(MARKET_API_URI);
        streamingTradeService = new HbdmStreamingTradeService(TRADE_API_URI);
    }

    @Override
    public Completable connect(ProductSubscription... args) {
        Completable conn = streamingMarketService.connect();
        if (exchangeSpecification.getApiKey() == null) {
            return conn;
        }
        return conn.andThen(streamingTradeService.connect());
    }

    @Override
    public Completable disconnect() {
        return null;
    }

    @Override
    public boolean isAlive() {
        return streamingMarketService.isSocketOpen() &&
                (exchangeSpecification.getApiKey() == null || streamingTradeService.isSocketOpen());
    }

    @Override
    public StreamingMarketDataService getStreamingMarketDataService() {
        return streamingMarketService;
    }

    @Override
    public void useCompressedMessages(boolean compressedMessages) {

    }

}
