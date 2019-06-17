package info.bitrich.xchangestream.hbdm;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.netty.channel.ChannelHandlerContext;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.knowm.xchange.hbdm.HbdmExchange;

public class HbdmStreamingExchange extends HbdmExchange implements StreamingExchange {

    protected HbdmStreamingTradeService streamingTradeService;

    private HbdmStreamingMarketService streamingMarketService;

    @Override
    protected void initServices() {
        super.initServices();
        streamingMarketService = new HbdmStreamingMarketService();
        streamingTradeService = new HbdmStreamingTradeService();
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
        Completable disconn = streamingMarketService.disconnect();
        if (exchangeSpecification.getApiKey() == null) {
            return disconn;
        }
        return disconn.andThen(streamingTradeService.disconnect());
    }

    @Override
    public Observable<ChannelHandlerContext> disconnectObservable() {
        Observable<ChannelHandlerContext> observable = streamingMarketService.subscribeDisconnect();
        if (exchangeSpecification.getApiKey() == null) {
            return observable;
        }
        return observable.mergeWith(streamingTradeService.subscribeDisconnect());
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

    @Override
    public Observable<Long> messageDelay() {
        return Observable.create(streamingMarketService::addDelayEmitter);
    }
}
