package info.bitrich.xchangestream.hbdm;

import org.junit.Ignore;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.hbdm.HbdmPrompt;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class HbdmStreamingServiceTest {

    private final HbdmTestProperties hbdmTestProperties = new HbdmTestProperties();

    @Test
    @Ignore("run mannually")
    public void testMarketStreamingService() throws InterruptedException {
        HbdmStreamingService streamingService = new HbdmStreamingMarketService("wss://www.hbdm.com/ws");
        streamingService.connect().blockingAwait();
//        streamingService.subscribeChannel("market.BTC_CQ.kline.1min").subscribe(System.out::println);
        ((HbdmStreamingMarketService) streamingService).getOrderBook(CurrencyPair.BTC_USD, HbdmPrompt.THIS_WEEK, "step6")
                .subscribe(orderBook -> {
                    List<BigDecimal> bidsPrices = orderBook.getBids().stream().map(LimitOrder::getLimitPrice)
                            .collect(Collectors.toList());
                    System.out.println(bidsPrices.size());
                    System.out.println(bidsPrices);
                });
        for (int i=0; i<100; i++) {
            Thread.sleep(1000);
        }
    }

    @Test
    @Ignore("run mannually")
    public void testTradingStreamingService() throws InterruptedException {
        assert hbdmTestProperties.isValid();
        HbdmStreamingTradeService streamingService = new HbdmStreamingTradeService("wss://api.hbdm.com/notification");
        ExchangeSpecification exchangeSpecification = (new HbdmStreamingExchange()).getDefaultExchangeSpecification();
        exchangeSpecification.setApiKey(hbdmTestProperties.getApiKey());
        exchangeSpecification.setSecretKey(hbdmTestProperties.getSecretKey());
        streamingService.setExchangeSpecification(exchangeSpecification);
        streamingService.connect().blockingAwait();
        streamingService.subscribeChannel("orders.btc").subscribe(System.out::println);
        for (;;) {
            Thread.sleep(1000);
        }
    }

}
