package info.bitrich.xchangestream.hbdm;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.hbdm.HbdmPrompt;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class HbdmExchangeTest {

    @Test
    @Ignore("run mannually")
    public void testOrderBooks() throws InterruptedException {
        ExchangeSpecification exSpec = new HbdmStreamingExchange().getDefaultExchangeSpecification();
        StreamingExchange exchange = StreamingExchangeFactory.INSTANCE.createExchange(exSpec);
        exchange.connect().blockingAwait();
        exchange.getStreamingMarketDataService().getOrderBook(CurrencyPair.BTC_USD, HbdmPrompt.QUARTER)
                .subscribe(orderBook -> {
                    List<BigDecimal> bidsPrices = orderBook.getBids().stream().map(LimitOrder::getLimitPrice)
                            .collect(Collectors.toList());
                    System.out.println("QUARTER");
                    System.out.println(bidsPrices.size());
                    System.out.println(bidsPrices);
                });

        exchange.getStreamingMarketDataService().getOrderBook(CurrencyPair.BTC_USD, HbdmPrompt.THIS_WEEK)
                .subscribe(orderBook -> {
                    List<BigDecimal> bidsPrices = orderBook.getBids().stream().map(LimitOrder::getLimitPrice)
                            .collect(Collectors.toList());
                    System.out.println("THIS_WEEK");
                    System.out.println(bidsPrices.size());
                    System.out.println(bidsPrices);
                });
        for (int i=0; i<100; i++) {
            Thread.sleep(1000);
        }
    }

}
