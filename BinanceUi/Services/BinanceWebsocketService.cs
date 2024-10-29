using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BinanceUi.Services.OrderBooks;
using BinanceUi.Various;
using ObservableLookup;

namespace BinanceUi.Services;

public class BinanceWebsocketService
{
    // documentation https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
    private readonly Uri _baseUri = new("wss://stream.binance.com:9443/ws");
    
    private readonly ObservableLookup<string, TickerData> _tickerLookup;
    private readonly Subject<SubscriptionMessage> _tickerRequests = new();
    
    private readonly ObservableLookup<string, AggTrade> _aggTradeLookup;
    private readonly Subject<SubscriptionMessage> _aggTradeRequests = new();
    
    private readonly JsonSerializerOptions _serializerOptions = new()
    {
        PropertyNameCaseInsensitive = false
    };

    public BinanceWebsocketService()
    {
        _tickerLookup = GetWebSocketObservable<TickerData>(_tickerRequests)
            .ToObservableLookup(t => t.Symbol, StringComparer.InvariantCultureIgnoreCase);

        _aggTradeLookup = GetWebSocketObservable<AggTrade>(_aggTradeRequests)
            .ToObservableLookup(t => t.Symbol, StringComparer.InvariantCultureIgnoreCase);
    }

    public IObservable<TickerData> GetTicker(string symbol)
    {
        return Observable.Create<TickerData>(t =>
        {
            var compositeDisposable = new CompositeDisposable();

            _tickerLookup[symbol]
                .Subscribe(t)
                .DisposeWith(compositeDisposable);

            _tickerRequests.OnNext(SubscriptionMessage.GetTickerSubscribe(symbol));

            var unsubscribe = Disposable.Create(() => _tickerRequests.OnNext(SubscriptionMessage.GetTickerUnsubscribe(symbol)));
            compositeDisposable.Add(unsubscribe);

            return compositeDisposable;
        });
    }

    public IObservable<AggTrade> GetAggTrades(string symbol)
    {
        return Observable.Create<AggTrade>(t =>
        {
            var compositeDisposable = new CompositeDisposable();

            _aggTradeLookup[symbol]
                .Subscribe(t)
                .DisposeWith(compositeDisposable);

            _aggTradeRequests.OnNext(SubscriptionMessage.GetAggTradeSubscribe(symbol));

            var unsubscribe = Disposable.Create(() => _aggTradeRequests.OnNext(SubscriptionMessage.GetAggTradeUnsubscribe(symbol)));
            compositeDisposable.Add(unsubscribe);

            return compositeDisposable;
        });
    }

    public IObservable<OrderBookSnapshot> GetOrderBookUpdates(string symbol, OrderBookDepth orderBookDepth = OrderBookDepth.D20, OrderBookRefreshSpeed speed = OrderBookRefreshSpeed.S100)
    {
        var orderBookDepthString = orderBookDepth switch
        {
            OrderBookDepth.D5 => 5,
            OrderBookDepth.D10 => 10,
            OrderBookDepth.D20 => 20,
            _ => throw new ArgumentOutOfRangeException(nameof(orderBookDepth), orderBookDepth, "Please add value")
        };

        var speedString = speed switch
        {
            OrderBookRefreshSpeed.S100 => 100,
            OrderBookRefreshSpeed.S1000 => 1000,
            _ => throw new ArgumentOutOfRangeException(nameof(speed), speed, "Please add value")
        };

        var subscriptionMessage = new SubscriptionMessage("SUBSCRIBE", new[] { $"{symbol.ToLowerInvariant()}@depth{orderBookDepthString}@{speedString}ms" }, 0);

        // we cannot share websockets between OrderBookSnapshot streams as there is no way to identify them back.
        return GetWebSocketObservable<OrderBookSnapshot>(Observable.Return(subscriptionMessage));
    }

    private IObservable<T> GetWebSocketObservable<T>(IObservable<SubscriptionMessage> subscriptionRequests)
    {
        return Observable.Create<T>(async (observer, cancellationToken) =>
        {
            var compositeDisposable = new CompositeDisposable();

            var buffer = new byte[1 << 12];

            try
            {
                // ClientWebSocket is fine with calling SendAsync/ReceiveAsync from different thread.
                // As long as we don't call SendAsync concurrently (or ReceiveAsync concurrently).
                var client = new ClientWebSocket();
                compositeDisposable.Add(client);

                var connectAsync = client.ConnectAsync(_baseUri, cancellationToken);

                subscriptionRequests
                    .Synchronize()
                    .IgnorePartialUnsubscribes()
                    .Select(t => JsonSerializer.Serialize(t))
                    // we want to be sure only a single thread access client.SendAsync at same time.
                    // Synchronize + Concat should enforce that.
                    .Select(t => Observable.FromAsync(async () =>
                    {
                        await connectAsync;
                        await client.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(t)), WebSocketMessageType.Text, true, CancellationToken.None);
                    }))
                    .Concat()
                    .Subscribe(_ => { }, exception =>
                    {
                        Debug.WriteLine($"Error {client.CloseStatus} - {client.CloseStatusDescription}");
                        observer.OnError(exception);
                    })
                    .DisposeWith(compositeDisposable);

                await connectAsync;

                while (client.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
                {
                    // ConfigureAwait as we want all message to be process on thread pool.
                    var result = await client.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken).ConfigureAwait(false);

                    switch (result.MessageType)
                    {
                        case WebSocketMessageType.Text:
                            var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                            Debug.WriteLine(message);

                            // that is the subscription reply. Not that robust code, but that should do
                            if (message == "{\"result\":null,\"id\":0}") continue;

                            var item = JsonSerializer.Deserialize<T>(message, _serializerOptions);
                            observer.OnNext(item!);
                            break;

                        case WebSocketMessageType.Close:
                            await client.CloseAsync(WebSocketCloseStatus.NormalClosure, "", cancellationToken);
                            observer.OnCompleted();
                            break;
                    }
                }
            }
            catch (TaskCanceledException)
            {
                // RX subscription has been disposed
            }
            catch (Exception ex)
            {
                observer.OnError(ex);
            }

            return compositeDisposable;
        });
    }
}

public enum OrderBookDepth
{
    D5,
    D10,
    D20
}

public enum OrderBookRefreshSpeed
{
    S100,
    S1000
}

internal static class BinanceWebsocketServiceExtensions
{
    public static IObservable<SubscriptionMessage> IgnorePartialUnsubscribes(this IObservable<SubscriptionMessage> input)
    {
        return input.Scan((dictionary: new Dictionary<string, int>(), message: (SubscriptionMessage?)null, skipNext: false), (a, b) =>
            {
                // when subscribing multiple to the same symbol, binance keep only a single subscription. We need to keep track of  how many subscription we got.
                var dico = a.dictionary;
                var newMessage = b;

                var key = newMessage.GetKey();

                if (newMessage.IsUnsubscribe())
                {
                    var subscriptions = --dico[key];

                    return (dico, newMessage, subscriptions != 0);
                }

                if (!dico.ContainsKey(key))
                    dico[key] = 0;

                dico[key]++;
                return (a.Item1, b, false);
            })
            .Where(t => !t.skipNext)
            .Select(t => t.message ?? throw new Exception("Not reachable"));
    }
}