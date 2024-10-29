using System.Reactive.Concurrency;
using System.Reactive.Linq;
using BinanceUi.Services;
using BinanceUi.Various;
using Moq;

namespace BinanceUi.Tests.Services;

[Timeout(15_000)]
internal class BinanceWebsocketServiceTests
{
    private BinanceWebsocketService _itemUnderTests;

    [SetUp]
    public void SetUp()
    {
        var schedulerRepository = new SchedulerRepository(ImmediateScheduler.Instance, ImmediateScheduler.Instance);
        _itemUnderTests = new BinanceWebsocketService(schedulerRepository);
    }

    [Test]
    public async Task WhenWeGetMultipleSymbolTicker()
    {
        var taskCompletionSource = new TaskCompletionSource();
        
        _itemUnderTests.GetTicker("BTCUSDT")
            .Take(5)
            .Materialize()
            .Subscribe(notification => Console.WriteLine($"A => {notification}"));

        _itemUnderTests.GetTicker("ETHBTC")
            .Take(5)
            .Materialize()
            .Subscribe(notification => Console.WriteLine($"B => {notification}"), () => taskCompletionSource.SetResult());

        await taskCompletionSource.Task;
    }

    [Test(Description = "We are testing here the logic to un-subscribe when we subscribed multiple times to the same symbol.")]
    public async Task WhenWeGetSameSymbolTickerMultipleTimes()
    {
        var taskCompletionSource = new TaskCompletionSource();
        
        _itemUnderTests.GetTicker("BTCUSDT")
            .Take(1)
            .Materialize()
            .Subscribe(notification => Console.WriteLine($"A => {notification}"));

        _itemUnderTests.GetTicker("BTCUSDT")
            .Take(5)
            .Materialize()
            .Subscribe(notification => Console.WriteLine($"B => {notification}"), () => taskCompletionSource.SetResult());

        await taskCompletionSource.Task;
    }

    [Test(Description = "We are testing here the logic to un-subscribe when we subscribed multiple times to the same symbol.")]
    [Timeout(60_000)]
    public async Task WhenWeSendTooManyRequests()
    {
        // we are getting from Binance Error PolicyViolation - Too many requests
        var task1 = Task.Run(async () => await Loop("BTCUSDT"));
        var task2 = Task.Run(async () => await Loop("ETHUSDT"));
        var task3 = Task.Run(async () => await Loop("BNBUSDT"));
        var task4 = Task.Run(async () => await Loop("ETHBTC"));
        var task5 = Task.Run(async () => await Loop("BNBBTC"));


        await Task.WhenAll(task1, task2, task3, task4, task5);
    }

    private async Task Loop(string symbol)
    {
        for (int i = 0; i < 15; i++)
        {
            var taskCompletionSource = new TaskCompletionSource();

            _itemUnderTests.GetTicker(symbol)
                .Take(1)
                .Materialize()
                .Subscribe(notification => Console.WriteLine($"{notification}"), () => taskCompletionSource.SetResult());

            await taskCompletionSource.Task;
        }
    }

    [Test]
    public async Task GetTicker()
    {
        var result = await _itemUnderTests.GetTicker("btcusdc")
            .Take(3)
            .ToList();

        Assert.That(result.Count, Is.EqualTo(3));
        Assert.That(result[0].Symbol, Is.EqualTo("BTCUSDC"));
    }
}