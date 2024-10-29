using System;
using System.Net.Http;
using System.Reactive.Concurrency;
using BinanceUi.Screens;
using BinanceUi.Screens.OrderBooks;
using BinanceUi.Screens.Tickers;
using BinanceUi.Screens.TradingPairs;
using BinanceUi.Services;
using Microsoft.Extensions.DependencyInjection;

namespace BinanceUi.Various;

public class DependencyInjectionContainer
{
    private readonly App _app;
    private readonly SynchronizationContextScheduler _synchronizationContextScheduler;
    private readonly ServiceProvider _buildServiceProvider;

    public DependencyInjectionContainer(App app, SynchronizationContextScheduler synchronizationContextScheduler)
    {
        _app = app;
        _synchronizationContextScheduler = synchronizationContextScheduler;
        var serviceProvider = new ServiceCollection();

        Register(serviceProvider);

        _buildServiceProvider = serviceProvider.BuildServiceProvider();
    }

    private void Register(ServiceCollection serviceProvider)
    {
        serviceProvider.AddSingleton(_app);
        serviceProvider.AddSingleton<MainWindow>();

        serviceProvider.AddSingleton(_synchronizationContextScheduler);
        serviceProvider.AddSingleton<ImmediateOrDispatcherScheduler>();
        serviceProvider.AddSingleton(t=>new SchedulerRepository(t.GetRequiredService<ImmediateOrDispatcherScheduler>(), t.GetRequiredService<SynchronizationContextScheduler>()));

        serviceProvider.AddSingleton<AppResourceRegistrator>();
        serviceProvider.AddSingleton<HttpClient>();
        serviceProvider.AddSingleton<MainWindowViewModel>();
        serviceProvider.AddSingleton<BinanceService>();
        serviceProvider.AddSingleton<BinanceWebsocketService>();

        serviceProvider.AddTransient<TradingInfoViewModel>();
        serviceProvider.AddTransient<SymbolTickersViewModel>();
        serviceProvider.AddTransient<SymbolTickerViewModel>();

        serviceProvider.AddSingleton<Func<TradingInfoViewModel>>(t => t.GetRequiredService<TradingInfoViewModel>);
        serviceProvider.AddSingleton<Func<SymbolTickersViewModel>>(t => t.GetRequiredService<SymbolTickersViewModel>);

        serviceProvider.AddSingleton<NewPageNavigator>();
        serviceProvider.AddSingleton<Func<SymbolItem, SymbolTickerViewModel>>(provider => a => provider.ResolveWith<SymbolTickerViewModel>(a));

        RegisterOrderBook(serviceProvider);
    }

    private void RegisterOrderBook(ServiceCollection serviceProvider)
    {
        serviceProvider.AddSingleton<Func<OrderBookEntryViewModel.Parameters, OrderBookEntryViewModel>>(provider => a => provider.ResolveWith<OrderBookEntryViewModel>(a));
        serviceProvider.AddSingleton<Func<SymbolItem, OrderBookViewModel>>(provider => a => provider.ResolveWith<OrderBookViewModel>(a, provider.ResolveWith<SymbolTickerViewModel>(a)));
    }

    public MainWindowViewModel GetViewModel() => _buildServiceProvider.GetRequiredService<MainWindowViewModel>();
    public AppResourceRegistrator GetResourceRegistrator() => _buildServiceProvider.GetRequiredService<AppResourceRegistrator>();
    public MainWindow GetMainWindow() => _buildServiceProvider.GetRequiredService<MainWindow>();
}