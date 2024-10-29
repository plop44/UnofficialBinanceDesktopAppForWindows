using System;
using System.Reactive.Concurrency;
using System.Windows;
using System.Windows.Threading;

namespace BinanceUi.Various;

/// <summary>
/// This scheduler is for the scenario where we want to execute some code in the UI thread
/// BUT if we are already there, we don't want to be dispatched at the end of the "pending to dispatch" queue.
/// </summary>
public class ImmediateOrDispatcherScheduler : IScheduler
{
    private readonly SynchronizationContextScheduler _synchronizationContextScheduler;

    // passed SynchronizationContextScheduler should be the one from Main thread.
    public ImmediateOrDispatcherScheduler(SynchronizationContextScheduler synchronizationContextScheduler)
    {
        _synchronizationContextScheduler = synchronizationContextScheduler;
    }

    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<IScheduler, TState, IDisposable> action)
    {
        return Dispatcher.CurrentDispatcher == Application.Current.Dispatcher
            ? Scheduler.Immediate.Schedule(state, dueTime, action)
            : _synchronizationContextScheduler.Schedule(state, dueTime, action);
    }

    public DateTimeOffset Now => Scheduler.Immediate.Now;

    public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
    {
        return Dispatcher.CurrentDispatcher == Application.Current?.Dispatcher
            ? Scheduler.Immediate.Schedule(state, action)
            : _synchronizationContextScheduler.Schedule(state, action);
    }

    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action)
    {
        return Dispatcher.CurrentDispatcher == Application.Current.Dispatcher
            ? Scheduler.Immediate.Schedule(state, dueTime, action)
            : _synchronizationContextScheduler.Schedule(state, dueTime, action);
    }
}