using System.Reactive.Concurrency;

namespace BinanceUi.Various;

public class SchedulerRepository
{
    public const string ResourceName = nameof(SchedulerRepository);

    public SchedulerRepository(IScheduler immediateOrDispatcherScheduler, IScheduler synchronizationContextScheduler)
    {
        ImmediateOrDispatcherScheduler = immediateOrDispatcherScheduler;
        SynchronizationContextScheduler = synchronizationContextScheduler;
    }

    public IScheduler ImmediateOrDispatcherScheduler { get; }
    public IScheduler SynchronizationContextScheduler { get; }
}