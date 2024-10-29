using System;
using System.Collections;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Microsoft.Extensions.DependencyInjection;

namespace BinanceUi.Various;

public static class VariousExtensions
{
    public static IObservable<NotifyCollectionChangedEventArgs> ToObservable(this INotifyCollectionChanged collection)
    {
        return Observable
            .FromEvent<NotifyCollectionChangedEventHandler, NotifyCollectionChangedEventArgs>(
                handler => (_, args) => handler(args),
                handler => collection.CollectionChanged += handler,
                handler => collection.CollectionChanged -= handler);
    }

    public static IObservable<bool> LoadedAsObservable(this FrameworkElement element)
    {
        return Observable.FromEventPattern<RoutedEventHandler, RoutedEventArgs>(
                handler => element.Loaded += handler,
                handler => element.Loaded -= handler)
            .Select(t => (t.Sender as FrameworkElement)!.IsLoaded)
            .StartWith(element.IsLoaded);
    }

    public static IObservable<T?> ToObservable<T>(this DependencyObject target, DependencyProperty property)
    {
        return Observable.Create<T>(o =>
        {
            var d = DependencyPropertyDescriptor.FromProperty(property, target.GetType());
            var h = new EventHandler((_, _) => o.OnNext((T)d.GetValue(target)));

            // we trigger it a first time with current value
            var value = (T)d.GetValue(target);
            if (value != null)
                o.OnNext(value);

            d.AddValueChanged(target, h);
            return () => d.RemoveValueChanged(target, h);
        });
    }

    public static T ResolveWith<T>(this IServiceProvider provider, params object[] parameters) where T : class
    {
        return ActivatorUtilities.CreateInstance<T>(provider, parameters);
    }

    public static T? FindVisualAncestor<T>(this DependencyObject dependencyObject) where T : DependencyObject
    {
        DependencyObject? nullableDependencyObject = dependencyObject;

        while (nullableDependencyObject != null && nullableDependencyObject is not T) nullableDependencyObject = VisualTreeHelper.GetParent(nullableDependencyObject);
        return (T?)nullableDependencyObject;
    }

    public static void RemoveFromItemsSource(this ItemsControl itemsControl, object item)
    {
        var disposable = item as IDisposable;
        (itemsControl.ItemsSource as IList)?.Remove(item);
        disposable?.Dispose();
    }
    public static IDisposable DisposeWith(this IDisposable disposable, CompositeDisposable compositeDisposable)
    {
        if (compositeDisposable == null)
            throw new ArgumentNullException(nameof(compositeDisposable));
        if (disposable == null)
            throw new ArgumentNullException(nameof(disposable));

        compositeDisposable.Add(disposable);
        return disposable;
    }
}