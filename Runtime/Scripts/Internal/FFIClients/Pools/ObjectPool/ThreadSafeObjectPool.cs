using System;
using System.Collections.Concurrent;

namespace LiveKit.Internal.FFIClients.Pools.ObjectPool
{
    public class ThreadSafeObjectPool<T> where T : class
    {
        private readonly ConcurrentBag<T> _objects = new ConcurrentBag<T>();
        private readonly Func<T> _objectGenerator;
        private readonly Action<T>? _actionOnRelease;

        public ThreadSafeObjectPool(Func<T> objectGenerator, Action<T>? actionOnRelease = null)
        {
            _objectGenerator = objectGenerator ?? throw new ArgumentNullException(nameof(objectGenerator));
            _actionOnRelease = actionOnRelease;
        }

        public T Get()
        {
            if (_objects.TryTake(out T item)) 
                return item;
                
            return _objectGenerator();
        }

        public void Release(T item)
        {
            _actionOnRelease?.Invoke(item);
            _objects.Add(item);
        }
    }
}