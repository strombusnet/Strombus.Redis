using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Strombus.Redis
{
    internal static class Extensions
    {
        internal static Task WaitOneAsync(this WaitHandle waitHandle)
        {
            return WaitOneAsync(waitHandle, -1);
        }

        internal static Task WaitOneAsync(this WaitHandle waitHandle, int timeoutMilliseconds)
        {
            if (waitHandle == null)
                throw new ArgumentNullException(nameof(waitHandle));

            var tcs = new TaskCompletionSource<bool>();
            var rwh = ThreadPool.RegisterWaitForSingleObject(waitHandle,
                delegate { tcs.TrySetResult(true); }, null, timeoutMilliseconds, true);
            var t = tcs.Task;
            t.ContinueWith((antecedent) => rwh.Unregister(null));
            return t;
        }

        internal static Task WaitOneAsync(this WaitHandle waitHandle, TimeSpan timeout)
        {
            return WaitOneAsync(waitHandle, (int)timeout.TotalMilliseconds);
        }
    }
}
