using Strombus.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections;

namespace Strombus.Redis
{
    public class InvalidTypeException : Exception
    {
        public string TypeName { get; }

        public InvalidTypeException(string message, string typeName) : base(message)
        {
            this.TypeName = typeName;
        }
    }

    public class RedisException: Exception
    {
        public string Prefix { get; }
        
        public RedisException(string message, string prefix) : base(message)
        {
            this.Prefix = prefix;
        }

        public RedisException(RedisError redisError) : base(redisError.Message)
        {
            this.Prefix = redisError.Prefix;
        }
    }

    public class RedisCriticalErrorEventArgs
    {
        public string Message = "";
    }

    public class RedisClient : IDisposable
    {
        TcpClient _tcpClient = null;
        NetworkStream _tcpStream = null;
        System.Text.Encoding _encoding;

        Task _pipelineIncomingMessagesTask = null;
        Task _pipelineOutgoingMessagesTask = null;
        CancellationTokenSource _pipelineCancellationTokenSource = null;
        AutoResetEvent _pipelineIncomingMessagesWaitHandle = null;
        AutoResetEvent _pipelineOutgoingMessagesWaitHandle = null;
        bool _pipelineShutdownInProcess = false;
        bool _pipelineEnabled = false;

        // a structure which we use to store return values from REDIS
        class RedisCommandInfo
        {
            public byte[] Query;
            public object Response;
            public RedisException Exception;
            public readonly System.Threading.ManualResetEvent ResponseReceivedWaitHandle;

            public RedisCommandInfo(byte[] query)
            {
                this.Query = query;
                this.Response = null;
                this.Exception = null;
                this.ResponseReceivedWaitHandle = new System.Threading.ManualResetEvent(false);
            }

            public void SetResponse(object response)
            {
                this.Response = response;
                this.ResponseReceivedWaitHandle.Set();
            }

            public void SetError(RedisError error)
            {
                this.Exception = new RedisException(error);
                this.ResponseReceivedWaitHandle.Set();
            }
        }
        // the queue we use to handle outgoing queries to the redis server (for pipelining support)
        System.Collections.Queue _redisCommandQueryInfoQueue;
        // the queue we use to handle incoming query replies from the redis server (for pipelining support)
        System.Collections.Queue _redisCommandResponseInfoQueue;
        // the lock we use to ensure the order of our outgoing requests matches our incoming responses
        // NOTE: we want to use this lock inside of asynchronous functions, so we need to be a little more hands-on than monitors (by using WaitHandles instead of lock objects)
        AutoResetEvent _redisConnectionLockWaitHandle;

        bool _isDisposed = false;

        public event EventHandler<RedisCriticalErrorEventArgs> RedisCriticalError;

        public RedisClient()
        {
            _encoding = Constants.SIMPLE_REDIS_ENCODING;
            _redisCommandQueryInfoQueue = new System.Collections.Queue();
            _redisCommandResponseInfoQueue = new System.Collections.Queue();
            _redisConnectionLockWaitHandle = new AutoResetEvent(true); // set to true so that it can be used
        }

        public async Task ConnectAsync(string host)
        {
            if (_isDisposed)
                return;

            _tcpClient = new TcpClient();
            // NOTE: consider establishing a timeout max for connect...and return true/false (i.e. false if we could not connect) */
            await _tcpClient.ConnectAsync(host, 6379).ConfigureAwait(false);

            // create stream
            _tcpStream = _tcpClient.GetStream();
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            // set _isDisposed to true before we start so that any cancellation exceptions from disposed/cancelled objects understand that the exception was expected
            _isDisposed = true;

            // cancel incoming/outgoing message processing
            if (_pipelineCancellationTokenSource != null)
                _pipelineCancellationTokenSource.Cancel();

            // unblock pipelining queues
            _pipelineOutgoingMessagesWaitHandle.Dispose();
            _pipelineIncomingMessagesWaitHandle.Dispose();

            // close tcp stream
            if (_tcpStream != null)
            {
                _tcpStream.Dispose();
                _tcpStream = null;
            }
            // close tcp client
            if (_tcpClient != null)
            {
                _tcpClient.Dispose();
                _tcpClient = null;
            }
        }

        public System.Text.Encoding Encoding
        {
            set
            {
                _encoding = value;
            }
            get
            {
                return _encoding;
            }
        }

        public async Task EnablePipelineAsync()
        {
            // lock the redis connection so that pipelining doesn't get enabled in the middle of a transmission: when this function returns any/all blocked commands will be pipelined.
            await _redisConnectionLockWaitHandle.WaitOneAsync().ConfigureAwait(false);
            try
            {
                if (_pipelineEnabled)
                    return;

                _pipelineCancellationTokenSource = new CancellationTokenSource();
                _pipelineOutgoingMessagesWaitHandle = new AutoResetEvent(false);
                _pipelineOutgoingMessagesTask = Task.Run(() => { ProcessOutgoingMessagesTask(); }, _pipelineCancellationTokenSource.Token);
                _pipelineIncomingMessagesWaitHandle = new AutoResetEvent(false);
                _pipelineIncomingMessagesTask = Task.Run(() => { ProcessIncomingMessagesTask(); }, _pipelineCancellationTokenSource.Token);

                _pipelineEnabled = true;
            }
            finally
            {
                _redisConnectionLockWaitHandle.Set();
            }
        }

        public async Task DisablePipelineAsync()
        {
            // lock the redis connection so that new messages are blocked while we are shutting down
            await _redisConnectionLockWaitHandle.WaitOneAsync().ConfigureAwait(false);
            try
            {
                if (!_pipelineEnabled)
                    return;

                _pipelineShutdownInProcess = true;

                // set cancellation notice and then unblock queue wait handles
                _pipelineCancellationTokenSource.Cancel();
                _pipelineIncomingMessagesWaitHandle.Set();
                _pipelineOutgoingMessagesWaitHandle.Set();
                // wait until any outstanding queries are sent.
                await Task.Run(() => _pipelineOutgoingMessagesTask.Wait(_pipelineCancellationTokenSource.Token));
                // wait until all outstanding replies are processed.
                await Task.Run(() => _pipelineIncomingMessagesTask.Wait(_pipelineCancellationTokenSource.Token));
                try
                {
                    await Task.Run(() => Task.WaitAll(new Task[] { _pipelineIncomingMessagesTask, _pipelineOutgoingMessagesTask }, _pipelineCancellationTokenSource.Token));
                }
                catch (OperationCanceledException)
                {
                    // this exception is a normal result of our cancellation request; proceed.
                }
                _pipelineIncomingMessagesTask = null;
                _pipelineOutgoingMessagesTask = null;

                _pipelineEnabled = false;
                _pipelineShutdownInProcess = false;
            }
            finally
            {
                _redisConnectionLockWaitHandle.Set();
            }
        }

        void ProcessIncomingMessagesTask()
        {
            RespParser parser = new RespParser();
            byte[] buffer = new byte[256];
            object response;
            int bytesRead;

            while (true)
            {
                try
                {
                    bytesRead = _tcpStream.Read(buffer, 0, buffer.Length);
                }
                catch (System.IO.IOException ex)
                {
                    // if we've been (or are being) disposed, then this exception is expected; simply return
                    SocketException socketException = ex.InnerException as SocketException;
                    if ((socketException != null) && (socketException.SocketErrorCode == SocketError.Interrupted) && (_isDisposed == true))
                    {
                        return;
                    }

                    // otherwise, we should attempt to reconnect, etc.

                    // NOTE: for now, we simply set bytesRead to zero so that our loop continues
                    bytesRead = 0; // or continue;
                }
                if (bytesRead > 0)
                {
                    parser.Append(buffer, 0, bytesRead);

                    if (parser.TryReadValue(out response))
                    {
                        if (_redisCommandResponseInfoQueue.Count == 0)
                        {
                            // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                            string failMessage = "*** CRITICAL ERROR *** Unexpected Redis message: " + response.ToString();
                            RaiseCriticalError(failMessage);
                        }
                        RedisCommandInfo info = (RedisCommandInfo)_redisCommandResponseInfoQueue.Dequeue();
                        if (response != null && response.GetType().Equals(typeof(RedisError)))
                        {
                            info.SetError((RedisError)response);
                        }
                        else
                        {
                            info.SetResponse(response);
                        }
                    }
                }
                else
                {
                    break;
                }

                if (_redisCommandResponseInfoQueue.Count == 0)
                {
                    // if our incoming message queue is empty and pipelining is being shut down, cancel this task.
                    _pipelineCancellationTokenSource.Token.ThrowIfCancellationRequested();
                }
            }
        }

        void ProcessOutgoingMessagesTask()
        {
            while(true)
            {
                // wait for a new outgoing message
                _pipelineOutgoingMessagesWaitHandle.WaitOne();
                // if pipelining is being shut down, cancel this task.
                _pipelineCancellationTokenSource.Token.ThrowIfCancellationRequested();

                while (_redisCommandQueryInfoQueue.Count > 0)
                {
                    // dequeue the outgoing query 
                    RedisCommandInfo info = (RedisCommandInfo)_redisCommandQueryInfoQueue.Dequeue();
                    // queue the query into the pending response queue
                    _redisCommandResponseInfoQueue.Enqueue(info);
                    // transmit query
                    _tcpStream.Write(info.Query, 0, info.Query.Length);

                    if (_redisCommandQueryInfoQueue.Count == 0)
                    {
                        // if our outgoing message queue is empty and pipelining is being shut down, cancel this task.
                        _pipelineCancellationTokenSource.Token.ThrowIfCancellationRequested();
                    }
                }
            }
        }

        void RaiseCriticalError(string failMessage)
        {
            Debug.Fail(failMessage);
            RedisCriticalError?.Invoke(this, new RedisCriticalErrorEventArgs() { Message = failMessage });
        }

        // this function sends a query, waits for a response, and then returns that response
        async Task<object> SendQueryAndWaitForResponseAsync(byte[] query)
        {
            // if we are currently shutting down the pipeline, hold the new query until shutdown is complete.
            while (_pipelineShutdownInProcess)
            {
                await Task.Delay(10).ConfigureAwait(false);
            }

            if (_pipelineEnabled)
            {
                RedisCommandInfo info = new RedisCommandInfo(query);
                _redisCommandQueryInfoQueue.Enqueue(info);
                if (info == null)
                {
                    Debug.WriteLine("CRITICAL ERROR!");
                }
                _pipelineOutgoingMessagesWaitHandle.Set();
                // wait for the response to be received (and release our thread in the process)
                await info.ResponseReceivedWaitHandle.WaitOneAsync().ConfigureAwait(false);
                // process the response (or error)
                if (info.Exception != null)
                {
                    throw info.Exception;
                }
                else
                {
                    return info.Response;
                }
            }
            else
            {
                _redisConnectionLockWaitHandle.WaitOne();
                try
                {
                    // transmit query
                    await _tcpStream.WriteAsync(query, 0, query.Length).ConfigureAwait(false);

                    // wait for response
                    RespParser parser = new RespParser();
                    byte[] buffer = new byte[32];
                    object response;
                    while (true)
                    {
                        int bytesRead = await _tcpStream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
                        if (bytesRead > 0)
                        {
                            parser.Append(buffer, 0, bytesRead);

                            if (parser.TryReadValue(out response))
                            {
                                if (response != null && response.GetType().Equals(typeof(RedisError)))
                                {
                                    throw new RedisException((RedisError)response);
                                }
                                else
                                {
                                    return response;
                                }
                            }
                        }
                    }
                }
                finally
                {
                    _redisConnectionLockWaitHandle.Set();
                }
            }
        }

        byte[] Convert_String_OrByteArray_ToByteArray<T>(string nameOfType, T value)
        {
            // validate type (and encode value as byte array, if required)
            if (value.GetType().Equals(typeof(byte[])))
            {
                return (byte[])(object)value;
            }
            else if (value.GetType().Equals(typeof(string)))
            {
                return _encoding.GetBytes((string)(object)value);
            }
            else
            {
                throw new InvalidTypeException(string.Format("{0} must be byte[] or string.", nameOfType.ToString()), typeof(T).Name);
            }
        }

        byte[][] Convert_StringArray_OrByteArrayArray_ToByteArrayArray<T>(string nameOfType, T[] values)
        {
            // validate type (and encode values as byte arrays, if required)
            if (values != null)
            {
                byte[][] valuesAsByteArrayArray = new byte[values.Length][];
                for (int iValue = 0; iValue < values.Length; iValue++)
                {
                    if (values[iValue].GetType().Equals(typeof(byte[])))
                    {
                        valuesAsByteArrayArray[iValue] = (byte[])(object)values[iValue];
                    }
                    else if (values[iValue].GetType().Equals(typeof(string)))
                    {
                        valuesAsByteArrayArray[iValue] = _encoding.GetBytes((string)(object)values[iValue]);
                    }
                    else
                    {
                        throw new InvalidTypeException(string.Format("{0} must be byte[] or string.", nameOfType.ToString()), typeof(T).Name);
                    }
                }

                return valuesAsByteArrayArray;
            }
            else
            {
                if (typeof(T).Equals(typeof(byte[])) || typeof(T).Equals(typeof(string)))
                {
                    return new byte[0][];
                }
                else
                {
                    throw new InvalidTypeException(string.Format("{0} must be byte[] or string.", nameOfType.ToString()), typeof(T).Name);
                }
            }
        }

        bool ValidateSuccessForReturn(object value)
        {
            if (value.GetType().Equals(typeof(long)))
            {
                return ((long)value > 0);
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + value.ToString();
                RaiseCriticalError(failMessage);
                // return the error value (a reasonable response)
                return false;
            }
        }

        TReturn ValidateReturnType_OrRaiseExceptionOnMismatch<TReturn>(string nameOfType, object value)
        {
            // validate type (and cast to return type)
            if (value.GetType().Equals(typeof(TReturn)))
            {
                return (TReturn)value;
            }
            else
            {
                throw new InvalidTypeException(string.Format("{0} does not match type of return value ( " + value.GetType().ToString() + " ).", nameOfType.ToString()), typeof(TReturn).Name);
            }
        }

        TReturn ValidateReturnType<TReturn>(object value, TReturn errorValue)
        {
            // validate type (and cast to return type)
            if (value.GetType().Equals(typeof(TReturn)))
            {
                return (TReturn)value;
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + value.ToString();
                RaiseCriticalError(failMessage);
                // return the error value (a reasonable response)
                return errorValue;
            }
        }

        #region Command Group: Lists

        public async Task<KeyValuePair<TKey, TValue>> ListPopLeftBlockingAsync<TKey, TValue>(TKey[] keys, long? timeoutInSeconds)
        {
            // validate key type (and encode keys as byte arrays, if required)
            byte[][] keysAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TKey), keys);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("BRPOP");
            foreach (byte[] keyAsByteArray in keysAsByteArrayArray)
            {
                builder.AppendRespBulkString(keyAsByteArray);
            }
            if (!timeoutInSeconds.HasValue)
                builder = builder.AppendRespBulkString("0");
            else
                builder = builder.AppendRespBulkString(timeoutInSeconds.Value.ToString());
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response == null || response.GetType().Equals(typeof(object[])))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)(null));
            }

            // return the data in the requested type
            if (response == null)
            {
                return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)null);
            }
            else if (response.GetType().Equals(typeof(object[])))
            {
                object[] responseAsArray = ((object[])response);
                // add the data in the requested type
                TKey key;
                TValue value;
                if (responseAsArray[0].GetType().Equals(typeof(byte[])))
                {
                    if (typeof(TKey).Equals(typeof(byte[])))
                    {
                        key = (TKey)(object)responseAsArray[0];
                    }
                    else if (typeof(TKey).Equals(typeof(string)))
                    {
                        key = (TKey)(object)_encoding.GetString((byte[])responseAsArray[0]);
                    }
                    else
                    {
                        throw new InvalidTypeException("TKey must be byte[] or string.", typeof(TKey).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response value: " + responseAsArray[1].ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)(null));
                }

                if (responseAsArray[1].GetType().Equals(typeof(byte[])))
                {
                    if (typeof(TValue).Equals(typeof(byte[])))
                    {
                        value = (TValue)(object)responseAsArray[1];
                    }
                    else if (typeof(TValue).Equals(typeof(string)))
                    {
                        value = (TValue)(object)_encoding.GetString((byte[])responseAsArray[1]);
                    }
                    else
                    {
                        throw new InvalidTypeException("TValue must be byte[] or string.", typeof(TValue).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response value: " + responseAsArray[1].ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)(null));
                }
                return new KeyValuePair<TKey, TValue>(key, value);
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)null);
            }
        }

        public async Task<KeyValuePair<TKey, TValue>> ListPopRightBlockingAsync<TKey, TValue>(TKey[] keys, long? timeoutInSeconds)
        {
            // validate key type (and encode keys as byte arrays, if required)
            byte[][] keysAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TKey), keys);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("BLPOP");
            foreach (byte[] keyAsByteArray in keysAsByteArrayArray)
            {
                builder.AppendRespBulkString(keyAsByteArray);
            }
            if (!timeoutInSeconds.HasValue)
                builder = builder.AppendRespBulkString("0");
            else
                builder = builder.AppendRespBulkString(timeoutInSeconds.Value.ToString());
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response == null || response.GetType().Equals(typeof(object[])))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)(null));
            }

            // return the data in the requested type
            if (response == null)
            {
                return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)null);
            }
            else if (response.GetType().Equals(typeof(object[])))
            {
                object[] responseAsArray = ((object[])response);
                // add the data in the requested type
                TKey key;
                TValue value;
                if (responseAsArray[0].GetType().Equals(typeof(byte[])))
                {
                    if (typeof(TKey).Equals(typeof(byte[])))
                    {
                        key = (TKey)(object)responseAsArray[0];
                    }
                    else if (typeof(TKey).Equals(typeof(string)))
                    {
                        key = (TKey)(object)_encoding.GetString((byte[])responseAsArray[0]);
                    }
                    else
                    {
                        throw new InvalidTypeException("TKey must be byte[] or string.", typeof(TKey).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response value: " + responseAsArray[1].ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)(null));
                }

                if (responseAsArray[1].GetType().Equals(typeof(byte[])))
                {
                    if (typeof(TValue).Equals(typeof(byte[])))
                    {
                        value = (TValue)(object)responseAsArray[1];
                    }
                    else if (typeof(TValue).Equals(typeof(string)))
                    {
                        value = (TValue)(object)_encoding.GetString((byte[])responseAsArray[1]);
                    }
                    else
                    {
                        throw new InvalidTypeException("TValue must be byte[] or string.", typeof(TValue).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response value: " + responseAsArray[1].ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)(null));
                }
                return new KeyValuePair<TKey, TValue>(key, value);
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return new KeyValuePair<TKey, TValue>((TKey)(object)null, (TValue)(object)null);
            }
        }

        public async Task<long> ListPushLeftAsync<TKey, TValue>(TKey key, TValue[] values)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate value type (and encode as a byte array, if required)
            byte[][] valuesAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TValue), values);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("LPUSH").AppendRespBulkString(keyAsByteArray);
            foreach (byte[] value in valuesAsByteArrayArray)
            {
                builder = builder.AppendRespBulkString(value);
            }
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateReturnType<long>(response, 0);
        }

        public async Task<long> ListPushRightAsync<TKey, TValue>(TKey key, TValue[] values)
        {
            // validate key type (and encode keys as byte arrays, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate value type (and encode values as byte arrays, if required)
            byte[][] valuesAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TValue), values);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("RPUSH").AppendRespBulkString(keyAsByteArray);
            foreach (byte[] value in valuesAsByteArrayArray)
            {
                builder = builder.AppendRespBulkString(value);
            }
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateReturnType<long>(response, 0);
        }

        public async Task<TValue[]> ListRangeAsync<TKey, TValue>(TKey key, long start, long stop)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("LRANGE").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(start.ToString()).AppendRespBulkString(stop.ToString());
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response.GetType().Equals(typeof(object[])))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return new TValue[] { };
            }

            // we have one or more values; populate them into a list and then return the list
            List<TValue> returnValues = new List<TValue>();
            foreach (object responseItem in (object[])response)
            {
                // add the data in the requested type
                if (responseItem.GetType().Equals(typeof(byte[])) || typeof(TValue).Equals(typeof(object)))
                {
                    if (typeof(TValue).Equals(typeof(byte[])))
                    {
                        returnValues.Add((TValue)responseItem);
                    }
                    else if (typeof(TValue).Equals(typeof(string)))
                    {
                        returnValues.Add((TValue)(object)_encoding.GetString((byte[])responseItem));
                    }
                    else
                    {
                        throw new InvalidTypeException("Return type must be byte[] or string.", typeof(TValue).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response array item: " + responseItem.ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return new TValue[] { };
                }
            }
            return returnValues.ToArray();
        }

        #endregion Command Group: Lists

        #region Command Group: Scripts

        public async Task<TReturn> EvalAsync<TKey, TParam, TReturn>(string script, TKey[] keys, TParam[] parameters)
        {
            // validate key type (and encode keys as byte arrays, if required)
            byte[][] keysAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TKey), keys);
            // validate parameter type (and encode parameters as byte arrays, if required)
            byte[][] parametersAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TParam), parameters);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("EVAL").AppendRespBulkString(script);
            builder.AppendRespBulkString(keysAsByteArrayArray.Length.ToString());
            foreach (byte[] key in keysAsByteArrayArray)
            {
                builder = builder.AppendRespBulkString(key);
            }
            foreach (byte[] parameter in parametersAsByteArrayArray)
            {
                builder = builder.AppendRespBulkString(parameter);
            }
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            if (typeof(TReturn).Equals(typeof(object)))
            {
                // if the return type is object, return any response.
                return (TReturn)response;
            }
            else if (typeof(TReturn).Equals(typeof(string)) && response == null)
            {
                return (TReturn)(object)null;
            }
            else if (typeof(TReturn).Equals(typeof(string)) && response.GetType().Equals(typeof(byte[])))
            {
                // if the return value is a byte array and the caller is expecting a string, convert the response to a string.
                return (TReturn)(object)_encoding.GetString((byte[])response);
            }
            else
            {
                // for all other cases, validate the return type.
                return ValidateReturnType_OrRaiseExceptionOnMismatch<TReturn>(nameof(TReturn), response);
            }
        }

        #endregion Command Group: Scripts

        #region Command Group: Common

        public async Task<long> DelAsync<TKey>(params TKey[] keys)
        {
            // validate key type (and encode keys as byte arrays, if required)
            byte[][] keysAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TKey), keys);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("DEL");
            foreach (byte[] keyAsByteArray in keysAsByteArrayArray)
            {
                builder.AppendRespBulkString(keyAsByteArray);
            }
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response.GetType().Equals(typeof(long)))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return -1;
            }

            return (long)response;
        }

        public async Task<long> ExistsAsync<TKey>(params TKey[] keys)
        {
            // validate key type (and encode keys as byte arrays, if required)
            byte[][] keysAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TKey), keys);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("EXISTS");
            foreach (byte[] keyAsByteArray in keysAsByteArrayArray)
            {
                builder.AppendRespBulkString(keyAsByteArray);
            }
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            return ValidateReturnType<long>(response, 0);
        }

        public async Task<bool> ExpireAsync<TKey>(TKey key, long expireInSeconds)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("EXPIRE").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(expireInSeconds.ToString());
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateSuccessForReturn(response);
        }

        public async Task<bool> ExpireAtAsync<TKey>(TKey key, long expireAtUnixTime)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate that expireAtUnixTime is not negative
            if (expireAtUnixTime < 0)
            {
                throw new ArgumentOutOfRangeException("expieAtUnixTime must be a non-negative value.", nameof(expireAtUnixTime));
            }

            // build request
            var builder = new RespBuilder().AppendRespBulkString("EXPIREAT").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(expireAtUnixTime.ToString());
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateSuccessForReturn(response);
        }

        public async Task<TValue> GetAsync<TKey, TValue>(TKey key)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("GET").AppendRespBulkString(keyAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response == null || response.GetType().Equals(typeof(byte[])))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return (TValue)(object)null;
            }

            // return the data in the requested type
            if (typeof(TValue).Equals(typeof(byte[])) || typeof(TValue).Equals(typeof(object)))
            {
                return (TValue)response;
            }
            else if (typeof(TValue).Equals(typeof(string)))
            {
                if (response == null)
                    return (TValue)(object)null;
                else
                    return (TValue)(object)_encoding.GetString((byte[])response);
            }
            else
            {
                throw new InvalidTypeException("Return type must be byte[] or string.", typeof(TValue).Name);
            }
        }

        public async Task<long> PttlAsync<TKey>(TKey key)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("PTTL").AppendRespBulkString(keyAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return (long)response;
        }

        // returns: success ("OK" response) or failure (null response)
        public async Task<bool> SelectAsync(long index)
        {
            // build request
            var builder = new RespBuilder().AppendRespBulkString("SELECT").AppendRespBulkString(index.ToString());
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            if (response != null && response.GetType().Equals(typeof(string)) && (string)response == "OK")
            {
                return true;
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return false;
            }
        }

        public async Task<bool> SetAsync<TKey, TValue>(TKey key, TValue value)
        {
            return await SetAsync<TKey, TValue>(key, value, -1, false, false).ConfigureAwait(false);
        }

        public async Task<bool> SetAsync<TKey, TValue>(TKey key, TValue value, long expiresMilliseconds)
        {
            return await SetAsync<TKey, TValue>(key, value, expiresMilliseconds, false, false).ConfigureAwait(false);
        }

        // description: Set 'key' to hold the string 'value'.  If 'key' already holds a value, it is overwritten, regardless of its type.
        // TKey: byte[] or string
        // TValue: byte[] or string or long
        // key: key
        // value: value
        // expiresMilliseconds: expiration time relative to now, in milliseconds
        // onlySetIfNotExists: only set the key if it does not already exist
        // onlySetIfExists: only set the key if it already exists
        // returns: success ("OK" response) or failure (null response)
        public async Task<bool> SetAsync<TKey, TValue>(TKey key, TValue value, long expiresMilliseconds, bool onlySetIfNotExists, bool onlySetIfExists)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate that value is not null
            if (value == null)
            {
                throw new ArgumentNullException("Value may not be null.", nameof(value));
            }
            // validate value type (and encode as a byte array, if required)
            byte[] valueAsByteArray = null;
            if (typeof(TValue).Equals(typeof(byte[])))
            {
                valueAsByteArray = value as byte[];
            }
            else if (typeof(TValue).Equals(typeof(string)))
            {
                valueAsByteArray = _encoding.GetBytes(value as string);
            }
            else if (typeof(TValue).Equals(typeof(long)))
            {
                valueAsByteArray = _encoding.GetBytes(value.ToString());
            }
            else
            {
                throw new InvalidTypeException("Value type must be byte[] or string or long.", typeof(TValue).Name);
            }

            // build request
            var builder = new RespBuilder().AppendRespBulkString("SET").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(valueAsByteArray);
            if (expiresMilliseconds >= 0) /* TODO: determine if "zero" is a valid timeout, or if it needs to be >= 1 */
                builder = builder.AppendRespBulkString("PX").AppendRespBulkString(expiresMilliseconds);
            if (onlySetIfNotExists)
                builder = builder.AppendRespBulkString("NX");
            if (onlySetIfExists)
                builder = builder.AppendRespBulkString("XX");
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            if (response != null)
            {
                if (response.GetType().Equals(typeof(string)) && (string)response == "OK")
                {
                    return true;
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return false;
                }
            }
            else
            {
                // if we receive null, return false.  This generally means that there is a temporary Redis issue; the official docs say to try again after a few moments.
                return false;
            }
        }

        #endregion Command Group: Common

        #region Command Group: Hashes

        public async Task<long> HashDelAsync<TKey, TField>(TKey key, TField field)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate field type (and encode as a byte array, if required)
            byte[] fieldAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TField), field);

            // build request
            RespBuilder builder = new RespBuilder().AppendRespBulkString("HDEL").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(fieldAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateReturnType<long>(response, 0);
        }

        public async Task<long> HashExistsAsync<TKey, TField>(TKey key, TField field)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate field type (and encode as a byte array, if required)
            byte[] fieldAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TField), field);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("HEXISTS").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(fieldAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            return ValidateReturnType<long>(response, 0);
        }

        public async Task<Dictionary<TField, TValue>> HashGetAllASync<TKey, TField, TValue>(TKey key)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("HGETALL").AppendRespBulkString(keyAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response.GetType().Equals(typeof(object[])))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return new Dictionary<TField, TValue>();
            }

            // return the data in the requested type
            object[] responseAsArray = ((object[])response);
            // add the data in the requested type
            TField field;
            TValue value;
            Dictionary<TField, TValue> result = new Dictionary<TField, TValue>();
            for (int i = 0; i < responseAsArray.Length - 1; i += 2)
            {
                if (responseAsArray[i + 0].GetType().Equals(typeof(byte[])))
                {
                    if (typeof(TField).Equals(typeof(byte[])))
                    {
                        field = (TField)(object)responseAsArray[i + 0];
                    }
                    else if (typeof(TField).Equals(typeof(string)))
                    {
                        field = (TField)(object)_encoding.GetString((byte[])responseAsArray[i + 0]);
                    }
                    else
                    {
                        throw new InvalidTypeException("TField must be byte[] or string.", typeof(TField).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response value: " + responseAsArray[1].ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return new Dictionary<TField, TValue>();
                }

                if (responseAsArray[i + 1].GetType().Equals(typeof(byte[])))
                {
                    if (typeof(TValue).Equals(typeof(byte[])))
                    {
                        value = (TValue)(object)responseAsArray[i + 1];
                    }
                    else if (typeof(TField).Equals(typeof(string)))
                    {
                        value = (TValue)(object)_encoding.GetString((byte[])responseAsArray[i + 1]);
                    }
                    else
                    {
                        throw new InvalidTypeException("TValue must be byte[] or string.", typeof(TValue).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response value: " + responseAsArray[1].ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return new Dictionary<TField, TValue>();
                }
                result.Add(field, value);
            }

            return result;
        }

        public async Task<TValue> HashGetAsync<TKey, TField, TValue>(TKey key, TField field)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate field type (and encode as a byte array, if required)
            byte[] fieldAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TField), field);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("HGET").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(fieldAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response == null || response.GetType().Equals(typeof(byte[])))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return (TValue)(object)null;
            }

            // return the data in the requested type
            if (typeof(TValue).Equals(typeof(byte[])) || typeof(TValue).Equals(typeof(object)))
            {
                return (TValue)response;
            }
            else if (typeof(TValue).Equals(typeof(string)))
            {
                if (response == null)
                    return (TValue)(object)null;
                else
                    return (TValue)(object)_encoding.GetString((byte[])response);
            }
            else
            {
                throw new InvalidTypeException("Return type must be byte[] or string.", typeof(TValue).Name);
            }
        }

        public async Task<long> HashIncrByAsync<TKey, TField>(TKey key, TField field, long increment)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate field type (and encode as a byte array, if required)
            byte[] fieldAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TField), field);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("HINCRBY").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(fieldAsByteArray).AppendRespBulkString(increment.ToString());
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response.GetType().Equals(typeof(long)))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return -1;
            }

            // return the data in the requested type
            return (long)response;
        }

        // description: Set 'field' in the hash stored at 'key' to hold the string 'value'.  If 'field' already holds a value in the key, it is overwritten.
        // TKey: byte[] or string
        // TField: byte[] or string
        // TValue: byte[] or string
        // key: key
        // field: field
        // value: value
        // returns: 1 if 'field' is a new field in the hash and 'value' was set; 0 if 'field' already exists in the has and the value was updated
        public async Task<long> HashSetAsync<TKey, TField, TValue>(TKey key, TField field, TValue value)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate field type (and encode as a byte array, if required)
            byte[] fieldAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TField), field);
            // validate that value is not null
            if (value == null)
            {
                throw new ArgumentNullException("Value may not be null.", nameof(value));
            }
            // validate value type (and encode as a byte array, if required)
            byte[] valueAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TValue), value);

            // build request
            RespBuilder builder = new RespBuilder().AppendRespBulkString("HSET").AppendRespBulkString(keyAsByteArray).AppendRespBulkString(fieldAsByteArray).AppendRespBulkString(valueAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateReturnType<long>(response, 0);
        }

        #endregion Command Group: Hashes

        #region Command Group: Sets

        public async Task<long> SetAddAsync<TKey, TValue>(TKey key, TValue[] values)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate value type (and encode values as byte arrays, if required)
            byte[][] valuesAsByteArrayArray = Convert_StringArray_OrByteArrayArray_ToByteArrayArray(nameof(TValue), values);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("SADD").AppendRespBulkString(keyAsByteArray);
            foreach (byte[] value in valuesAsByteArrayArray)
            {
                builder = builder.AppendRespBulkString(value);
            }
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateReturnType<long>(response, 0);
        }

        public async Task<long> SetIsMemberAsync<TKey, TValue>(TKey key, TValue value)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);
            // validate value type (and encode values as byte arrays, if required)
            byte[] valueAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TValue), value);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("SISMEMBER").AppendRespBulkString(keyAsByteArray);
            builder = builder.AppendRespBulkString(valueAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateReturnType<long>(response, 0);
        }

        public async Task<List<TValue>> SetMembersAsync<TKey, TValue>(TKey key)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("SMEMBERS").AppendRespBulkString(keyAsByteArray);
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response == null)
            {
                // valid response; return null.
                return null;
            }
            else if (response.GetType().Equals(typeof(object[])))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return null;
            }

            // we have one or more values; populate them into a list and then return the list
            List<TValue> returnValues = new List<TValue>();
            foreach (object responseItem in (object[])response)
            {
                // add the data in the requested type
                if (responseItem.GetType().Equals(typeof(byte[])))
                {
                    if (typeof(TValue).Equals(typeof(byte[])) || typeof(TValue).Equals(typeof(object)))
                    {
                        returnValues.Add((TValue)responseItem);
                    }
                    else if (typeof(TValue).Equals(typeof(string)))
                    {
                        returnValues.Add((TValue)(object)_encoding.GetString((byte[])responseItem));
                    }
                    else
                    {
                        throw new InvalidTypeException("Return type must be byte[] or string.", typeof(TValue).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response array item: " + responseItem.ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return null;
                }
            }
            return returnValues;
        }

        #endregion Command Group: Sets

        #region Command Group: Sorted Sets

        public async Task<List<TValue>> SortedSetRangyByScoreAsync<TKey, TValue>(TKey key, double min, double max, bool includeMin = true, bool includeMax = true, bool withScores = false, double offset = 0, double count = Double.PositiveInfinity)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("ZRANGEBYSCORE").AppendRespBulkString(keyAsByteArray);
            //
            if (Double.IsNegativeInfinity(min))
            {
                builder.AppendRespBulkString("-inf");
            }
            else if (Double.IsPositiveInfinity(min))
            {
                builder.AppendRespBulkString("+inf");
            }
            else
            {
                builder.AppendRespBulkString((includeMin ? "" : "(") + min.ToString());
            }
            //
            if (Double.IsNegativeInfinity(max))
            {
                builder.AppendRespBulkString("-inf");
            }
            else
            if (Double.IsPositiveInfinity(max))
            {
                builder.AppendRespBulkString("+inf");
            }
            else
            {
                builder.AppendRespBulkString((includeMax ? "" : "(") + max.ToString());
            }
            //
            if (withScores)
            {
                builder.AppendRespBulkString("WITHSCORES");
            }
            //
            if (offset != 0 || count != double.PositiveInfinity)
            {
                builder.AppendRespBulkString("LIMIT").AppendRespBulkString(offset.ToString());
                if (count == double.PositiveInfinity)
                {
                    // NOTE: the Redis docs don't specify that "+inf" is a valid count value, so we use the largest allowable integer value instead
                    builder.AppendRespBulkString(Math.Pow(2,53).ToString());
                }
                else
                {
                    builder.AppendRespBulkString(count.ToString());
                }
            }
            //
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response == null)
            {
                // valid response; return null.
                return null;
            }
            else if (response.GetType().Equals(typeof(object[])))
            {
                // valid response; proceed.
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
                // return a reasonable failure response
                return null;
            }

            // we have one or more values; populate them into a list and then return the list
            List<TValue> returnValues = new List<TValue>();
            foreach (object responseItem in (object[])response)
            {
                // add the data in the requested type
                if (responseItem.GetType().Equals(typeof(byte[])))
                {
                    if (typeof(TValue).Equals(typeof(byte[])) || typeof(TValue).Equals(typeof(object)))
                    {
                        returnValues.Add((TValue)responseItem);
                    }
                    else if (typeof(TValue).Equals(typeof(string)))
                    {
                        returnValues.Add((TValue)(object)_encoding.GetString((byte[])responseItem));
                    }
                    else
                    {
                        throw new InvalidTypeException("Return type must be byte[] or string.", typeof(TValue).Name);
                    }
                }
                else
                {
                    // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                    string failMessage = "*** CRITICAL ERROR *** Unknown Redis response array item: " + responseItem.ToString();
                    RaiseCriticalError(failMessage);
                    // return a reasonable failure response
                    return null;
                }
            }
            return returnValues;
        }

        public async Task<long> SortedSetRemoveRangyByScoreAsync<TKey>(TKey key, double min, double max, bool includeMin = true, bool includeMax = true)
        {
            // validate key type (and encode as a byte array, if required)
            byte[] keyAsByteArray = Convert_String_OrByteArray_ToByteArray(nameof(TKey), key);

            // build request
            var builder = new RespBuilder().AppendRespBulkString("ZREMRANGEBYSCORE").AppendRespBulkString(keyAsByteArray);
            //
            if (Double.IsNegativeInfinity(min))
            {
                builder.AppendRespBulkString("-inf");
            }
            else if (Double.IsPositiveInfinity(min))
            {
                builder.AppendRespBulkString("+inf");
            }
            else
            {
                builder.AppendRespBulkString((includeMin ? "" : "(") + min.ToString());
            }
            //
            if (Double.IsNegativeInfinity(max))
            {
                builder.AppendRespBulkString("-inf");
            }
            else 
            if (Double.IsPositiveInfinity(max))
            {
                builder.AppendRespBulkString("+inf");
            }
            else
            {
                builder.AppendRespBulkString((includeMax ? "" : "(") + max.ToString());
            }
            //
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            return ValidateReturnType<long>(response, 0);
        }

        #endregion Command Group: Sorted Sets

        #region Command Group: Server

        public async Task<long> TimeAsync()
        {
            // build request
            var builder = new RespBuilder().AppendRespBulkString("TIME");
            var query = builder.ToByteArrayAsRespArray();

            // send request and wait for response
            object response = await SendQueryAndWaitForResponseAsync(query).ConfigureAwait(false);

            // validate response
            if (response.GetType().Equals(typeof(object[])))
            {
                object[] responseAsArray = ((object[])response);
                if (responseAsArray.Length >= 2 &&
                    responseAsArray[0].GetType().Equals(typeof(byte[])) &&
                    responseAsArray[1].GetType().Equals(typeof(byte[])))
                {
                    string secondsAsString = _encoding.GetString((byte[])responseAsArray[0]);
                    string microsecondsAsString = _encoding.GetString((byte[])responseAsArray[1]);
                    long seconds;
                    long microseconds;
                    if (long.TryParse(secondsAsString, out seconds) && long.TryParse(microsecondsAsString, out microseconds))
                    {
                        return (seconds * 1000000) + microseconds;
                    }
                    else
                    {
                        // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                        string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                        RaiseCriticalError(failMessage);
                    }
                }
            }
            else
            {
                // NOTE: if this happens: (a) the Redis command protocol has changed; (b) we are experiencing fatal data corruption; (c) there is a bug in our code.
                string failMessage = "*** CRITICAL ERROR *** Unknown Redis response: " + response.ToString();
                RaiseCriticalError(failMessage);
            }

            // return a reasonable failure response
            return -1;
        }

        #endregion Command Group: Server

    }
}
