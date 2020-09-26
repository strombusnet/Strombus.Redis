using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Strombus.Redis
{
    public struct RedisError
    {
        public string Prefix;
        public string Message;

        public RedisError(string prefix, string message)
        {
            this.Prefix = prefix;
            this.Message = message;
        }

        public RedisError(string message)
        {
            this.Prefix = null; /* TODO: should this be string.Empty instead? */
            this.Message = message;
        }
    }
}
