using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Strombus.Redis
{
    class Constants
    {
        /* NOTE: REDIS documentation does not indicate which encoding should be used for REDIS Simple Strings, Redis Errors, etc.
         *       We have made the assumption that iso-8859-1 should be used, since it is the first page of UNICODE and is non-OS specific.
         *       If this assumption turns out to be incorrect, then this default encoding should be changed (and all usage of the default
         *       encoding in the RespDualCursorByteArrayBuffer class should be double-checked to ensure that it is not adversely affected. */
        internal static readonly System.Text.Encoding SIMPLE_REDIS_ENCODING = System.Text.Encoding.GetEncoding("iso-8859-1");
    }
}
