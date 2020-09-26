using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Strombus.Redis
{
    class RespBuilder
    {
        int _elementCount;
        RespDualCursorByteArrayBuffer _builder;

        byte[] CRLF = Constants.SIMPLE_REDIS_ENCODING.GetBytes("\r\n");

        public RespBuilder()
        {
            _builder = new RespDualCursorByteArrayBuffer();
            _elementCount = 0;
        }

        public RespBuilder AppendRespSimpleString(string value)
        {
            byte[] encodedValue = Constants.SIMPLE_REDIS_ENCODING.GetBytes(value);

            if (encodedValue.Contains((byte)'\r') || encodedValue.Contains((byte)'\n'))
            {
                throw new ArgumentException("Simple strings may not contain \r or \n.", nameof(value));
            }

            _builder.Write((byte)'+');
            _builder.Write(encodedValue);
            _builder.Write(CRLF);

            _elementCount++;

            return this;
        }

        public RespBuilder AppendRespError(string message)
        {
            byte[] encodedMessage = Constants.SIMPLE_REDIS_ENCODING.GetBytes(message);

            if (encodedMessage.Contains((byte)'\r') || encodedMessage.Contains((byte)'\n'))
            {
                throw new ArgumentException("Error messages may not contain \r or \n.", nameof(message));
            }

            _builder.Write((byte)'-');
            _builder.Write(encodedMessage);
            _builder.Write(CRLF);

            _elementCount++;

            return this;
        }

        public RespBuilder AppendRespError(string prefix, string message)
        {
            if (prefix.ToUpperInvariant() != prefix)
            {
                throw new ArgumentException("Error prefixes must be in ALL CAPS.", nameof(prefix));
            }

            byte[] encodedPrefix = Constants.SIMPLE_REDIS_ENCODING.GetBytes(prefix.Trim());
            byte[] encodedMessage = Constants.SIMPLE_REDIS_ENCODING.GetBytes(message);

            if (encodedPrefix.Contains((byte)'\r') || encodedPrefix.Contains((byte)'\n'))
            {
                throw new ArgumentException("Error prefixes may not contain \r or \n.", nameof(prefix));
            }
            if (encodedMessage.Contains((byte)'\r') || encodedMessage.Contains((byte)'\n'))
            {
                throw new ArgumentException("Error messages may not contain \r or \n.", nameof(message));
            }

            _builder.Write((byte)'-');
            _builder.Write(encodedPrefix);
            _builder.Write((byte)' ');
            _builder.Write(encodedMessage);
            _builder.Write(CRLF);

            _elementCount++;

            return this;
        }

        public RespBuilder AppendRespInteger(long value)
        {
            _builder.Write((byte)':');
            _builder.Write(Constants.SIMPLE_REDIS_ENCODING.GetBytes(value.ToString()));
            _builder.Write(CRLF);

            _elementCount++;

            return this;
        }

        /* NOTE: "bulk string" is RESP's name for an array of binary data */
        public RespBuilder AppendRespBulkString(string value)
        {
            byte[] encodedValue = Constants.SIMPLE_REDIS_ENCODING.GetBytes(value);
            return AppendRespBulkString(encodedValue, 0, encodedValue.Length);
        }

        public RespBuilder AppendRespBulkString(string value, System.Text.Encoding encoding)
        {
            byte[] encodedValue = encoding.GetBytes(value);
            return AppendRespBulkString(encodedValue, 0, encodedValue.Length);
        }

        public RespBuilder AppendRespBulkString(long value)
        {
            byte[] encodedValue = Constants.SIMPLE_REDIS_ENCODING.GetBytes(value.ToString());
            return AppendRespBulkString(encodedValue, 0, encodedValue.Length);
        }

        public RespBuilder AppendRespBulkString(double value)
        {
            byte[] encodedValue = Constants.SIMPLE_REDIS_ENCODING.GetBytes(value.ToString());
            return AppendRespBulkString(encodedValue, 0, encodedValue.Length);
        }

        public RespBuilder AppendRespBulkString(byte[] value)
        {
            return AppendRespBulkString(value, 0, value.Length);
        }

        public RespBuilder AppendRespBulkString(byte[] value, int index, int length)
        {
            _builder.Write((byte)'$');
            _builder.Write(Constants.SIMPLE_REDIS_ENCODING.GetBytes(length.ToString()));
            _builder.Write(CRLF);
            _builder.Write(value, index, length);
            _builder.Write(CRLF);

            _elementCount++;

            return this;
        }

        /* RespNull = RESP Null Bulk String */
        public RespBuilder AppendRespNull()
        {
            _builder.Write((byte)'$');
            _builder.Write(Constants.SIMPLE_REDIS_ENCODING.GetBytes("-1"));
            _builder.Write(CRLF);

            _elementCount++;

            return this;
        }

        /* NOTE: this function is only called for special applications where a legacy 'null array' is required */
        public RespBuilder AppendRespNullArray()
        {
            _builder.Write((byte)'*');
            _builder.Write(Constants.SIMPLE_REDIS_ENCODING.GetBytes("-1"));
            _builder.Write(CRLF);

            _elementCount++;

            return this;
        }

        public RespBuilder AppendRespBuilderAsRespArray(RespBuilder value)
        {
            _builder.Write(value.ToByteArrayAsRespArray());

            _elementCount++;

            return this;
        }

        /* return our data as an array of bytes; if we have zero elements or more than one element, we first wrap the data in an array */
        public byte[] ToByteArray()
        {
            if (_elementCount == 1)
            {
                // by default, we return single elements as-is, not wrapped in an array; to wrap one element in an array, call .ToByteArrayAsRespArray() instead.
                return _builder.ToArray();
            }
            else
            {
                // if the contents are zero elements or >1 elements, return a RESP array.
                return ToByteArrayAsRespArray();
            }
        }

        public byte[] ToByteArrayAsRespArray()
        {
            // create a prefix array consisting of the RESP Array header ('*' followed by number of elements followed by CrLf)
            // calculate the size (in bytes) of the encoded element count
            int elementCountLengthInByteArray = Constants.SIMPLE_REDIS_ENCODING.GetByteCount(_elementCount.ToString());
            // create the prefix array
            byte[] prefixArray = new byte[1 + elementCountLengthInByteArray + 2];
            prefixArray[0] = (byte)'*';
            Array.Copy(Constants.SIMPLE_REDIS_ENCODING.GetBytes(_elementCount.ToString()), 0, prefixArray, 1, elementCountLengthInByteArray);
            Array.Copy(CRLF, 0, prefixArray, 1 + elementCountLengthInByteArray, CRLF.Length);

            return _builder.ToArray(prefixArray);
        }
    }
}

