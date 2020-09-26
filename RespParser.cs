using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Strombus.Redis
{
    public class RespParser
    {
        RespDualCursorByteArrayBuffer _buffer = new RespDualCursorByteArrayBuffer();
        object _bufferLock = new object();

        public Encoding DefaultBulkStringEncoding { get; set; } = Constants.SIMPLE_REDIS_ENCODING;

        public RespParser()
        {
            _buffer = new RespDualCursorByteArrayBuffer();
        }

        // NOTE: Append is called by the client to add new (incoming) RESP data to our buffer
        public void Append(byte value)
        {
            _buffer.Write(value);
        }

        public void Append(byte[] value)
        {
            _buffer.Write(value);
        }

        public void Append(byte[] value, int index, int length)
        {
            _buffer.Write(value, index, length);
        }

        /* NOTE: this function returns the RESP value and removes it from our buffer.  
         *       This function SHOULD NOT be used unless all data in the buffer is complete. For potentially-incomplete data, use TryGetValue(...)*/
        public object ReadValue()
        {
            object value;
            bool success = TryReadValue(out value);
            if (!success)
                throw new Exception(); /* we should never get here; our caller should never request the value if data is invalid/incomplete */

            return value;
        }

        public bool TryReadValue(out object value)
        {
            // default return value; ignored by client if we return false
            value = null;

            int index = 0;
            bool success = TryParseRespObject(ref index, out value);
            if (success == false)
                return false;

            _buffer.RemoveBytesAtFront(index); // remove the retrieved characters

            return true;
        }

        bool ReadCharacterAtCurrentCursor(RespDualCursorByteArrayBuffer buffer, out char value)
        {
            return ReadCharacterAtCurrentCursor(buffer, out value, Constants.SIMPLE_REDIS_ENCODING);
        }

        bool ReadCharacterAtCurrentCursor(RespDualCursorByteArrayBuffer buffer, out char value, System.Text.Encoding encoding)
        {
            // default return value; ignored by client if we return false
            value = '\0';

            int byteValue = _buffer.ReadByte();
            if (byteValue == -1)
                return false;

            try
            {
                value = Constants.SIMPLE_REDIS_ENCODING.GetString(new byte[] { (byte)byteValue })[0];
                return true;
            }
            catch
            {
                return false;
            }
        }

        bool ReadByteAtCurrentCursor(RespDualCursorByteArrayBuffer buffer, out byte value)
        {
            // default return value; ignored by client if we return false
            value = 0;

            int byteValue = _buffer.ReadByte();
            if (byteValue == -1)
                return false;

            value = (byte)byteValue;
            return true;
        }

        bool TryParseRespSimpleString(ref int index, out string value)
        {
            bool readSuccess = false;
            char ch;

            // default return value; ignored by client if we return false
            value = null;

            lock (_bufferLock)
            {
                // move our read cursor to 'index'
                _buffer.ReadCursorPosition = index;

                // sanity check: make sure we are reading a value of the desired type
                byte respType;
                readSuccess = ReadByteAtCurrentCursor(_buffer, out respType);
                if (!readSuccess)
                    return false; // no data was available
                if (respType != (byte)'+')
                    return false; // invalid cast

                StringBuilder builder = new StringBuilder();
                while (true)
                {
                    readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                    if (!readSuccess)
                        return false;

                    if (ch == '\r')
                    {
                        // now, grab (and ignore) the next character: '\n'
                        readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                        if (!readSuccess)
                            return false;

                        // to optimize for speed, ignore the next character as recommended by the REDIS protocol document.
                        //if (ch != '\n')
                        //    return false;

                        break;
                    }

                    builder.Append(ch);
                }

                value = builder.ToString();
                index = _buffer.ReadCursorPosition;
                return true;
            }
        }

        bool TryParseRespError(ref int index, out string value)
        {
            bool readSuccess = false;
            char ch;

            // default return value; ignored by client if we return false
            value = null;

            lock (_bufferLock)
            {
                // move our read cursor to 'index'
                _buffer.ReadCursorPosition = index;

                // sanity check: make sure we are reading a value of the desired type
                byte respType;
                readSuccess = ReadByteAtCurrentCursor(_buffer, out respType);
                if (!readSuccess)
                    return false; // no data was available
                if (respType != (byte)'-')
                    return false; // invalid cast

                StringBuilder builder = new StringBuilder();
                while (true)
                {
                    readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                    if (!readSuccess)
                        return false;

                    if (ch == '\r')
                    {
                        // now, grab (and ignore) the next character: '\n'
                        readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                        if (!readSuccess)
                            return false;

                        // to optimize for speed, ignore the next character as recommended by the REDIS protocol document.
                        //if (ch != '\n')
                        //    return false;

                        break;
                    }

                    builder.Append(ch);
                }

                value = builder.ToString();
                index = _buffer.ReadCursorPosition;
                return true;
            }
        }

        bool TryParseRespInteger(ref int index, out long value)
        {
            bool readSuccess = false;
            char ch;

            // default return value; ignored by client if we return false
            value = 0;

            lock (_bufferLock)
            {
                // move our read cursor to 'index'
                _buffer.ReadCursorPosition = index;

                // sanity check: make sure we are reading a value of the desired type
                byte respType;
                readSuccess = ReadByteAtCurrentCursor(_buffer, out respType);
                if (!readSuccess)
                    return false; // no data was available
                if (respType != (byte)':')
                    return false; // invalid cast

                int valuePos = 0;
                bool valueIsNegative = false;
                while (true)
                {
                    readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                    if (!readSuccess)
                        return false;

                    if (ch == '\r')
                    {
                        // now, grab (and ignore) the next character: '\n'
                        readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                        if (!readSuccess)
                            return false;

                        // to optimize for speed, ignore the next character as recommended by the REDIS protocol document.
                        //if (ch != '\n')
                        //    return false;

                        break;
                    }
                    else if (valuePos == 0 && ch == '-')
                    {
                        valueIsNegative = true;
                    }
                    else
                    {
                        value = (value * 10) + (ch - '0');
                    }
                    valuePos++;
                }

                if (valueIsNegative)
                    value = -value;

                index = _buffer.ReadCursorPosition;
                return true;
            }
        }

        bool TryParseRespBulkString(ref int index, out byte[] value)
        {
            bool readSuccess = false;
            char ch;

            // default return value; ignored by client if we return false
            value = null;

            lock (_bufferLock)
            {
                // move our read cursor to 'index'
                _buffer.ReadCursorPosition = index;

                // sanity check: make sure we are reading a value of the desired type
                byte respType;
                readSuccess = ReadByteAtCurrentCursor(_buffer, out respType);
                if (!readSuccess)
                    return false; // no data was available
                if (respType != (byte)'$')
                    return false; // invalid cast

                int lengthPos = 0;
                int length = 0;
                bool lengthIsNegative = false;
                while (true)
                {
                    readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                    if (!readSuccess)
                        return false;

                    if (ch == '\r')
                    {
                        // now, grab (and ignore) the next character: '\n'
                        readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                        if (!readSuccess)
                            return false;

                        // to optimize for speed, ignore the next character as recommended by the REDIS protocol document.
                        //if (ch != '\n')
                        //    return false;

                        break;
                    }
                    else if (lengthPos == 0 && ch == '-')
                    {
                        lengthIsNegative = true;
                    }
                    else
                    {
                        length = (length * 10) + (ch - '0');
                    }
                    lengthPos++;
                }

                if (lengthIsNegative)
                    length = -length;

                /* special case: null string */
                if (length == -1)
                {
                    value = null;
                    index = _buffer.ReadCursorPosition;
                    return true;
                }
                else if (length < -1)
                {
                    return false;
                }

                value = new byte[length];
                for (int i = 0; i < length; i++)
                {
                    readSuccess = ReadByteAtCurrentCursor(_buffer, out value[i]);
                    if (!readSuccess)
                        return false;
                }

                readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                if (!readSuccess)
                    return false;
                if (ch == '\r')
                {
                    // now, grab (and ignore) the next character: '\n'
                    readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                    if (!readSuccess)
                        return false;

                    // to optimize for speed, ignore the next character as recommended by the REDIS protocol document.
                    //if (ch != '\n')
                    //    return false;
                }

                index = _buffer.ReadCursorPosition;
                return true;
            }
        }

        bool TryParseRespArray(ref int index, out object[] value)
        {
            bool readSuccess = false;
            char ch;

            // default return value; ignored by client if we return false
            value = null;

            lock (_bufferLock)
            {
                // move our read cursor to 'index'
                _buffer.ReadCursorPosition = index;

                // sanity check: make sure we are reading a value of the desired type
                byte respType;
                readSuccess = ReadByteAtCurrentCursor(_buffer, out respType);
                if (!readSuccess)
                    return false; // no data was available
                if (respType != (byte)'*')
                    return false; // invalid cast

                int lengthPos = 0;
                int length = 0;
                bool lengthIsNegative = false;
                while (true)
                {
                    readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                    if (!readSuccess)
                        return false;

                    if (ch == '\r')
                    {
                        // now, grab (and ignore) the next character: '\n'
                        readSuccess = ReadCharacterAtCurrentCursor(_buffer, out ch);
                        if (!readSuccess)
                            return false;

                        // to optimize for speed, ignore the next character as recommended by the REDIS protocol document.
                        //if (ch != '\n')
                        //    return false;

                        break;
                    }
                    else if (lengthPos == 0 && ch == '-')
                    {
                        lengthIsNegative = true;
                    }
                    else
                    {
                        length = (length * 10) + (ch - '0');
                    }
                    lengthPos++;
                }

                if (lengthIsNegative)
                    length = -length;

                /* special case: null array */
                if (length == -1)
                {
                    value = null;
                    return true;
                }
                else if (length < -1)
                {
                    return false;
                }

                index = _buffer.ReadCursorPosition;

                value = new object[length];
                for (int i = 0; i < length; i++)
                {
                    bool success = TryParseRespObject(ref index, out value[i]);

                    if (!success)
                        return false;
                }

                index = _buffer.ReadCursorPosition;
                return true;
            }
        }

        bool TryParseRespObject(ref int index, out object value)
        {
            byte respType;

            // default return value; ignored by client if we return false; will be ignored by the caller if we return false
            value = null;

            // move our read cursor to 'index'
            _buffer.ReadCursorPosition = index;

            bool readSuccess = ReadByteAtCurrentCursor(_buffer, out respType);
            if (!readSuccess)
                return false; // no data was available

            switch (respType)
            {
                case (byte)'+':
                    {
                        string stringValue;
                        bool success = TryParseRespSimpleString(ref index, out stringValue);
                        value = stringValue;
                        return success;
                    }
                case (byte)'-':
                    {
                        string errorValue;
                        bool success = TryParseRespError(ref index, out errorValue);
                        if (success)
                        {
                            if (errorValue.IndexOf(' ') >= 0 && (errorValue.Substring(0, errorValue.IndexOf(' ')) == errorValue.Substring(0, errorValue.IndexOf(' ')).ToUpperInvariant()))
                            {
                                value = new RedisError(errorValue.Substring(0, errorValue.IndexOf(' ')), errorValue.Substring(errorValue.IndexOf(' ') + 1));
                            }
                            else
                            {
                                value = new RedisError(errorValue);
                            }
                        }
                        else
                        {
                            value = errorValue;
                        }
                        return success;
                    }
                case (byte)':':
                    {
                        long integerValue;
                        bool success = TryParseRespInteger(ref index, out integerValue);
                        value = integerValue;
                        return success;
                    }
                case (byte)'$':
                    {
                        byte[] byteArrayValue;
                        bool success = TryParseRespBulkString(ref index, out byteArrayValue);
                        value = byteArrayValue;
                        return success;
                    }
                case (byte)'*':
                    {
                        object[] objectArrayValue;
                        bool success = TryParseRespArray(ref index, out objectArrayValue);
                        value = objectArrayValue;
                        return success;
                    }
                default:
                    return false;
            }
        }
    }
}
