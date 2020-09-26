using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Strombus.Redis
{
    /* this class builds a byte array in a memory efficient manner.
     *
     * this class contains two cursors: a read cursor and a write cursor
     * - the Append(value) functions append data to the end of the buffer
     * - the write cursor is controlled by append, and cannot otherwise be changed by the user
     * - the read cursor also auto-advances when data is read from the buffer
     * - the read cursor can also be manually set by the user
     * 
     * this class also enables the caller to remove bytes from the beginning of the buffer (which is particularly useful for parsing incoming RESP responses)
     */
    public class RespDualCursorByteArrayBuffer
    {
        const int ELEMENT_SIZE = 256;

        // we build a list of byte arrays to conserve memory and enable high-speed byte array building
        List<byte[]> _bufferElements = new List<byte[]>();

        // current write element
        byte[] _currentWriteElement = null;
        // write index of element within buffer
        int _currentWriteElementIndex = -1;
        // write cursor within buffer element
        int _currentWritePositionInElement = 0;
        // absolute write index in overall buffer
        int _currentWritePositionInBuffer = 0;

        // read index of element within buffer
        int _currentReadElementIndex = -1;
        // read cursor within buffer element
        int _currentReadPositionInElement = 0;

        int _ignoredBytesAtFrontOfBuffer = 0;

        // buffer lock
        object _bufferLock = new object();

        public RespDualCursorByteArrayBuffer() : this(0)
        {
        }

        public RespDualCursorByteArrayBuffer(int capacity)
        {
            // create our initial list of buffer elements
            int elementCount = Math.Max(1, (capacity / ELEMENT_SIZE) + ((capacity % ELEMENT_SIZE > 0) ? 1 : 0));
            for (int iElement = 0; iElement < elementCount; iElement++)
            {
                _bufferElements.Add(new byte[ELEMENT_SIZE]);
            }
            _currentWriteElementIndex = 0;
            _currentWriteElement = _bufferElements[_currentWriteElementIndex];
            _currentWritePositionInElement = 0;

            _currentReadElementIndex = 0;
            _currentReadPositionInElement = 0;
        }

        public int RemoveBytesAtFront(int length)
        {
            int totalBytesRemoved = 0;
            lock (_bufferLock)
            {
                int totalBytesToRemove = Math.Min(length, this.Length);
                int bytesToRemove;

                while (totalBytesRemoved < totalBytesToRemove)
                {
                    // remove some or all of the data in the first element
                    bytesToRemove = Math.Min(ELEMENT_SIZE - _ignoredBytesAtFrontOfBuffer, totalBytesToRemove - totalBytesRemoved);
                    if (bytesToRemove > 0)
                    {
                        // remove part of the first element in the array
                        _ignoredBytesAtFrontOfBuffer += bytesToRemove;
                        totalBytesRemoved += bytesToRemove;

                        // if we have removed byte values which existed after the current read cursor, reset the read cursor to "new position zero."
                        if (_currentReadElementIndex == 0)
                            _currentReadPositionInElement = Math.Max(_currentReadPositionInElement, _ignoredBytesAtFrontOfBuffer);
                    }

                    // check if the first element was completed emptied (in which case it needs to be removed)
                    if (_ignoredBytesAtFrontOfBuffer == ELEMENT_SIZE)
                    {
                        // remove the first element in the array
                        _bufferElements.RemoveAt(0);
                        _currentWriteElementIndex--;
                        _currentWritePositionInBuffer -= ELEMENT_SIZE;
                        _currentReadElementIndex--;

                        _ignoredBytesAtFrontOfBuffer = 0;
                    }
                }

                // if our buffer was completely emptied, add a single buffer element
                if (_bufferElements.Count == 0)
                {
                    _bufferElements.Add(new byte[ELEMENT_SIZE]);

                    _currentWriteElementIndex = 0;

                    _currentReadElementIndex = 0;
                }

                _currentWriteElement = _bufferElements[_currentWriteElementIndex];
            }

            return totalBytesRemoved;
        }

        void MoveToNextBufferElement()
        {
            lock (_bufferLock)
            {
                // sanity check: if the previous buffer element is not full, we should not be moving on to the next buffer element
                if (_currentWritePositionInElement != ELEMENT_SIZE)
                    throw new InvalidOperationException();

                if (_currentWriteElementIndex == _bufferElements.Count - 1)
                {
                    _currentWriteElement = new byte[ELEMENT_SIZE];
                    _bufferElements.Add(_currentWriteElement);
                }
                else
                {
                    _currentWriteElement = _bufferElements[_currentWriteElementIndex + 1];
                }

                _currentWritePositionInElement = 0;
                _currentWriteElementIndex++;
            }
        }

        public int Length
        {
            get
            {
                lock (_bufferLock)
                {
                    return _currentWritePositionInBuffer - _ignoredBytesAtFrontOfBuffer;
                }
            }
        }

        public void Write(byte value)
        {
            Write(new byte[] { value });
        }

        public void Write(byte[] value)
        {
            Write(value, 0, value.Length);
        }

        public void Write(byte[] value, int index, int length)
        {
            int valuePosition = index;

            lock (_bufferLock)
            {
                int lengthToCopy = Math.Min(ELEMENT_SIZE - _currentWritePositionInElement, index + length - valuePosition);
                while (lengthToCopy > 0)
                {
                    Array.Copy(value, valuePosition, _currentWriteElement, _currentWritePositionInElement, lengthToCopy);
                    valuePosition += lengthToCopy;
                    _currentWritePositionInElement += lengthToCopy;
                    _currentWritePositionInBuffer += lengthToCopy;

                    // if we have filled the current buffer element, add an empty buffer element.
                    if (_currentWritePositionInElement == ELEMENT_SIZE)
                        MoveToNextBufferElement();

                    lengthToCopy = Math.Min(ELEMENT_SIZE - _currentWritePositionInElement, index + length - valuePosition);
                }
            }
        }

        // ReadCursorPosition is the logical position in the buffer.  It is equal to the actual position minus the "ignoredBytesAtFrontOfBuffer" count.
        public int ReadCursorPosition
        {
            set
            {
                lock (_bufferLock)
                {
                    if (value > _currentWritePositionInBuffer - _ignoredBytesAtFrontOfBuffer)
                        throw new IndexOutOfRangeException();

                    _currentReadElementIndex = ((value + _ignoredBytesAtFrontOfBuffer) / ELEMENT_SIZE);
                    _currentReadPositionInElement = (value + _ignoredBytesAtFrontOfBuffer) % ELEMENT_SIZE;
                }
            }
            get
            {
                lock (_bufferLock)
                {
                    return (_currentReadElementIndex * ELEMENT_SIZE) + _currentReadPositionInElement - _ignoredBytesAtFrontOfBuffer;
                }
            }
        }

        // the read cursor is independent of the write cursor, so we read from the current read cursor position.
        public int Read(byte[] buffer, int index, int length)
        {
            int totalBytesRead = 0;

            lock (_bufferLock)
            {
                if (length > _currentWritePositionInBuffer - (_currentReadElementIndex * ELEMENT_SIZE) - _currentReadPositionInElement)
                {
                    throw new ArgumentOutOfRangeException();
                }

                int bytesToRead;
                while (totalBytesRead < length)
                {
                    bytesToRead = Math.Min(length - totalBytesRead, (ELEMENT_SIZE - _currentReadPositionInElement));
                    Array.Copy(_bufferElements[_currentReadElementIndex], _currentReadPositionInElement, buffer, index + totalBytesRead, bytesToRead);
                    _currentReadPositionInElement += bytesToRead;
                    totalBytesRead += bytesToRead;

                    if (_currentReadPositionInElement == ELEMENT_SIZE)
                    {
                        _currentReadElementIndex++;
                        _currentReadPositionInElement = 0;
                    }
                }
            }

            return totalBytesRead;
        }

        public int ReadByte()
        {
            int returnValue;

            lock (_bufferLock)
            {
                if (_currentWritePositionInBuffer == (_currentReadElementIndex * ELEMENT_SIZE) + _currentReadPositionInElement)
                {
                    return -1;
                }

                returnValue = _bufferElements[_currentReadElementIndex][_currentReadPositionInElement];
                _currentReadPositionInElement++;

                if (_currentReadPositionInElement == ELEMENT_SIZE)
                {
                    _currentReadElementIndex++;
                    _currentReadPositionInElement = 0;
                }
            }

            return returnValue;
        }

        // NOTE: Pass in a prefix array to push a set of bytes in front of the existing buffer; this is particularly useful for building RESP Arrays (where the element count is not known in advance).
        public byte[] ToArray(byte[] prefixArray = null)
        {
            byte[] byteArray;

            int prefixArrayLength = (prefixArray != null ? prefixArray.Length : 0);

            lock (_bufferLock)
            {
                byteArray = new byte[prefixArrayLength + _currentWritePositionInBuffer - _ignoredBytesAtFrontOfBuffer];
                if (prefixArrayLength > 0)
                {
                    Array.Copy(prefixArray, byteArray, prefixArrayLength);
                }

                int byteArrayPosition = 0;

                int totalLength = _currentWritePositionInBuffer - _ignoredBytesAtFrontOfBuffer;
                int elementLengthToCopy;
                for (int iElement = 0; iElement < _bufferElements.Count; iElement++)
                {
                    byte[] element = _bufferElements[iElement];
                    if (iElement == 0 && _ignoredBytesAtFrontOfBuffer > 0)
                    {
                        elementLengthToCopy = Math.Min(ELEMENT_SIZE - _ignoredBytesAtFrontOfBuffer, totalLength - byteArrayPosition);
                        Array.Copy(element, _ignoredBytesAtFrontOfBuffer, byteArray, prefixArrayLength + byteArrayPosition, elementLengthToCopy);
                    }
                    else
                    {
                        elementLengthToCopy = Math.Min(ELEMENT_SIZE, totalLength - byteArrayPosition);
                        Array.Copy(element, 0, byteArray, prefixArrayLength + byteArrayPosition, elementLengthToCopy);
                    }
                    byteArrayPosition += elementLengthToCopy;
                }
            }

            return byteArray;
        }
    }
}
