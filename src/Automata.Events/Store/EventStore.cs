using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Force.Crc32;

namespace Automata.Events.Store
{
    [StructLayout(LayoutKind.Sequential)]
    public struct EventMeta
    {
        public ulong Id;
        public DateTime Created;
    }

    public struct Event
    {
        public EventMeta Meta => throw new NotImplementedException();
        public Stream OpenPayload() => throw new NotImplementedException();
    }

    public interface IEventPayloads : IEnumerable<ArraySegment<byte>>
    {
        int Count { get; }
    }
    
    public class EventPayloads : List<ArraySegment<byte>>, IEventPayloads { }
    
    public class EventStoreOptions
    {
        public int PageSize { get; set; }
        public TimeSpan MinimumRetention { get; set; }
    }
    
    public class EventStore
    {
        public Task<IEnumerable<EventMeta>> Store(IEventPayloads payload) => throw new NotImplementedException();
        public IObservable<Event> Listen(ulong? since = null) => throw new NotImplementedException();
    }

    public interface ILogWriter
    {
        Task<IEnumerable<EventMeta>> Write(IEventPayloads payload);
        long Flush();
    }

    public class EventLogPage : ILogWriter
    {
        private readonly ulong _start;
        private readonly MemoryMappedViewAccessor _view;
        private readonly List<Action> _awaiting;

        private bool _isComplete;
        private long _position;
        private uint _count;
        private byte[] _crcBuffer;
        
        public EventLogPage(ulong start, bool complete, MemoryMappedViewAccessor view)
        {
            _start = start;
            _isComplete = complete;
            _view = view;
            _awaiting = new List<Action>();
            _position = 0;
            _count = 0;
            _crcBuffer = new byte[512];
        }

        public unsafe Task<IEnumerable<EventMeta>> Write(IEventPayloads payload)
        {
            lock (_view) {
                // early out if we are complete
                if (_isComplete) {
                    return null;
                }
                
                // early out if we don't have enough space left in the log
                var space = _view.Capacity - _position;
                if (space < CalculateSize(payload)) {
                    return null;
                }

                var events = new List<EventMeta>();

                try {
                    byte* ptr = null;
                    _view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

                    var start = _position;

                    // write event count in the batch
                    *(ptr + _position) = (byte)payload.Count;
                    _position += sizeof(byte);

                    foreach (var e in payload) {
                        // write event meta block
                        var meta = new EventMeta {
                            Created = DateTime.UtcNow,
                            Id = _start + _count
                        };

                        *(EventMeta*) (ptr + _position) = meta;
                        _position += sizeof(EventMeta);

                        // write event payload
                        *(ushort*) (ptr + _position) = (ushort) e.Count;
                        _position += sizeof(ushort);

                        Marshal.Copy(e.Array, e.Offset, (IntPtr) (ptr + _position), e.Count);
                        _position += e.Count;

                        // increment event count
                        _count++;
                        events.Add(meta);
                    }

                    // post-fix batch with crc
                    var length = (int)(_position - start);
                    var crcBuffer = GetBuffer(length);
                    Marshal.Copy((IntPtr)(ptr + start), crcBuffer, 0, length);
                    var crc = Crc32Algorithm.Compute(crcBuffer, 0, length);

                    *(uint*) (ptr + _position) = crc;
                    _position += sizeof(uint);
                }
                finally {
                    _view.SafeMemoryMappedViewHandle.ReleasePointer();
                }
                
                // store a handle to the task - to be completed when the buffer is flushed
                var tcs = new TaskCompletionSource<IEnumerable<EventMeta>>();
                _awaiting.Add(() => tcs.SetResult(events));

                return tcs.Task;
            }
        }

        public void Complete()
        {
            lock (_view) {
                if (_isComplete) {
                    throw new InvalidOperationException("Log page has already been completed");
                }
                
                _isComplete = true;
            }
        }

        private unsafe long CalculateSize(IEventPayloads payloads)
        {
            // event count + crc
            // foreach event: meta + payload
            
            long size = sizeof(byte) + sizeof(uint);
            foreach (var e in payloads) {
                size += sizeof(EventMeta);
                size += e.Count;
            }

            return size;
        }

        private byte[] GetBuffer(int minLength)
        {
            if (_crcBuffer.Length < minLength) {
                var length = _crcBuffer.Length;
                while (length < minLength) {
                    length *= 2;
                }
                
                _crcBuffer = new byte[length];
            }

            return _crcBuffer;
        }

        public long Flush()
        {
            lock (_view) {
                _view.Flush();

                foreach (var write in _awaiting) {
                    write();
                }
                
                _awaiting.Clear();
                
                return _position;
            }
        }
    }
}