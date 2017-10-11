using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Threading.Tasks;
using Automata.Events.Store;
using Shouldly;
using Xunit;

namespace Automata.Events.UnitTests.Store.EventLogPageTests
{
    public class WhenIncomplete : IDisposable
    {
        private readonly string _filename;
        private readonly MemoryMappedFile _file;
        private readonly MemoryMappedViewAccessor _accessor;
        private readonly EventLogPage _log;

        public WhenIncomplete()
        {
            _filename = Path.GetTempFileName();
            File.WriteAllBytes(_filename, new byte[256]);
            _file = MemoryMappedFile.CreateFromFile(_filename, FileMode.Create, null, 256, MemoryMappedFileAccess.CopyOnWrite);
            _accessor = _file.CreateViewAccessor(0, 256, MemoryMappedFileAccess.CopyOnWrite);
            _log = new EventLogPage(0, false, _accessor);
        }

        public void Dispose()
        {
            _accessor.Dispose();
            _file.Dispose();
            File.Delete(_filename);
        }
        
        [Fact]
        public void FlushShould_CompletePreviousWriteTasks()
        {
            var payloads = new EventPayloads() {
                new ArraySegment<byte>(new byte[] { 0, 1, 2, 3, 4 })
            };

            var task = _log.Write(payloads);
            
            task.IsCompleted.ShouldBeFalse();

            _log.Flush();
            
            task.IsCompleted.ShouldBeTrue();
        }

        [Fact]
        public void WriteShould_ReturnTaskWhenSpaceAvailable()
        {
            var payloads = new EventPayloads {
                new ArraySegment<byte>(new byte[] { 0, 1, 2, 3, 4 })
            };

            var task = _log.Write(payloads);

            task.ShouldNotBe(null);
        }

        [Fact]
        public void WriteShould_ReturnNullWhenSpaceUnavailable()
        {
            using (var accessor = _file.CreateViewAccessor(0, 4, MemoryMappedFileAccess.CopyOnWrite)) {
                var log = new EventLogPage(0, false, accessor);

                var payloads = new EventPayloads {
                    new ArraySegment<byte>(new byte[] { 0, 1, 2, 3, 4 })
                };

                var task = log.Write(payloads);
                
                task.ShouldBe(null);
            }
        }

        [Fact]
        public async Task WriteShould_ReturnSequentialIds()
        {
            var payloads = new EventPayloads {
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 })
            };

            var task = _log.Write(payloads);
            _log.Flush();

            var result = await task;
            var ids = result.Select(e => e.Id).ToList();

            ids.ShouldBe(Enumerable.Range((int) ids.Min(), payloads.Count).Select(id => (ulong) id));
        }

        [Fact]
        public async Task WriteShould_ReturnContiguousIdsAcrossCalls()
        {
            var payloads = new EventPayloads {
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 })
            };

            var a = _log.Write(payloads);
            var b = _log.Write(payloads);
            var c = _log.Write(payloads);
            _log.Flush();

            var result = (await a).Concat(await b).Concat(await c);
            var ids = result.Select(e => e.Id).OrderBy(id => id).ToList();

            ids.ShouldBe(Enumerable.Range((int) ids.Min(), payloads.Count * 3).Select(id => (ulong) id));
        }

        [Fact]
        public async Task WriteShould_ReturnOrderedIdsAcrossFlushBoundries()
        {
            var payloads = new EventPayloads {
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 }),
                new ArraySegment<byte>(new byte[] { 0, 1 })
            };

            var a = _log.Write(payloads);
            var b = _log.Write(payloads);
            _log.Flush();
            var c = _log.Write(payloads);
            _log.Flush();

            var firstMax = (await a).Concat(await b).Select(e => e.Id).Max();
            var secondMin = (await c).Select(e => e.Id).Min();

            firstMax.ShouldBeLessThan(secondMin);
        }
    }
}