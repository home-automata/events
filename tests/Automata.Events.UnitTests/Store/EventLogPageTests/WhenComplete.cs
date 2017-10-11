using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using Automata.Events.Store;
using Shouldly;
using Xunit;

namespace Automata.Events.UnitTests.Store.EventLogPageTests
{
    public class WhenComplete : IDisposable
    {
        private readonly string _filename;
        private readonly MemoryMappedFile _file;
        private readonly MemoryMappedViewAccessor _accessor;
        private readonly EventLogPage _log;

        public WhenComplete()
        {
            _filename = Path.GetTempFileName();
            File.WriteAllBytes(_filename, new byte[256]);
            _file = MemoryMappedFile.CreateFromFile(_filename, FileMode.Create, null, 128, MemoryMappedFileAccess.CopyOnWrite);
            _accessor = _file.CreateViewAccessor(0, 128, MemoryMappedFileAccess.CopyOnWrite);
            _log = new EventLogPage(0, true, _accessor);
        }

        public void Dispose()
        {
            _accessor.Dispose();
            _file.Dispose();
            File.Delete(_filename);
        }

        [Fact]
        public void WriteShould_ReturnNull()
        {
            var payloads = new EventPayloads() {
                new ArraySegment<byte>(new byte[] { 0, 1, 2, 3, 4 })
            };

            var task = _log.Write(payloads);
            
            task.ShouldBeNull();
        }
    }
}