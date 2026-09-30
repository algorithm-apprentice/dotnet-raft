using DotnetRaft.Core;

namespace DotnetRaft.Tracker;

internal sealed class InflightWindow
{
    private readonly int _capacity;
    private readonly ulong _maxBytes;
    private Inflight[] _buffer = [];
    private ulong _bytes;
    private int _count;
    private int _start;

    internal InflightWindow(int capacity, ulong maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        _capacity = capacity;
        _maxBytes = maxBytes;
    }

    internal int Count => _count;

    internal ulong Bytes => _bytes;

    internal int Capacity => _capacity;

    internal ulong MaxBytes => _maxBytes;

    internal bool IsFull =>
        _count == _capacity
        || (_maxBytes != 0 && _bytes >= _maxBytes);

    internal void Add(ulong lastIndex, ulong bytes)
    {
        if (IsFull)
        {
            throw new RaftInvariantException(
                "Cannot add to a full inflight window.");
        }

        if (_count > 0)
        {
            int newestSlot = GetPhysicalIndex(_count - 1);
            ulong newestIndex = _buffer[newestSlot].LastIndex;
            if (lastIndex <= newestIndex)
            {
                throw new RaftInvariantException(
                    $"Inflight index {lastIndex} does not follow {newestIndex}.");
            }
        }

        if (bytes > ulong.MaxValue - _bytes)
        {
            throw new RaftInvariantException(
                $"Adding {bytes} bytes would overflow inflight byte accounting.");
        }

        int nextSlot = GetPhysicalIndex(_count);
        while (nextSlot >= _buffer.Length)
        {
            Grow();
        }

        _buffer[nextSlot] = new Inflight(lastIndex, bytes);
        _count++;
        _bytes += bytes;
    }

    internal void FreeThrough(ulong acknowledgedIndex)
    {
        if (_count == 0
            || acknowledgedIndex < _buffer[_start].LastIndex)
        {
            return;
        }

        int slot = _start;
        int releasedCount = 0;
        ulong releasedBytes = 0;

        while (releasedCount < _count)
        {
            Inflight inflight = _buffer[slot];
            if (acknowledgedIndex < inflight.LastIndex)
            {
                break;
            }

            releasedBytes += inflight.Bytes;
            releasedCount++;
            slot = IncrementSlot(slot);
        }

        _count -= releasedCount;
        _bytes -= releasedBytes;
        _start = _count == 0 ? 0 : slot;
    }

    internal void Reset()
    {
        _start = 0;
        _count = 0;
        _bytes = 0;
    }

    internal InflightWindow Clone()
    {
        var clone = new InflightWindow(_capacity, _maxBytes)
        {
            _start = _start,
            _count = _count,
            _bytes = _bytes,
            _buffer = (Inflight[])_buffer.Clone(),
        };

        return clone;
    }

    private int GetPhysicalIndex(int logicalOffset)
    {
        long slot = (long)_start + logicalOffset;
        if (slot >= _capacity)
        {
            slot -= _capacity;
        }

        return checked((int)slot);
    }

    private int IncrementSlot(int slot)
    {
        long next = (long)slot + 1;
        if (next >= _capacity)
        {
            next -= _capacity;
        }

        return checked((int)next);
    }

    private void Grow()
    {
        int newSize = _buffer.Length == 0
            ? 1
            : (int)Math.Min(_capacity, (long)_buffer.Length * 2);
        if (newSize <= _buffer.Length)
        {
            throw new RaftInvariantException(
                $"Inflight buffer cannot grow beyond {_buffer.Length} slots.");
        }

        Array.Resize(ref _buffer, newSize);
    }

    private readonly record struct Inflight(
        ulong LastIndex,
        ulong Bytes);
}
