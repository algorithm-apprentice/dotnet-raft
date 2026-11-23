using System.Text;

namespace DotnetRaft.Protocol;

/// <summary>Owned opaque bytes with value equality, independent of any wire codec.</summary>
public readonly struct RaftBytes : IEquatable<RaftBytes>
{
    private readonly ReadOnlyMemory<byte> data;

    private RaftBytes(byte[] owned) => data = owned;

    public static RaftBytes Empty => default;
    public int Length => data.Length;
    public bool IsEmpty => data.IsEmpty;
    public ReadOnlyMemory<byte> Memory => data;
    public ReadOnlySpan<byte> Span => data.Span;

    public static RaftBytes CopyFrom(ReadOnlySpan<byte> bytes) => bytes.IsEmpty ? default : new(bytes.ToArray());

    public static RaftBytes CopyFromUtf8(string text) => new(Encoding.UTF8.GetBytes(text));

    public byte[] ToByteArray() => data.ToArray();
    public string ToStringUtf8() => Encoding.UTF8.GetString(Span);

    public bool Equals(RaftBytes other) => Span.SequenceEqual(other.Span);
    public override bool Equals(object? obj) => obj is RaftBytes other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(Span);
        return hash.ToHashCode();
    }

    public static bool operator ==(RaftBytes left, RaftBytes right) => left.Equals(right);
    public static bool operator !=(RaftBytes left, RaftBytes right) => !left.Equals(right);
}
