using System.Security.Cryptography;

namespace DotnetRaft.Examples.KvCluster;

/// <summary>The immutable SHA-256 identity of a canonical KV command.</summary>
public sealed class KvCommandFingerprint : IEquatable<KvCommandFingerprint>
{
    private readonly byte[] digest;

    private KvCommandFingerprint(byte[] digest) => this.digest = digest;

    internal static KvCommandFingerprint Compute(ReadOnlySpan<byte> payload) => new(SHA256.HashData(payload));

    internal static KvCommandFingerprint Read(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != SHA256.HashSizeInBytes)
        {
            throw new InvalidDataException("Command fingerprint is not a SHA-256 digest.");
        }

        return new KvCommandFingerprint(bytes.ToArray());
    }

    internal byte[] ToByteArray() => digest.AsSpan().ToArray();

    public bool Equals(KvCommandFingerprint? other) =>
        other is not null && digest.AsSpan().SequenceEqual(other.digest);

    public override bool Equals(object? obj) => obj is KvCommandFingerprint other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(digest);
        return hash.ToHashCode();
    }
}
