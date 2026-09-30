using DotnetRaft.Core;
using DotnetRaft.Protocol;

using Google.Protobuf;

namespace DotnetRaft.Tests.Core;

public sealed class CoreUtilityTests
{
    [Fact]
    public void EntryIdUsesTermAndIndex()
    {
        var entry = new Entry
        {
            Term = 10,
            Index = 123,
            Data = ByteString.CopyFromUtf8("data"),
        };

        Assert.Equal(new EntryId(10, 123), EntryId.From(entry));
        Assert.NotEqual(new EntryId(9, 123), EntryId.From(entry));
        Assert.NotEqual(new EntryId(10, 122), EntryId.From(entry));
    }

    [Fact]
    public void LogSliceValidatesContiguousIndexesAndMonotonicTerms()
    {
        var cases = new[]
        {
            new LogSliceCase(0, new EntryId(0, 0), [], true, new EntryId(0, 0)),
            new LogSliceCase(0, new EntryId(10, 123), [], false, default),
            new LogSliceCase(10, new EntryId(10, 123), [], true, new EntryId(10, 123)),
            new LogSliceCase(1, new EntryId(0, 0), Entries((1, 1)), true, new EntryId(1, 1)),
            new LogSliceCase(0, new EntryId(0, 0), Entries((1, 1)), false, default),
            new LogSliceCase(2, new EntryId(1, 1), Entries((2, 1), (3, 1), (4, 2)), true, new EntryId(2, 4)),
            new LogSliceCase(1, new EntryId(1, 1), Entries((2, 1), (3, 1), (4, 2)), false, default),
            new LogSliceCase(10, new EntryId(5, 123), Entries((124, 6)), true, new EntryId(6, 124)),
            new LogSliceCase(10, new EntryId(5, 123), Entries((111, 5)), false, default),
            new LogSliceCase(10, new EntryId(2, 12), Entries((13, 2), (15, 2)), false, default),
            new LogSliceCase(10, new EntryId(2, 12), Entries((13, 2), (14, 1)), false, default),
            new LogSliceCase(10, new EntryId(2, 12), Entries((13, 2), (14, 3)), true, new EntryId(3, 14)),
        };

        foreach (var testCase in cases)
        {
            var slice = new LogSlice(testCase.Term, testCase.Previous, testCase.Entries);

            if (!testCase.IsValid)
            {
                Assert.Throws<RaftInvariantException>(slice.Validate);
                continue;
            }

            slice.Validate();
            Assert.Equal(testCase.Last, slice.LastEntryId);
            Assert.Equal(testCase.Last.Index, slice.LastIndex);
        }
    }

    [Fact]
    public void LimitSizeAlwaysReturnsTheFirstEntry()
    {
        var entries = Entries((4, 4), (5, 5), (6, 6));
        var firstTwoSize = (ulong)(entries[0].CalculateSize() + entries[1].CalculateSize());

        Assert.Equal([entries[0]], EntrySizing.LimitSize(entries, 0));
        Assert.Equal([entries[0], entries[1]], EntrySizing.LimitSize(entries, firstTwoSize));
        Assert.Equal(entries, EntrySizing.LimitSize(entries, ulong.MaxValue));
    }

    [Fact]
    public void EmptyEntryHasZeroPayloadSize()
    {
        Assert.Equal(0UL, EntrySizing.PayloadSize(new Entry()));
    }

    [Fact]
    public void CloneEntriesDoesNotShareMessages()
    {
        var original = Entries((1, 1));
        original[0].Data = ByteString.CopyFromUtf8("original");

        var clone = ProtocolCloning.CloneEntries(original);
        clone[0].Term = 2;
        clone[0].Data = ByteString.CopyFromUtf8("clone");

        Assert.Equal(1UL, original[0].Term);
        Assert.Equal("original", original[0].Data.ToStringUtf8());
    }

    [Fact]
    public void MessageClassifiersMatchTheReferenceSets()
    {
        var local = new HashSet<MessageType>
        {
            MessageType.MsgHup,
            MessageType.MsgBeat,
            MessageType.MsgUnreachable,
            MessageType.MsgSnapStatus,
            MessageType.MsgCheckQuorum,
            MessageType.MsgStorageAppend,
            MessageType.MsgStorageAppendResp,
            MessageType.MsgStorageApply,
            MessageType.MsgStorageApplyResp,
        };
        var responses = new HashSet<MessageType>
        {
            MessageType.MsgAppResp,
            MessageType.MsgVoteResp,
            MessageType.MsgHeartbeatResp,
            MessageType.MsgUnreachable,
            MessageType.MsgReadIndexResp,
            MessageType.MsgPreVoteResp,
            MessageType.MsgStorageAppendResp,
            MessageType.MsgStorageApplyResp,
        };

        foreach (var type in Enum.GetValues<MessageType>())
        {
            Assert.Equal(local.Contains(type), MessageClassifier.IsLocal(type));
            Assert.Equal(responses.Contains(type), MessageClassifier.IsResponse(type));
        }
    }

    [Fact]
    public void VoteMessagesMapToTheirResponseTypes()
    {
        Assert.Equal(MessageType.MsgVoteResp, MessageClassifier.VoteResponseType(MessageType.MsgVote));
        Assert.Equal(MessageType.MsgPreVoteResp, MessageClassifier.VoteResponseType(MessageType.MsgPreVote));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => MessageClassifier.VoteResponseType(MessageType.MsgApp));
    }

    [Fact]
    public void LocalMessageTargetsAreReserved()
    {
        Assert.True(RaftMessageTargets.IsLocal(RaftMessageTargets.LocalAppendThread));
        Assert.True(RaftMessageTargets.IsLocal(RaftMessageTargets.LocalApplyThread));
        Assert.False(RaftMessageTargets.IsLocal(RaftMessageTargets.None));
        Assert.False(RaftMessageTargets.IsLocal(1));
    }

    private static Entry[] Entries(params (ulong Index, ulong Term)[] values)
    {
        return values.Select(value => new Entry
        {
            Index = value.Index,
            Term = value.Term,
        }).ToArray();
    }

    private sealed record LogSliceCase(
        ulong Term,
        EntryId Previous,
        Entry[] Entries,
        bool IsValid,
        EntryId Last);
}
