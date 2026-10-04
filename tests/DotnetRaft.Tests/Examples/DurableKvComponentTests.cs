using System.Text;

using DotnetRaft.Examples.KvCluster;
using DotnetRaft.Protocol;
using DotnetRaft.Storage.Sqlite;

using Google.Protobuf;

using Microsoft.Data.Sqlite;

namespace DotnetRaft.Tests.Examples;

public sealed class DurableKvComponentTests
{
    [Fact]
    public void CommandCodecSupportsSetDeleteAndStableFingerprint()
    {
        var set = new KvCommand(
            Guid.Parse(
                "11111111-2222-3333-4444-555555555555"),
            KvCommandType.Set,
            "color",
            "blue");
        var delete = new KvCommand(
            Guid.Parse(
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            KvCommandType.Delete,
            "color",
            null);

        Assert.Equal(
            set,
            DurableKvCommandCodec.Decode(
                DurableKvCommandCodec.Encode(set)));
        Assert.Equal(
            delete,
            DurableKvCommandCodec.Decode(
                DurableKvCommandCodec.Encode(
                    delete)));
        Assert.Equal(
            DurableKvCommandCodec.Fingerprint(set),
            DurableKvCommandCodec.Fingerprint(set));
        Assert.NotEqual(
            DurableKvCommandCodec.Fingerprint(set),
            DurableKvCommandCodec.Fingerprint(
                set with
                {
                    Value = "green",
                }));
        Assert.Throws<InvalidDataException>(
            () => DurableKvCommandCodec.Encode(
                set with
                {
                    RequestId = Guid.Empty,
                }));
        Assert.Throws<InvalidDataException>(
            () => DurableKvCommandCodec.Encode(
                set with
                {
                    Key = " ",
                }));
        Assert.Throws<InvalidDataException>(
            () => DurableKvCommandCodec.Encode(
                set with
                {
                    Value = null,
                }));
        Assert.Throws<InvalidDataException>(
            () => DurableKvCommandCodec.Encode(
                delete with
                {
                    Value = "unexpected",
                }));
        Assert.Throws<InvalidDataException>(
            () => DurableKvCommandCodec.Encode(
                set with
                {
                    Type = (KvCommandType)99,
                }));
        Assert.Throws<InvalidDataException>(
            () => DurableKvCommandCodec.Decode(
                "{\"unknown\":true}"u8));
        string wrongCase =
            System.Text.Encoding.UTF8.GetString(
                    DurableKvCommandCodec.Encode(
                        set))
                .Replace(
                    "\"requestId\"",
                    "\"RequestId\"",
                    StringComparison.Ordinal);
        Assert.Throws<InvalidDataException>(
            () => DurableKvCommandCodec.Decode(
                System.Text.Encoding.UTF8
                    .GetBytes(wrongCase)));
    }

    [Fact]
    public void ApplicationStorePersistsMutationsAndDeduplication()
    {
        using var directory =
            new TemporaryDirectory();
        Guid setId = Guid.Parse(
            "11111111-2222-3333-4444-555555555555");
        var set = new KvCommand(
            setId,
            KvCommandType.Set,
            "color",
            "blue");
        var delete = new KvCommand(
            Guid.Parse(
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            KvCommandType.Delete,
            "color",
            null);

        using (var state =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
            Assert.True(state.WasCreated);
            state.AdvanceNoOp(1);
            KvApplyResult first =
                state.ApplyCommand(
                    2,
                    ByteString.CopyFrom(
                        DurableKvCommandCodec.Encode(
                            set)));
            KvApplyResult duplicate =
                state.ApplyCommand(
                    3,
                    ByteString.CopyFrom(
                        DurableKvCommandCodec.Encode(
                            set)));
            KvApplyResult conflict =
                state.ApplyCommand(
                    4,
                    ByteString.CopyFrom(
                        DurableKvCommandCodec.Encode(
                            set with
                            {
                                Value = "green",
                            })));
            state.ApplyCommand(
                5,
                ByteString.CopyFrom(
                    DurableKvCommandCodec.Encode(
                        delete)));

            Assert.False(first.Duplicate);
            Assert.Equal(2UL, first.ResultIndex);
            Assert.True(duplicate.Duplicate);
            Assert.Equal(2UL, duplicate.ResultIndex);
            Assert.True(conflict.Conflict);
            Assert.False(
                state.ReadLocal("color").Found);
        }

        using var reopened =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        Assert.False(reopened.WasCreated);
        Assert.Equal(5UL, reopened.PhysicalApplied);
        KvRequestResolution resolution =
            Assert.IsType<KvRequestResolution>(
                reopened.ResolveRequest(set));
        Assert.Equal(2UL, resolution.ResultIndex);
        Assert.False(resolution.Conflict);
        Assert.True(
            Assert.IsType<KvRequestResolution>(
                reopened.ResolveRequest(
                    set with
                    {
                        Value = "other",
                    }))
            .Conflict);
    }

    [Fact]
    public void ApplicationSchemaMatchesApproval()
    {
        using var directory =
            new TemporaryDirectory();
        using (var state =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
        }

        var actual = new StringBuilder();
        var builder =
            new SqliteConnectionStringBuilder
            {
                DataSource =
                    directory.ApplicationPath,
                Mode = SqliteOpenMode.ReadOnly,
                Pooling = false,
            };
        using (var connection =
               new SqliteConnection(
                   builder.ToString()))
        {
            connection.Open();
            using (SqliteCommand version =
                   connection.CreateCommand())
            {
                version.CommandText =
                    "PRAGMA user_version;";
                actual.Append("user_version=")
                    .Append(
                        version.ExecuteScalar())
                    .Append("\n\n");
            }

            using SqliteCommand schema =
                connection.CreateCommand();
            schema.CommandText =
                """
                SELECT type, name, sql
                FROM sqlite_schema
                WHERE name NOT LIKE 'sqlite_%'
                ORDER BY type, name;
                """;
            using SqliteDataReader reader =
                schema.ExecuteReader();
            while (reader.Read())
            {
                actual.Append(reader.GetString(0))
                    .Append(' ')
                    .Append(reader.GetString(1))
                    .Append('\n')
                    .Append(reader.GetString(2))
                    .Append("\n\n");
            }
        }

        string approved = Path.Combine(
            AppContext.BaseDirectory,
            "Examples",
            "TestData",
            "DurableApplicationSchema.approved.sql");
        Assert.Equal(
            File.ReadAllText(approved)
                .Replace(
                    "\r\n",
                    "\n",
                    StringComparison.Ordinal),
            actual.ToString()
                .TrimEnd('\n')
                + '\n');
    }

    [Fact]
    public void ApplicationSnapshotIsDeterministicAndIdempotent()
    {
        using var directory =
            new TemporaryDirectory();
        var confState = new ConfState();
        confState.Voters.Add([1, 2, 3]);
        ByteString snapshot;
        using (var state =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
            state.ApplyConfiguration(
                1,
                confState);
            state.ApplyCommand(
                2,
                ByteString.CopyFrom(
                    DurableKvCommandCodec.Encode(
                        new KvCommand(
                            Guid.Parse(
                                "11111111-2222-3333-4444-555555555555"),
                            KvCommandType.Set,
                            "b",
                            "2"))));
            state.ApplyCommand(
                3,
                ByteString.CopyFrom(
                    DurableKvCommandCodec.Encode(
                        new KvCommand(
                            Guid.Parse(
                                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                            KvCommandType.Set,
                            "a",
                            "1"))));
            snapshot = state.CreateSnapshotData();
            Assert.Equal(
                snapshot,
                state.CreateSnapshotData());
        }

        using var restoreDirectory =
            new TemporaryDirectory();
        using var restored =
            new SqliteKeyValueStateMachine(
                restoreDirectory.ApplicationPath,
                nodeId: 1);
        restored.Restore(
            3,
            snapshot,
            confState);
        restored.Restore(
            3,
            snapshot,
            confState);

        Assert.Equal("1",
            restored.ReadLocal("a").Value);
        Assert.Equal("2",
            restored.ReadLocal("b").Value);
        Assert.Equal(3UL, restored.PhysicalApplied);
        Assert.Equal(
            confState,
            restored.ConfState);
    }

    [Fact]
    public void ApplicationSnapshotUsesOneUnicodeOrdering()
    {
        using var directory =
            new TemporaryDirectory();
        var configuration = new ConfState();
        configuration.Voters.Add(1);
        ByteString snapshot;
        using (var state =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
            state.ApplyConfiguration(
                1,
                configuration);
            state.ApplyCommand(
                2,
                ByteString.CopyFrom(
                    DurableKvCommandCodec.Encode(
                        new KvCommand(
                            Guid.Parse(
                                "11111111-2222-3333-4444-555555555555"),
                            KvCommandType.Set,
                            "\uE000",
                            "bmp"))));
            state.ApplyCommand(
                3,
                ByteString.CopyFrom(
                    DurableKvCommandCodec.Encode(
                        new KvCommand(
                            Guid.Parse(
                                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                            KvCommandType.Set,
                            "\U00010000",
                            "supplementary"))));
            snapshot = state.CreateSnapshotData();
        }

        using var restoreDirectory =
            new TemporaryDirectory();
        using var restored =
            new SqliteKeyValueStateMachine(
                restoreDirectory.ApplicationPath,
                nodeId: 1);
        restored.Restore(
            3,
            snapshot,
            configuration);
        Assert.Equal(
            "bmp",
            restored.ReadLocal("\uE000").Value);
        Assert.Equal(
            "supplementary",
            restored.ReadLocal("\U00010000").Value);
    }

    [Fact]
    public void ApplicationStoreRejectsWrongNodeId()
    {
        using var directory =
            new TemporaryDirectory();
        using (var state =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
            state.AdvanceNoOp(1);
        }

        Assert.Throws<InvalidDataException>(
            () => _ =
                new SqliteKeyValueStateMachine(
                    directory.ApplicationPath,
                    nodeId: 2));
    }

    [Fact]
    public void ApplicationStoreEnforcesCursorAndReadBarrier()
    {
        using var directory =
            new TemporaryDirectory();
        using var state =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);

        Assert.Throws<InvalidOperationException>(
            () => state.AdvanceNoOp(2));
        Assert.Throws<InvalidOperationException>(
            () => state.EnsureApplied(1));
        Assert.Throws<InvalidOperationException>(
            () => state.ReadAtLeast(
                "key",
                1));
        state.AdvanceNoOp(1);
        state.EnsureApplied(1);
        Assert.False(
            state.ReadAtLeast(
                "key",
                1).Found);
        Assert.Throws<InvalidOperationException>(
            () => state.AdvanceNoOp(1));
    }

    [Fact]
    public void ApplicationRestoreRejectsMismatchedMetadata()
    {
        using var sourceDirectory =
            new TemporaryDirectory();
        var configuration = new ConfState();
        configuration.Voters.Add(1);
        ByteString snapshot;
        using (var source =
               new SqliteKeyValueStateMachine(
                   sourceDirectory.ApplicationPath,
                   nodeId: 1))
        {
            source.ApplyConfiguration(
                1,
                configuration);
            snapshot = source.CreateSnapshotData();
        }

        using var targetDirectory =
            new TemporaryDirectory();
        using var target =
            new SqliteKeyValueStateMachine(
                targetDirectory.ApplicationPath,
                nodeId: 1);
        Assert.Throws<InvalidDataException>(
            () => target.Restore(
                2,
                snapshot,
                configuration));
        var wrongConfiguration =
            new ConfState();
        wrongConfiguration.Voters.Add(2);
        Assert.Throws<InvalidDataException>(
            () => target.Restore(
                1,
                snapshot,
                wrongConfiguration));
        target.Restore(
            1,
            snapshot,
            configuration);
        target.AdvanceNoOp(2);
        Assert.Throws<InvalidOperationException>(
            () => target.Restore(
                1,
                snapshot,
                configuration));

        string wrongVersion =
            snapshot.ToStringUtf8()
                .Replace(
                    "\"version\":1",
                    "\"version\":2",
                    StringComparison.Ordinal);
        using var versionDirectory =
            new TemporaryDirectory();
        using var versionTarget =
            new SqliteKeyValueStateMachine(
                versionDirectory.ApplicationPath,
                nodeId: 1);
        Assert.Throws<InvalidDataException>(
            () => versionTarget.Restore(
                1,
                ByteString.CopyFromUtf8(
                    wrongVersion),
                configuration));
    }

    [Fact]
    public async Task PendingProposalRegistryMatchesFingerprint()
    {
        var registry =
            new PendingProposalRegistry();
        var command = new KvCommand(
            Guid.Parse(
                "11111111-2222-3333-4444-555555555555"),
            KvCommandType.Set,
            "color",
            "blue");
        using PendingProposalRegistry
            .PendingProposalRegistration first =
            registry.Register(command);
        using PendingProposalRegistry
            .PendingProposalRegistration second =
            registry.Register(command);
        Assert.Same(first.Task, second.Task);
        Assert.True(first.IsOwner);
        Assert.False(second.IsOwner);
        Assert.Throws<KvRequestConflictException>(
            () => registry.Register(
                command with
                {
                    Value = "green",
                }));

        var winner = command with
        {
            Value = "red",
        };
        Assert.True(
            registry.Complete(
                new KvApplyResult(
                    command.RequestId,
                    DurableKvCommandCodec
                        .Fingerprint(winner),
                    ResultIndex: 4,
                    PhysicalApplied: 4,
                    Duplicate: false,
                    Conflict: false)));
        KvApplyResult result =
            await first.Task;
        Assert.True(result.Conflict);
        Assert.False(result.Duplicate);

        PendingProposalRegistry
            .PendingProposalRegistration abandonedFirst =
            registry.Register(
                command with
                {
                    RequestId = Guid.Parse(
                        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                });
        PendingProposalRegistry
            .PendingProposalRegistration abandonedSecond =
            registry.Register(
                command with
                {
                    RequestId = Guid.Parse(
                        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                });
        PendingProposalRegistry
            .PendingProposalSubmission
            abandonedSubmission =
                Assert.IsType<
                    PendingProposalRegistry
                        .PendingProposalSubmission>(
                    abandonedFirst
                        .BeginSubmission());
        abandonedFirst.Dispose();
        abandonedSecond.Dispose();
        Assert.True(
            abandonedSubmission
                .CancellationToken
                .IsCancellationRequested);
        using PendingProposalRegistry
            .PendingProposalRegistration replacement =
            registry.Register(
                command with
                {
                    RequestId = Guid.Parse(
                        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                    Value = "replacement",
                });
        Assert.True(replacement.IsOwner);
    }

    [Fact]
    public async Task PendingProposalRegistryRetainsSharedWaiter()
    {
        var registry =
            new PendingProposalRegistry();
        var command = new KvCommand(
            Guid.Parse(
                "11111111-2222-3333-4444-555555555555"),
            KvCommandType.Set,
            "color",
            "blue");
        PendingProposalRegistry
            .PendingProposalRegistration first =
            registry.Register(command);
        using PendingProposalRegistry
            .PendingProposalRegistration second =
            registry.Register(command);
        PendingProposalRegistry
            .PendingProposalSubmission submission =
            Assert.IsType<
                PendingProposalRegistry
                    .PendingProposalSubmission>(
                first.BeginSubmission());
        first.Dispose();
        Assert.False(
            submission.CancellationToken
                .IsCancellationRequested);

        KvApplyResult applied = new(
            command.RequestId,
            DurableKvCommandCodec.Fingerprint(
                command),
            ResultIndex: 7,
            PhysicalApplied: 7,
            Duplicate: false,
            Conflict: false);
        Assert.True(registry.Complete(applied));
        Assert.True(
            submission.CancellationToken
                .IsCancellationRequested);
        Assert.Equal(
            7UL,
            (await second.Task).ResultIndex);

        var abandonedCommand =
            command with
            {
                RequestId = Guid.Parse(
                    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            };
        PendingProposalRegistry
            .PendingProposalRegistration abandoned =
            registry.Register(abandonedCommand);
        abandoned.Dispose();
        Assert.False(
            registry.Complete(
                applied with
                {
                    RequestId =
                        abandonedCommand.RequestId,
                }));
    }

    [Fact]
    public async Task RestoredRequestCompletesPendingProposal()
    {
        using var directory =
            new TemporaryDirectory();
        using var state =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        var registry =
            new PendingProposalRegistry();
        var command = new KvCommand(
            Guid.Parse(
                "11111111-2222-3333-4444-555555555555"),
            KvCommandType.Set,
            "color",
            "blue");
        using PendingProposalRegistry
            .PendingProposalRegistration registration =
            registry.Register(command);
        _ = state.ApplyCommand(
            1,
            ByteString.CopyFrom(
                DurableKvCommandCodec.Encode(
                    command)));

        Assert.Equal(
            1,
            registry.CompleteResolved(
                state.ResolveAppliedRequest));
        KvApplyResult result =
            await registration.Task;
        Assert.True(result.Duplicate);
        Assert.False(result.Conflict);
        Assert.Equal(1UL, result.ResultIndex);
        Assert.Equal(1UL, result.PhysicalApplied);
    }

    [Fact]
    public void RecoveryCreatesMatchingSnapshotWhenApplicationIsAhead()
    {
        using var directory =
            new TemporaryDirectory();
        var peers =
            new FixedMembershipConfiguration(
                [1UL, 2UL, 3UL]);
        using (var raft =
               new SqliteStorage(
                   directory.RaftPath))
        using (var application =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                });
            raft.Append(
            [
                new Entry
                {
                    Index = 1,
                    Term = 1,
                },
                new Entry
                {
                    Index = 2,
                    Term = 2,
                },
            ]);
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                    Commit = 2,
                });
            var first = new ConfState();
            first.Voters.Add(1);
            application.ApplyConfiguration(
                1,
                first);
            var second = new ConfState();
            second.Voters.Add([1, 2]);
            application.ApplyConfiguration(
                2,
                second);
        }

        using var reopenedRaft =
            new SqliteStorage(directory.RaftPath);
        using var reopenedApplication =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        DurableRecoveryState recovery =
            DurableHostRecovery.Reconcile(
                reopenedRaft,
                reopenedApplication,
                peers);

        Assert.False(recovery.StartNew);
        Assert.Equal(2UL, recovery.Applied);
        Assert.Equal(
            2UL,
            reopenedRaft.GetSnapshot()
                .Metadata.Index);
        Assert.Equal(
            reopenedApplication.ConfState,
            reopenedRaft.GetSnapshot()
                .Metadata.ConfState);
        Assert.Equal(
            3UL,
            reopenedRaft.GetFirstIndex());
    }

    [Fact]
    public void RecoveryRestoresRetainedSnapshotWhenApplicationIsBehind()
    {
        using var donorDirectory =
            new TemporaryDirectory();
        var first = new ConfState();
        first.Voters.Add(1);
        var confState = new ConfState();
        confState.Voters.Add([1, 2]);
        ByteString data;
        using (var donor =
               new SqliteKeyValueStateMachine(
                   donorDirectory.ApplicationPath,
                   nodeId: 1))
        {
            donor.ApplyConfiguration(1, first);
            donor.ApplyConfiguration(2, confState);
            data = donor.CreateSnapshotData();
        }

        using var directory =
            new TemporaryDirectory();
        using (var application =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
        }

        using (var raft =
               new SqliteStorage(directory.RaftPath))
        {
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                });
            raft.Append(
            [
                new Entry
                {
                    Index = 1,
                    Term = 1,
                },
                new Entry
                {
                    Index = 2,
                    Term = 2,
                },
            ]);
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                    Commit = 2,
                });
            raft.CreateSnapshot(
                2,
                confState,
                data);
            raft.Compact(2);
        }

        using var reopenedRaft =
            new SqliteStorage(directory.RaftPath);
        using var reopenedApplication =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        DurableRecoveryState recovery =
            DurableHostRecovery.Reconcile(
                reopenedRaft,
                reopenedApplication,
                new FixedMembershipConfiguration(
                    [1UL, 2UL, 3UL]));

        Assert.Equal(2UL, recovery.Applied);
        Assert.Equal(
            2UL,
            reopenedApplication.PhysicalApplied);
        Assert.Equal(
            confState,
            reopenedApplication.ConfState);
    }

    [Fact]
    public void RecoveryRejectsRecreatedApplicationBesideRaftState()
    {
        using var directory =
            new TemporaryDirectory();
        using (var raft =
               new SqliteStorage(directory.RaftPath))
        {
            raft.SetHardState(
                new HardState
                {
                    Term = 1,
                });
            raft.Append(
            [
                new Entry
                {
                    Index = 1,
                    Term = 1,
                },
            ]);
            raft.SetHardState(
                new HardState
                {
                    Term = 1,
                    Commit = 1,
                });
        }

        using var reopenedRaft =
            new SqliteStorage(directory.RaftPath);
        Assert.Throws<InvalidDataException>(
            () => DurableHostRecovery.OpenApplication(
                reopenedRaft,
                directory.ApplicationPath,
                nodeId: 1));
        Assert.False(
            File.Exists(
                directory.ApplicationPath));
        Assert.Throws<InvalidDataException>(
            () => DurableHostRecovery.OpenApplication(
                reopenedRaft,
                directory.ApplicationPath,
                nodeId: 2));
        Assert.False(
            File.Exists(
                directory.ApplicationPath));
    }

    [Fact]
    public void RecoveryRejectsReplayBehindCompactedPrefix()
    {
        using var directory =
                new TemporaryDirectory();
        using (var application =
                   new SqliteKeyValueStateMachine(
                       directory.ApplicationPath,
                       nodeId: 1))
        {
        }

        using (var raft =
                   new SqliteStorage(directory.RaftPath))
        {
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                });
            raft.Append(
            [
                new Entry
                    {
                        Index = 1,
                        Term = 1,
                    },
                    new Entry
                    {
                        Index = 2,
                        Term = 2,
                    },
                ]);
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                    Commit = 2,
                });
            raft.Compact(1);
        }

        using var reopenedRaft =
                new SqliteStorage(directory.RaftPath);
        using var reopenedApplication =
                new SqliteKeyValueStateMachine(
                    directory.ApplicationPath,
                    nodeId: 1);
        Assert.Throws<InvalidDataException>(
                () => DurableHostRecovery.Reconcile(
                    reopenedRaft,
                    reopenedApplication,
                    new FixedMembershipConfiguration(
                        [1UL, 2UL, 3UL])));
    }

    [Fact]
    public void RecoveryRepairsCommitAtApplicationBoundary()
    {
        using var directory =
            new TemporaryDirectory();
        var first = new ConfState();
        first.Voters.Add(1);
        using (var application =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
            application.ApplyConfiguration(
                1,
                first);
        }

        using (var raft =
               new SqliteStorage(directory.RaftPath))
        {
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                });
            raft.Append(
            [
                new Entry
                {
                        Index = 1,
                        Term = 1,
                },
                new Entry
                {
                        Index = 2,
                        Term = 2,
                },
            ]);
        }

        using var reopenedRaft =
            new SqliteStorage(directory.RaftPath);
        using var reopenedApplication =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        DurableRecoveryState result =
            DurableHostRecovery.Reconcile(
                reopenedRaft,
                reopenedApplication,
                new FixedMembershipConfiguration(
                        [1UL, 2UL, 3UL]));

        Assert.Equal(1UL, result.Applied);
        Assert.Equal(
            1UL,
            reopenedRaft.GetHardState()?.Commit);
        Assert.Equal(
            1UL,
            reopenedRaft.GetSnapshot()
                .Metadata.Index);
    }

    [Fact]
    public void RecoveryRejectsApplicationBeyondRaftLog()
    {
        using var directory =
            new TemporaryDirectory();
        var first = new ConfState();
        first.Voters.Add(1);
        var second = new ConfState();
        second.Voters.Add([1, 2]);
        using (var application =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
            application.ApplyConfiguration(
                1,
                first);
            application.ApplyConfiguration(
                2,
                second);
        }

        using (var raft =
               new SqliteStorage(directory.RaftPath))
        {
            raft.SetHardState(
                new HardState
                {
                    Term = 1,
                });
            raft.Append(
            [
                new Entry
                {
                        Index = 1,
                        Term = 1,
                },
            ]);
            raft.SetHardState(
                new HardState
                {
                    Term = 1,
                    Commit = 1,
                });
        }

        using var reopenedRaft =
            new SqliteStorage(directory.RaftPath);
        using var reopenedApplication =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        Assert.Throws<InvalidDataException>(
            () => DurableHostRecovery.Reconcile(
                reopenedRaft,
                reopenedApplication,
                new FixedMembershipConfiguration(
                        [1UL, 2UL, 3UL])));
    }

    [Fact]
    public void RecoveryPreservesTransportableSnapshotWhenNewOneIsOversize()
    {
        using var directory =
            new TemporaryDirectory();
        var first = new ConfState();
        first.Voters.Add(1);
        var second = new ConfState();
        second.Voters.Add([1, 2]);
        var third = new ConfState();
        third.Voters.Add([1, 2, 3]);
        ByteString snapshotAtThree;
        using (var application =
               new SqliteKeyValueStateMachine(
                   directory.ApplicationPath,
                   nodeId: 1))
        {
            application.ApplyConfiguration(1, first);
            application.ApplyConfiguration(2, second);
            application.ApplyConfiguration(3, third);
            snapshotAtThree =
                application.CreateSnapshotData();
            application.ApplyCommand(
                4,
                ByteString.CopyFrom(
                        DurableKvCommandCodec.Encode(
                            new KvCommand(
                                Guid.Parse(
                                    "11111111-2222-3333-4444-555555555555"),
                                KvCommandType.Set,
                                "large",
                                new string('x', 4096)))));
        }

        using (var raft =
               new SqliteStorage(directory.RaftPath))
        {
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                });
            raft.Append(
            [
                new Entry
                {
                        Index = 1,
                        Term = 1,
                },
                new Entry
                {
                        Index = 2,
                        Term = 1,
                },
                new Entry
                {
                        Index = 3,
                        Term = 1,
                },
                new Entry
                {
                        Index = 4,
                        Term = 2,
                },
            ]);
            raft.SetHardState(
                new HardState
                {
                    Term = 2,
                    Commit = 4,
                });
            raft.CreateSnapshot(
                3,
                third,
                snapshotAtThree);
            raft.Compact(3);
        }

        using var reopenedRaft =
            new SqliteStorage(directory.RaftPath);
        using var reopenedApplication =
            new SqliteKeyValueStateMachine(
                directory.ApplicationPath,
                nodeId: 1);
        DurableRecoveryState result =
            DurableHostRecovery.Reconcile(
                reopenedRaft,
                reopenedApplication,
                new FixedMembershipConfiguration(
                        [1UL, 2UL, 3UL]),
                maximumSnapshotBytes: 1024);

        Assert.Equal(4UL, result.Applied);
        Assert.Equal(
            3UL,
            reopenedRaft.GetSnapshot()
                .Metadata.Index);
        Assert.Equal(4UL, reopenedRaft.GetFirstIndex());
        var restarted = DotnetRaft.RawNode.Restart(
            new RaftConfig
            {
                Id = 1,
                ElectionTick = 10,
                HeartbeatTick = 1,
                Storage = reopenedRaft,
                Applied = result.Applied,
            });
        Assert.Equal(
            4UL,
            restarted.GetStatus().Basic.Applied);
    }

    [Theory]
    [InlineData("raft-committed")]
    [InlineData("application-restored")]
    [InlineData("commit-repaired")]
    [InlineData("acknowledged")]
    public void PendingSnapshotRecoveryIsIdempotent(
        string phase)
    {
        using var donorDirectory =
                new TemporaryDirectory();
        var first = new ConfState();
        first.Voters.Add(1);
        var second = new ConfState();
        second.Voters.Add([1, 2]);
        var third = new ConfState();
        third.Voters.Add([1, 2, 3]);
        ByteString data;
        using (var donor =
                   new SqliteKeyValueStateMachine(
                       donorDirectory.ApplicationPath,
                       nodeId: 1))
        {
            donor.ApplyConfiguration(1, first);
            donor.ApplyConfiguration(2, second);
            donor.ApplyConfiguration(3, third);
            data = donor.CreateSnapshotData();
        }

        using var directory =
                new TemporaryDirectory();
        using (var initialize =
                   new SqliteKeyValueStateMachine(
                       directory.ApplicationPath,
                       nodeId: 1))
        {
        }

        using (var raft =
                   new SqliteStorage(directory.RaftPath))
        using (var application =
                   new SqliteKeyValueStateMachine(
                       directory.ApplicationPath,
                       nodeId: 1))
        {
            raft.SetHardState(
                new HardState
                {
                    Term = 3,
                    Vote = 2,
                });
            Snapshot snapshot = new()
            {
                Metadata = new SnapshotMetadata
                {
                    Index = 3,
                    Term = 3,
                    ConfState = third.Clone(),
                },
                Data = data,
            };
            raft.ApplySnapshot(snapshot);
            if (phase is
                "application-restored"
                or "commit-repaired"
                or "acknowledged")
            {
                application.Restore(
                    3,
                    data,
                    third);
            }

            if (phase is
                "commit-repaired"
                or "acknowledged")
            {
                raft.SetHardState(
                    new HardState
                    {
                        Term = 3,
                        Vote = 2,
                        Commit = 3,
                    });
            }

            if (phase == "acknowledged")
            {
                raft.AcknowledgeApplicationSnapshot(
                    3);
            }
        }

        using var reopenedRaft =
                new SqliteStorage(directory.RaftPath);
        using var reopenedApplication =
                new SqliteKeyValueStateMachine(
                    directory.ApplicationPath,
                    nodeId: 1);
        DurableRecoveryState recovery =
                DurableHostRecovery.Reconcile(
                    reopenedRaft,
                    reopenedApplication,
                    new FixedMembershipConfiguration(
                        [1UL, 2UL, 3UL]));

        Assert.Equal(3UL, recovery.Applied);
        Assert.Null(
                reopenedRaft
                    .GetPendingApplicationSnapshot());
        HardState hardState =
                Assert.IsType<HardState>(
                    reopenedRaft.GetHardState());
        Assert.Equal(3UL, hardState.Term);
        Assert.Equal(2UL, hardState.Vote);
        Assert.Equal(3UL, hardState.Commit);
        Assert.Equal(3UL,
                reopenedApplication.PhysicalApplied);
    }

    private sealed class TemporaryDirectory
        : IDisposable
    {
        internal TemporaryDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "dotnet-raft-durable-kv-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path);
        }

        internal string Path { get; }

        internal string RaftPath =>
            System.IO.Path.Combine(
                Path,
                "raft.db");

        internal string ApplicationPath =>
            System.IO.Path.Combine(
                Path,
                "application.db");

        public void Dispose()
        {
            if (Directory.Exists(Path))
            {
                Directory.Delete(
                    Path,
                    recursive: true);
            }
        }
    }
}
