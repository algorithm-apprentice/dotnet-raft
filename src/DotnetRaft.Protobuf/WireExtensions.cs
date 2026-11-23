using Google.Protobuf;

namespace DotnetRaft.Protobuf.Wire;

// Only the adapter may see generated extension fields. The core retains their
// serialized form, never a generated object or an UnknownFieldSet reference.
internal interface IWireExtensions
{
    UnknownFieldSet? ExtensionFields { get; set; }
}

internal sealed partial class Entry : IWireExtensions
{
    UnknownFieldSet? IWireExtensions.ExtensionFields { get => _unknownFields; set => _unknownFields = value; }
}

internal sealed partial class SnapshotMetadata : IWireExtensions
{
    UnknownFieldSet? IWireExtensions.ExtensionFields { get => _unknownFields; set => _unknownFields = value; }
}

internal sealed partial class Snapshot : IWireExtensions
{
    UnknownFieldSet? IWireExtensions.ExtensionFields { get => _unknownFields; set => _unknownFields = value; }
}

internal sealed partial class Message : IWireExtensions
{
    UnknownFieldSet? IWireExtensions.ExtensionFields { get => _unknownFields; set => _unknownFields = value; }
}

internal sealed partial class ConfState : IWireExtensions
{
    UnknownFieldSet? IWireExtensions.ExtensionFields { get => _unknownFields; set => _unknownFields = value; }
}

internal sealed partial class ConfChange : IWireExtensions
{
    UnknownFieldSet? IWireExtensions.ExtensionFields { get => _unknownFields; set => _unknownFields = value; }
}

internal sealed partial class ConfChangeSingle : IWireExtensions
{
    UnknownFieldSet? IWireExtensions.ExtensionFields { get => _unknownFields; set => _unknownFields = value; }
}

internal sealed partial class ConfChangeV2 : IWireExtensions
{
    UnknownFieldSet? IWireExtensions.ExtensionFields { get => _unknownFields; set => _unknownFields = value; }
}
