using System.Diagnostics.CodeAnalysis;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace Wolverine.SqlServer.Transport;

public partial class SqlServerQueue
{
    internal string? NServiceBusSchema { get; private set; }
    internal string? NServiceBusTableName { get; private set; }
    internal string? NServiceBusConnectionString { get; private set; }

    internal bool IsNServiceBusInterop => NServiceBusSchema != null;

    internal string NServiceBusQualifiedTable =>
        $"[{NServiceBusSchema}].[{NServiceBusTableName ?? Name}]";

    /// <summary>
    /// Configure this SQL Server queue endpoint to interoperate with an existing NServiceBus SQL Server
    /// transport queue. The NServiceBus queue table is assumed to already exist — Wolverine will not
    /// create or migrate it.
    /// </summary>
    /// <remarks>
    /// When the endpoint mode is <see cref="EndpointMode.Durable"/> and both tables share the same
    /// database catalog, messages are atomically moved from the NServiceBus table into Wolverine's
    /// inbox within a single transaction, providing at-least-once delivery guarantees on process crash.
    /// If the NServiceBus table is in a different catalog (or multi-tenancy is active), the listener
    /// falls back to buffered mode with the same at-most-once semantics that NServiceBus itself uses
    /// without TransactionScope.
    /// <para>
    /// NServiceBus serializes message bodies with Newtonsoft.Json by default. To deserialize those
    /// bodies correctly, add <c>WolverineFx.Newtonsoft</c> and configure the endpoint's
    /// <c>DefaultSerializer</c> with <c>new NewtonsoftSerializer(new JsonSerializerSettings())</c>.
    /// Unlike other NServiceBus interop transports, Wolverine does not switch the serializer
    /// automatically here because <c>Wolverine.SqlServer</c> does not take a dependency on
    /// <c>Wolverine.Newtonsoft</c>.
    /// </para>
    /// </remarks>
    /// <param name="schema">Schema containing the NServiceBus queue table. Defaults to "dbo".</param>
    /// <param name="tableName">NServiceBus queue table name. Defaults to the Wolverine queue name.</param>
    /// <param name="connectionString">
    /// Override connection string pointing at the NServiceBus database.
    /// Defaults to the transport's own connection string.
    /// </param>
    [RequiresUnreferencedCode(
        "NServiceBus SQL Server interop uses Type.GetType() for message type resolution and is not trim-safe.")]
    internal void UseNServiceBusInterop(string schema = "dbo", string? tableName = null,
        string? connectionString = null)
    {
        NServiceBusSchema = schema;
        NServiceBusTableName = tableName;
        NServiceBusConnectionString = connectionString;
    }
}
