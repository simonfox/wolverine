using System.Diagnostics.CodeAnalysis;
using Wolverine.Configuration;

namespace Wolverine.SqlServer.Transport;

public class SqlServerSubscriberConfiguration : SubscriberConfiguration<SqlServerSubscriberConfiguration, SqlServerQueue>
{
    public SqlServerSubscriberConfiguration(SqlServerQueue endpoint) : base(endpoint)
    {
    }

    /// <summary>
    /// Configure this publisher to send to an existing NServiceBus SQL Server transport queue.
    /// The NServiceBus queue table is assumed to already exist.
    /// </summary>
    /// <param name="schema">Schema containing the NServiceBus queue table. Defaults to "dbo".</param>
    /// <param name="tableName">NServiceBus queue table name. Defaults to the Wolverine queue name.</param>
    /// <param name="connectionString">Override connection string. Defaults to the transport connection string.</param>
    [RequiresUnreferencedCode(
        "NServiceBus SQL Server interop uses Type.GetType() for message type resolution and is not trim-safe.")]
    public SqlServerSubscriberConfiguration UseNServiceBusInterop(string schema = "dbo",
        string? tableName = null, string? connectionString = null)
    {
        add(e => e.UseNServiceBusInterop(schema, tableName, connectionString));
        return this;
    }
}