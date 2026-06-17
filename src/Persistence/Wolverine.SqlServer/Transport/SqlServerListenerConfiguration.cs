using System.Diagnostics.CodeAnalysis;
using Wolverine.Configuration;
using Wolverine.ErrorHandling;

namespace Wolverine.SqlServer.Transport;

public class SqlServerListenerConfiguration : ListenerConfiguration<SqlServerListenerConfiguration, SqlServerQueue>
{
    public SqlServerListenerConfiguration(SqlServerQueue endpoint) : base(endpoint)
    {
    }

    public SqlServerListenerConfiguration(Func<SqlServerQueue> source) : base(source)
    {
    }

    /// <summary>
    ///     The maximum number of messages to receive in a single batch when listening
    ///     in either buffered or durable modes. The default is 20.
    /// </summary>
    public SqlServerListenerConfiguration MaximumMessagesToReceive(int maximum)
    {
        add(e => e.MaximumMessagesToReceive = maximum);
        return this;
    }

    /// <summary>
    ///     Configure how often to poll for new messages when the queue is idle.
    ///     If not set, falls back to DurabilitySettings.ScheduledJobPollingTime (default 5s).
    /// </summary>
    public SqlServerListenerConfiguration PollingInterval(TimeSpan interval)
    {
        add(e => e.PollingInterval = interval);
        return this;
    }

    /// <summary>
    /// Configure this listener to receive from an existing NServiceBus SQL Server transport queue.
    /// The NServiceBus queue table is assumed to already exist.
    /// </summary>
    /// <param name="schema">Schema containing the NServiceBus queue table. Defaults to "dbo".</param>
    /// <param name="tableName">NServiceBus queue table name. Defaults to the Wolverine queue name.</param>
    /// <param name="connectionString">Override connection string. Defaults to the transport connection string.</param>
    [RequiresUnreferencedCode(
        "NServiceBus SQL Server interop uses Type.GetType() for message type resolution and is not trim-safe.")]
    public SqlServerListenerConfiguration UseNServiceBusInterop(string schema = "dbo",
        string? tableName = null, string? connectionString = null)
    {
        add(e => e.UseNServiceBusInterop(schema, tableName, connectionString));
        return this;
    }

    /// <summary>
    ///     Add circuit breaker exception handling to this listener
    /// </summary>
    /// <param name="configure"></param>
    /// <returns></returns>
    public SqlServerListenerConfiguration CircuitBreaker(Action<CircuitBreakerOptions>? configure = null)
    {
        add(e =>
        {
            e.CircuitBreakerOptions = new CircuitBreakerOptions();
            configure?.Invoke(e.CircuitBreakerOptions);
        });

        return this;
    }
}