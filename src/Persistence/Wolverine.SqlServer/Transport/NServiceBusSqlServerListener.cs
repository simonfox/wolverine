using System.Diagnostics.CodeAnalysis;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Weasel.SqlServer;
using Wolverine.Configuration;
using Wolverine.RDBMS;
using Wolverine.Runtime;
using Wolverine.Runtime.Serialization;
using Wolverine.Transports;

namespace Wolverine.SqlServer.Transport;

internal class NServiceBusSqlServerListener : IListener
{
    private readonly CancellationTokenSource _cancellation = new();
    private readonly string _connectionString;
    private readonly ILogger<NServiceBusSqlServerListener> _logger;
    private readonly TimeSpan _pollingInterval;
    private readonly SqlServerQueue _queue;
    private readonly string _qualifiedTable;
    private readonly IReceiver _receiver;
    private readonly DurabilitySettings _durabilitySettings;
    private readonly bool _useDurableInbox;
    private readonly string _inboxTable;
    private readonly string _popSql;
    private readonly string _durablePopSql;
    private Task? _task;

    internal readonly NServiceBusSqlServerSender Sender;

    [RequiresUnreferencedCode(
        "NServiceBus SQL Server interop uses Type.GetType() for message type resolution and is not trim-safe.")]
    public NServiceBusSqlServerListener(SqlServerQueue queue, IWolverineRuntime runtime, IReceiver receiver,
        string connectionString, string qualifiedTable)
    {
        Address = queue.Uri;
        _queue = queue;
        _receiver = receiver;
        _connectionString = connectionString;
        _qualifiedTable = qualifiedTable;
        _logger = runtime.LoggerFactory.CreateLogger<NServiceBusSqlServerListener>();
        _pollingInterval = queue.PollingInterval ?? runtime.DurabilitySettings.ScheduledJobPollingTime;
        _durabilitySettings = runtime.DurabilitySettings;
        _inboxTable = $"{queue.Parent.MessageStorageSchemaName}.{DatabaseConstants.IncomingTable}";

        Sender = new NServiceBusSqlServerSender(queue, connectionString, qualifiedTable);

        _useDurableInbox = queue.Mode == EndpointMode.Durable && canUseDurableInbox();

        if (queue.Mode == EndpointMode.Durable && !_useDurableInbox)
        {
            _logger.LogWarning(
                "NServiceBus SQL Server interop queue {Queue} is configured as Durable but the NServiceBus " +
                "catalog differs from Wolverine's message storage, or multi-tenancy is active. " +
                "Falling back to buffered mode — messages may be lost if the process crashes between " +
                "receiving and processing.",
                qualifiedTable);
        }

        // Buffered pop: returns IsExpired flag so expired messages can be discarded without processing.
        _popSql = $@"
DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
SET NOCOUNT ON;

WITH message AS (
    SELECT TOP(@count) Id, Headers, Body, Expires
    FROM {qualifiedTable} WITH (UPDLOCK, READPAST, ROWLOCK)
    ORDER BY RowVersion)
DELETE FROM message
OUTPUT
    deleted.Id,
    deleted.Headers,
    deleted.Body,
    CASE WHEN deleted.Expires IS NULL THEN 0
         WHEN deleted.Expires > GETUTCDATE() THEN 0
         ELSE 1 END;

IF (@NOCOUNT = 'ON') SET NOCOUNT ON;
IF (@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

        // Durable pop: returns raw Expires so it can be stored as KeepUntil in the Wolverine inbox.
        _durablePopSql = $@"
DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
SET NOCOUNT ON;

WITH message AS (
    SELECT TOP(@count) Id, Headers, Body, Expires
    FROM {qualifiedTable} WITH (UPDLOCK, READPAST, ROWLOCK)
    ORDER BY RowVersion)
DELETE FROM message
OUTPUT
    deleted.Id,
    deleted.Headers,
    deleted.Body,
    deleted.Expires;

IF (@NOCOUNT = 'ON') SET NOCOUNT ON;
IF (@NOCOUNT = 'OFF') SET NOCOUNT OFF;";
    }

    public Uri Address { get; }
    public IHandlerPipeline? Pipeline => _receiver.Pipeline;

    public async Task StartAsync()
    {
        _task = Task.Run(listenAsync, _cancellation.Token);
    }

    public ValueTask CompleteAsync(Envelope envelope) => ValueTask.CompletedTask;

    public async ValueTask DeferAsync(Envelope envelope)
    {
        await Sender.SendAsync(envelope);
    }

    public ValueTask DisposeAsync() => StopAsync();

    public async ValueTask StopAsync()
    {
        await _cancellation.CancelAsync();
        _task?.SafeDispose();
    }

    private async Task listenAsync()
    {
        var failedCount = 0;

        while (!_cancellation.Token.IsCancellationRequested)
        {
            try
            {
                var messages = _useDurableInbox
                    ? await TryPopDurablyAsync(_queue.MaximumMessagesToReceive, _cancellation.Token)
                    : await TryPopAsync(_queue.MaximumMessagesToReceive, _cancellation.Token);

                failedCount = 0;

                if (messages.Count > 0)
                    await _receiver.ReceivedAsync(this, messages.ToArray());
                else
                    await Task.Delay(_pollingInterval, _cancellation.Token);
            }
            catch (Exception e)
            {
                if (e is TaskCanceledException && _cancellation.IsCancellationRequested) break;

                failedCount++;
                var pauseTime = failedCount > 5 ? 1.Seconds() : (failedCount * 100).Milliseconds();
                _logger.LogError(e, "Error receiving from NServiceBus SQL Server queue {Table}", _qualifiedTable);
                await Task.Delay(pauseTime);
            }
        }
    }

    // Exposed internal for testing
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "NServiceBus interop is documented as not AOT-safe.")]
    internal async Task<IReadOnlyList<Envelope>> TryPopAsync(int count, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        var results = new List<Envelope>();

        await using var cmd = conn.CreateCommand(_popSql).With("count", count);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            try
            {
                var id = await reader.GetFieldValueAsync<Guid>(0, cancellationToken);
                var headersJson = await reader.GetFieldValueAsync<string>(1, cancellationToken);
                var body = await reader.IsDBNullAsync(2, cancellationToken)
                    ? null
                    : await reader.GetFieldValueAsync<byte[]>(2, cancellationToken);
                var isExpired = await reader.GetFieldValueAsync<int>(3, cancellationToken);

                if (isExpired == 1)
                {
                    _logger.LogDebug("Discarding expired NServiceBus message {Id} from {Table}", id, _qualifiedTable);
                    continue;
                }

                results.Add(NServiceBusSqlServerEnvelopeMapper.ReadFromNServiceBus(id, headersJson, body));
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error reading NServiceBus message from {Table}", _qualifiedTable);
            }
        }

        return results;
    }

    // Exposed internal for testing.
    // Atomically moves messages from the NServiceBus table into Wolverine's inbox within a single
    // transaction. Requires both tables to be in the same database catalog — see CanUseDurableInbox().
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "NServiceBus interop is documented as not AOT-safe.")]
    internal async Task<IReadOnlyList<Envelope>> TryPopDurablyAsync(int count, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        try
        {
            // Step 1: Delete from NServiceBus table, capturing id/headers/body/expires
            var rawMessages = new List<(Guid Id, string Headers, byte[]? Body, DateTime? Expires)>();

            await using (var cmd = conn.CreateCommand(_durablePopSql).With("count", count))
            {
                cmd.Transaction = (SqlTransaction)tx;
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

                while (await reader.ReadAsync(cancellationToken))
                {
                    var id = await reader.GetFieldValueAsync<Guid>(0, cancellationToken);
                    var headers = await reader.GetFieldValueAsync<string>(1, cancellationToken);
                    var body = await reader.IsDBNullAsync(2, cancellationToken)
                        ? null
                        : await reader.GetFieldValueAsync<byte[]>(2, cancellationToken);
                    var expires = await reader.IsDBNullAsync(3, cancellationToken)
                        ? (DateTime?)null
                        : await reader.GetFieldValueAsync<DateTime>(3, cancellationToken);

                    rawMessages.Add((id, headers, body, expires));
                }
            }

            if (rawMessages.Count == 0)
            {
                await tx.CommitAsync(cancellationToken);
                return Array.Empty<Envelope>();
            }

            // Step 2: Map NServiceBus rows to Wolverine envelopes in C#
            var envelopes = new List<Envelope>(rawMessages.Count);
            foreach (var (id, headers, body, expires) in rawMessages)
            {
                try
                {
                    var envelope = NServiceBusSqlServerEnvelopeMapper.ReadFromNServiceBus(id, headers, body);
                    if (expires.HasValue) envelope.KeepUntil = expires;
                    envelopes.Add(envelope);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error mapping NServiceBus message {Id} from {Table} — discarding", id,
                        _qualifiedTable);
                }
            }

            // Step 3: Insert reconstructed envelopes into Wolverine inbox, still in the same transaction
            var inboxInsertSql = $@"
INSERT INTO {_inboxTable} (id, status, owner_id, body, message_type, received_at, keep_until)
VALUES (@id, 'Incoming', @node, @body, @type, @address, @keepUntil)";

            foreach (var envelope in envelopes)
            {
                await using var insertCmd = conn.CreateCommand(inboxInsertSql)
                    .With("id", envelope.Id)
                    .With("node", _durabilitySettings.AssignedNodeNumber)
                    .With("body", EnvelopeSerializer.Serialize(envelope))
                    .With("type", envelope.MessageType ?? string.Empty)
                    .With("address", Address.ToString())
                    .With("keepUntil", (object?)envelope.KeepUntil ?? DBNull.Value);

                insertCmd.Transaction = (SqlTransaction)tx;
                await insertCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            await tx.CommitAsync(cancellationToken);
            return envelopes;
        }
        catch
        {
            await tx.RollbackAsync(cancellationToken);
            throw;
        }
    }

    private bool canUseDurableInbox()
    {
        // Multi-tenancy: inbox tables are in per-tenant databases; we cannot span catalogs in one transaction.
        if (_queue.Parent.Databases != null) return false;

        var wolveConnStr = _queue.Parent.Settings.ConnectionString;
        if (wolveConnStr == null) return false;

        // No connection string override means the NServiceBus table is in the same database.
        if (_queue.NServiceBusConnectionString == null) return true;

        return sameCatalog(_connectionString, wolveConnStr);
    }

    private static bool sameCatalog(string connA, string connB)
    {
        try
        {
            var a = new SqlConnectionStringBuilder(connA);
            var b = new SqlConnectionStringBuilder(connB);
            return string.Equals(a.DataSource, b.DataSource, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(a.InitialCatalog, b.InitialCatalog, StringComparison.OrdinalIgnoreCase);
        }
        catch
        {
            return false;
        }
    }
}
