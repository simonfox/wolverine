using Microsoft.Data.SqlClient;
using Weasel.SqlServer;
using Wolverine.Transports.Sending;

namespace Wolverine.SqlServer.Transport;

internal class NServiceBusSqlServerSender : ISqlServerQueueSender
{
    private readonly string _connectionString;
    private readonly string _insertSql;

    public NServiceBusSqlServerSender(SqlServerQueue queue, string connectionString, string qualifiedTable)
    {
        Destination = queue.Uri;
        _connectionString = connectionString;

        _insertSql = $@"
DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
SET NOCOUNT ON;

INSERT INTO {qualifiedTable} (Id, Recoverable, Expires, Headers, Body)
VALUES (
    @Id,
    1,
    CASE WHEN @TimeToBeReceivedMs IS NOT NULL
        THEN DATEADD(ms, @TimeToBeReceivedMs, GETUTCDATE())
    END,
    @Headers,
    @Body);

IF (@NOCOUNT = 'ON') SET NOCOUNT ON;
IF (@NOCOUNT = 'OFF') SET NOCOUNT OFF;";
    }

    public bool SupportsNativeScheduledSend => false;
    public Uri Destination { get; }

    public async Task<bool> PingAsync()
    {
        try
        {
            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            return true;
        }
        catch
        {
            return false;
        }
    }

    public async ValueTask SendAsync(Envelope envelope)
    {
        var (headersJson, body) = NServiceBusSqlServerEnvelopeMapper.WriteToNServiceBus(envelope);

        long? ttlMs = envelope.DeliverBy.HasValue
            ? (long)(envelope.DeliverBy.Value - DateTimeOffset.UtcNow).TotalMilliseconds
            : null;

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        await conn.CreateCommand(_insertSql)
            .With("Id", envelope.Id)
            .With("TimeToBeReceivedMs", (object?)ttlMs ?? DBNull.Value)
            .With("Headers", headersJson)
            .With("Body", body.Length > 0 ? (object)body : DBNull.Value)
            .ExecuteNonQueryAsync();
    }

    // NServiceBus SQL transport has no scheduled-message table.
    // Re-enqueue immediately; Wolverine's durability layer manages retry timing.
    public Task ScheduleRetryAsync(Envelope envelope, CancellationToken cancellationToken)
        => SendAsync(envelope).AsTask();
}
