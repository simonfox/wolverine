using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Wolverine.Util;

namespace Wolverine.SqlServer.Transport;

[UnconditionalSuppressMessage("Trimming", "IL2026",
    Justification = "NServiceBus SQL Server interop is documented as not AOT/trim-safe.")]
[UnconditionalSuppressMessage("Trimming", "IL3050",
    Justification = "NServiceBus SQL Server interop is documented as not AOT/trim-safe.")]
internal static class NServiceBusSqlServerEnvelopeMapper
{
    private static readonly JsonSerializerOptions _options = new() { WriteIndented = false };

    [RequiresUnreferencedCode(
        "NServiceBus EnclosedMessageTypes resolution uses Type.GetType() which is not trim-safe. " +
        "NServiceBus SQL Server interop is not supported in AOT/trimmed applications.")]
    internal static Envelope ReadFromNServiceBus(Guid id, string headersJson, byte[]? body)
    {
        var envelope = new Envelope
        {
            Id = id,
            Data = body ?? Array.Empty<byte>()
        };

        Dictionary<string, string>? headers;
        try
        {
            headers = JsonSerializer.Deserialize<Dictionary<string, string>>(headersJson, _options);
        }
        catch
        {
            headers = null;
        }

        if (headers == null) return envelope;

        foreach (var (key, value) in headers)
        {
            switch (key)
            {
                case "NServiceBus.MessageId":
                    if (Guid.TryParse(value, out var msgId)) envelope.Id = msgId;
                    break;

                case "NServiceBus.EnclosedMessageTypes":
                    // NSB format: "Full.TypeName, AssemblyName" — multiple types separated by ";"
                    var first = value.Split(';')[0].Trim();
                    var type = Type.GetType(first);
                    envelope.MessageType = type != null ? type.ToMessageTypeName() : first;
                    break;

                case "NServiceBus.CorrelationId":
                    envelope.CorrelationId = value;
                    break;

                case "NServiceBus.ConversationId":
                    if (Guid.TryParse(value, out var convId)) envelope.ConversationId = convId;
                    break;

                case "NServiceBus.TimeSent":
                    if (DateTimeOffset.TryParse(value, out var sentAt))
                        envelope.SentAt = sentAt.UtcDateTime;
                    break;

                case "NServiceBus.ReplyToAddress":
                    if (!string.IsNullOrEmpty(value))
                        envelope.ReplyUri = new Uri($"sqlserver://queue/{value}");
                    break;

                case "NServiceBus.ContentType":
                    envelope.ContentType = value;
                    break;

                default:
                    envelope.Headers[key] = value;
                    break;
            }
        }

        return envelope;
    }

    internal static (string headersJson, byte[] body) WriteToNServiceBus(Envelope envelope)
    {
        var headers = new Dictionary<string, string>
        {
            ["NServiceBus.MessageId"] = envelope.Id.ToString(),
            ["NServiceBus.TimeSent"] = envelope.SentAt.ToString("yyyy-MM-dd HH:mm:ss:ffffff Z")
        };

        if (!string.IsNullOrEmpty(envelope.MessageType))
            headers["NServiceBus.EnclosedMessageTypes"] = envelope.MessageType;

        if (!string.IsNullOrEmpty(envelope.CorrelationId))
            headers["NServiceBus.CorrelationId"] = envelope.CorrelationId;

        if (envelope.ConversationId != Guid.Empty)
            headers["NServiceBus.ConversationId"] = envelope.ConversationId.ToString();

        if (envelope.ReplyUri != null)
        {
            // sqlserver://queue/{name} → extract the endpoint name as the NSB reply address
            var segments = envelope.ReplyUri.Segments;
            headers["NServiceBus.ReplyToAddress"] = segments.Length > 1
                ? segments.Last().TrimEnd('/')
                : envelope.ReplyUri.Host;
        }

        if (!string.IsNullOrEmpty(envelope.ContentType))
            headers["NServiceBus.ContentType"] = envelope.ContentType;

        foreach (var (key, value) in envelope.Headers)
            if (value != null && !headers.ContainsKey(key))
                headers[key] = value;

        return (JsonSerializer.Serialize(headers, _options), envelope.Data ?? Array.Empty<byte>());
    }
}
