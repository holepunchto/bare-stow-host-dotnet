using System.Buffers.Binary;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Holepunch.BareStowHost;

/// <summary>
/// The framing of the control and user data channels, wire-compatible with the JavaScript
/// <c>bare-stow-protocol</c>.
/// </summary>
public static class Frame
{
    /// <summary>The tag of a control frame.</summary>
    public const byte Control = 0x00;

    /// <summary>The tag of a user data frame.</summary>
    public const byte User = 0x01;

    /// <summary>Encodes a frame of <paramref name="tag"/> carrying <paramref name="payload"/>.</summary>
    public static byte[] Encode(byte tag, ReadOnlySpan<byte> payload)
    {
        var frame = new byte[5 + payload.Length];

        BinaryPrimitives.WriteUInt32LittleEndian(frame, (uint)payload.Length + 1);

        frame[4] = tag;
        payload.CopyTo(frame.AsSpan(5));

        return frame;
    }

    /// <summary>Encodes a control frame of <paramref name="type"/> with integer <paramref name="fields"/>, in order.</summary>
    public static byte[] EncodeControl(string type, params ReadOnlySpan<(string Key, long Value)> fields)
    {
        using var buffer = new MemoryStream();

        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("type", type);

            foreach (var (key, value) in fields)
            {
                writer.WriteNumber(key, value);
            }

            writer.WriteEndObject();
        }

        return Encode(Control, buffer.ToArray());
    }

    /// <summary>Decodes the JSON object of a control frame, or returns <c>null</c> if it isn't one.</summary>
    public static JsonObject? DecodeControl(ReadOnlySpan<byte> payload)
    {
        try
        {
            return JsonNode.Parse(payload) as JsonObject;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
