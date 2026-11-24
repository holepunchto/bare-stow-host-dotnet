using System.Buffers.Binary;

namespace Holepunch.BareStowHost;

/// <summary>Decodes frames from a stream of bytes.</summary>
public sealed class FrameDecoder
{
    private byte[] _buffer = [];

    /// <summary>Feeds <paramref name="data"/> to the decoder and returns the frames it completes.</summary>
    public List<(byte Tag, byte[] Payload)> Feed(ReadOnlySpan<byte> data)
    {
        var buffer = new byte[_buffer.Length + data.Length];

        _buffer.CopyTo(buffer, 0);
        data.CopyTo(buffer.AsSpan(_buffer.Length));

        var frames = new List<(byte, byte[])>();
        var offset = 0;

        while (buffer.Length - offset >= 4)
        {
            var length = (int)BinaryPrimitives.ReadUInt32LittleEndian(buffer.AsSpan(offset));

            var start = offset + 4;
            var end = start + length;

            if (buffer.Length < end)
            {
                break;
            }

            if (length > 0)
            {
                frames.Add((buffer[start], buffer[(start + 1)..end]));
            }

            offset = end;
        }

        _buffer = buffer[offset..];

        return frames;
    }
}
