using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace Holepunch.BareStowHost.Tests;

sealed class End : Stream
{
    private readonly Channel<byte[]> _incoming = Channel.CreateUnbounded<byte[]>();
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private byte[]? _chunk;
    private int _offset;

    public End Peer { get; set; } = null!;

    public bool Closed { get; private set; }

    public Exception? WriteError { get; set; }

    private Exception? _readError;

    // Completes once a read has seen the end of the incoming data.
    public Task Drained => _drained.Task;

    public static (End, End) Pair()
    {
        var a = new End();
        var b = new End();

        a.Peer = b;
        b.Peer = a;

        return (a, b);
    }

    public override void Close()
    {
        foreach (var end in new[] { this, Peer })
        {
            end.Closed = true;
            end._incoming.Writer.TryComplete();
        }
    }

    // Fails reads and writes on this end, as a transport that breaks does.
    public void Break(Exception err)
    {
        _readError = err;
        WriteError = err;
        _incoming.Writer.TryComplete();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        while (_chunk is null && !_incoming.Reader.TryRead(out _chunk))
        {
            if (!await _incoming.Reader.WaitToReadAsync(cancellationToken))
            {
                _drained.TrySetResult();

                if (_readError is not null)
                {
                    throw _readError;
                }

                return 0;
            }
        }

        var n = Math.Min(buffer.Length, _chunk.Length - _offset);

        _chunk.AsMemory(_offset, n).CopyTo(buffer);

        _offset += n;

        if (_offset == _chunk.Length)
        {
            _chunk = null;
            _offset = 0;
        }

        return n;
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (Closed)
        {
            throw new IOException("End is closed");
        }

        if (WriteError is not null)
        {
            throw WriteError;
        }

        Peer._incoming.Writer.TryWrite(buffer.ToArray());
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        Write(buffer.Span);

        return ValueTask.CompletedTask;
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override bool CanRead => true;

    public override bool CanWrite => true;

    public override bool CanSeek => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();
}

sealed class Worker
{
    private readonly End _end;
    private readonly Channel<(byte Tag, byte[] Payload)> _frames = Channel.CreateUnbounded<(byte, byte[])>();

    public Worker(End end)
    {
        _end = end;

        _ = Task.Run(async () =>
        {
            var decoder = new FrameDecoder();
            var buffer = new byte[1024];
            int n;

            while ((n = await end.ReadAsync(buffer)) > 0)
            {
                foreach (var frame in decoder.Feed(buffer.AsSpan(0, n)))
                {
                    _frames.Writer.TryWrite(frame);
                }
            }
        });
    }

    public void Send(string type, params ReadOnlySpan<(string, long)> fields) => _end.Write(Frame.EncodeControl(type, fields));

    public void SendJson(string json) => _end.Write(Frame.Encode(Frame.Control, Encoding.UTF8.GetBytes(json)));

    public void Write(string data) => _end.Write(Frame.Encode(Frame.User, Encoding.UTF8.GetBytes(data)));

    public async Task<JsonObject> ReceiveAsync(string type)
    {
        while (true)
        {
            var (tag, payload) = await _frames.Reader.ReadAsync();

            if (tag == Frame.Control && Frame.DecodeControl(payload) is { } message && (string?)message["type"] == type)
            {
                return message;
            }
        }
    }

    public async Task<string> DataAsync()
    {
        while (true)
        {
            var (tag, payload) = await _frames.Reader.ReadAsync();

            if (tag == Frame.User)
            {
                return Encoding.UTF8.GetString(payload);
            }
        }
    }

    public void Close() => _end.Close();

    // Closes the transport once asked to terminate.
    public async Task CloseOnTerminateAsync()
    {
        await ReceiveAsync("terminate");

        Close();
    }
}
