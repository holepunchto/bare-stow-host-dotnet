using System.Net.Sockets;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace Holepunch.BareStowHost;

/// <summary>
/// The host side of a stowed bundle, layering readiness, termination, and exit reporting over a
/// transport.
/// </summary>
/// <remarks>
/// Reading yields the user data written by the worker until the transport ends, and throws a
/// <see cref="WorkerException"/> once the worker has reported a fault or exited without saying
/// why. Writing sends user data to the worker, and disposing terminates the worker. Each
/// <see cref="WorkerException"/> it raises carries a <see cref="WorkerException.Code"/> that tells
/// the kind of fault apart.
/// <para>
/// Whoever creates it must call <see cref="Exited"/> once the worker exits. A transport that ends
/// without the worker saying why is held back until then, so that the handle fails with the
/// reason. A transport that fails instead waits for the exit for the grace period, failing with
/// the exit and the failure as its <see cref="Exception.InnerException"/>, or calling <c>kill</c>
/// and failing with <c>TRANSPORT_ERROR</c> if the worker hasn't exited by then.
/// </para>
/// </remarks>
public sealed class Ipc : Stream
{
    /// <summary>The default time a terminating worker gets to stop and exit.</summary>
    public static readonly TimeSpan DefaultGrace = TimeSpan.FromSeconds(5);

    private const int BrokenPipe = unchecked((int)0x8007006D);
    private const int PipeClosing = unchecked((int)0x800700E8);

    private readonly Stream _transport;
    private readonly Action _kill;
    private readonly TimeSpan _grace;
    private readonly Timer? _timer;
    private readonly SemaphoreSlim _writing = new(1, 1);
    private readonly Channel<byte[]> _data = Channel.CreateUnbounded<byte[]>();
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Lock _lock = new();

    private byte[]? _chunk;
    private int _offset;
    private int? _exitCode;
    private bool _exitReported;
    private bool _askedToTerminate;
    private bool _ended;
    private WorkerException? _failure;
    private WorkerException? _transportFailure;
    private Timer? _failing;
    private Task? _terminating;

    /// <summary>
    /// Wraps the host side of a stowed bundle's <paramref name="transport"/> and sends the grace
    /// period to the worker.
    /// </summary>
    /// <param name="transport">The transport to the bundle, such as a <c>Worklet</c> or <c>Sidecar</c>.</param>
    /// <param name="kill">Stops a worker that ignores <c>terminate</c> for the grace period. Defaults to disposing <paramref name="transport"/>.</param>
    /// <param name="timeout">How long to wait for the worker to signal ready before failing <see cref="Ready"/> and terminating it, or <c>null</c> to wait indefinitely.</param>
    /// <param name="grace">How long a terminating worker gets to stop and exit before <paramref name="kill"/> is called. Defaults to <see cref="DefaultGrace"/>.</param>
    public Ipc(Stream transport, Action? kill = null, TimeSpan? timeout = null, TimeSpan? grace = null)
    {
        _transport = transport;
        _kill = kill ?? transport.Dispose;
        _grace = grace ?? DefaultGrace;

        if (timeout is { } delay)
        {
            _timer = new Timer(_ => TimeOut(), null, delay, Timeout.InfiniteTimeSpan);
        }

        _ = SendAsync(Frame.EncodeControl("configure", ("grace", (long)_grace.TotalMilliseconds)));
        _ = Task.Run(ReadFramesAsync);
    }

    /// <summary>
    /// Completes once the worker has signaled ready.
    /// </summary>
    /// <remarks>
    /// It fails with a <see cref="WorkerException"/> if the worker errors, exits, closes, or times
    /// out before then, in which case the worker is terminated.
    /// </remarks>
    public Task Ready => _ready.Task;

    /// <summary>The exit code the worker reported, if any.</summary>
    public int? ExitCode
    {
        get
        {
            lock (_lock)
            {
                return _exitCode;
            }
        }
    }

    /// <summary>Waits for the transport to end and returns the exit code the worker reported, if any.</summary>
    public async Task<int?> WaitForExitAsync()
    {
        await _closed.Task;

        return ExitCode;
    }

    /// <summary>
    /// Asks the worker to terminate and waits for the transport to end, calling <c>kill</c> if the
    /// worker hasn't exited within the grace period.
    /// </summary>
    /// <returns>The exit code the worker reported, if any.</returns>
    public async Task<int?> TerminateAsync()
    {
        await CloseAsync();

        return ExitCode;
    }

    /// <summary>Like <see cref="TerminateAsync"/>, without the exit code.</summary>
    public Task CloseAsync()
    {
        lock (_lock)
        {
            if (_terminating is null)
            {
                _askedToTerminate = !_ended;
                _terminating = _askedToTerminate ? Task.Run(TerminateCoreAsync) : _closed.Task;
            }

            return _terminating;
        }
    }

    /// <summary>
    /// Reports that the worker has exited, as observed by the transport, such as the process of a
    /// sidecar. Whoever creates the handle must call it once the worker exits.
    /// </summary>
    /// <remarks>
    /// If the worker exited without saying why and was not asked to, the handle fails with a
    /// <c>WORKER_EXITED</c> or <c>WORKER_KILLED</c> <see cref="WorkerException"/> carrying
    /// <see cref="WorkerException.ExitCode"/> and <see cref="WorkerException.Signal"/>. If the
    /// transport failed while waiting for the exit, the failure is its
    /// <see cref="Exception.InnerException"/>.
    /// </remarks>
    public void Exited(int? code, int? signal = null)
    {
        WorkerException? cause;

        lock (_lock)
        {
            if (_exitReported || _askedToTerminate || _failure is not null)
            {
                return;
            }

            cause = _transportFailure;

            _failing?.Dispose();
        }

        Fail(signal is { } number
            ? new WorkerException($"Worker was killed by signal {number}", "WORKER_KILLED", exitCode: code, signal: number, innerException: cause)
            : new WorkerException($"Worker exited with code {code ?? -1}", "WORKER_EXITED", exitCode: code, innerException: cause));
    }

    /// <inheritdoc/>
    public override bool CanRead => true;

    /// <inheritdoc/>
    public override bool CanWrite => true;

    /// <inheritdoc/>
    public override bool CanSeek => false;

    /// <inheritdoc/>
    public override long Length => throw new NotSupportedException();

    /// <inheritdoc/>
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <inheritdoc/>
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    /// <inheritdoc/>
    public override void SetLength(long value) => throw new NotSupportedException();

    /// <inheritdoc/>
    public override void Flush()
    {
    }

    /// <inheritdoc/>
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0)
        {
            return 0;
        }

        while (_chunk is null && !_data.Reader.TryRead(out _chunk))
        {
            if (!await _data.Reader.WaitToReadAsync(cancellationToken))
            {
                lock (_lock)
                {
                    if (_failure is not null)
                    {
                        throw _failure;
                    }
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

    /// <inheritdoc/>
    public override int Read(Span<byte> buffer)
    {
        var array = new byte[buffer.Length];

        var n = ReadAsync(array).AsTask().GetAwaiter().GetResult();

        array.AsSpan(0, n).CopyTo(buffer);

        return n;
    }

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    /// <inheritdoc/>
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <inheritdoc/>
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        await WriteFrameAsync(Frame.Encode(Frame.User, buffer.Span), cancellationToken);

    /// <inheritdoc/>
    public override void Write(ReadOnlySpan<byte> buffer) => WriteAsync(buffer.ToArray()).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    /// <inheritdoc/>
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        await CloseAsync();

        Dispose();
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            CloseAsync().GetAwaiter().GetResult();

            _timer?.Dispose();
            _failing?.Dispose();
        }

        base.Dispose(disposing);
    }

    // Frames sent once the transport has closed are dropped, as the exit of the worker already
    // accounts for them, and a failure while it's open is reported through the handle.
    private async Task SendAsync(byte[] frame)
    {
        try
        {
            await WriteFrameAsync(frame);
        }
        catch (WorkerException)
        {
        }
    }

    private async Task WriteFrameAsync(byte[] frame, CancellationToken cancellationToken = default)
    {
        await _writing.WaitAsync(cancellationToken);

        try
        {
            await _transport.WriteAsync(frame, cancellationToken);
            await _transport.FlushAsync(cancellationToken);
        }
        catch (Exception err) when (err is not OperationCanceledException)
        {
            throw OnTransportError(err);
        }
        finally
        {
            _writing.Release();
        }
    }

    // Returns the error for a writer to throw. Killing the worker may dispose the transport while
    // it's being used, and the failures of a worker that may have crashed wait for its exit as an
    // end does, calling kill if it hasn't exited within the grace period.
    private WorkerException OnTransportError(Exception err)
    {
        var failure = new WorkerException(err.Message, "TRANSPORT_ERROR", innerException: err);

        lock (_lock)
        {
            if (PeerGone(err) || _ended || _exitReported || _askedToTerminate)
            {
                return new WorkerException("Worker closed", "WORKER_CLOSED", innerException: err);
            }

            if (MayHaveCrashed() && _transportFailure is null)
            {
                _transportFailure = failure;
                _timer?.Dispose();
                _failing = new Timer(_ => KillAfterFailure(), null, _grace, Timeout.InfiniteTimeSpan);
            }
        }

        return failure;
    }

    private void KillAfterFailure()
    {
        lock (_lock)
        {
            if (_failure is not null || _askedToTerminate)
            {
                return;
            }
        }

        _kill();

        Fail(_transportFailure!);
    }

    // A peer that goes away resets or breaks the connection rather than ending it, which is no
    // different from it closing. On Unix, a broken pipe is reported as a shut down socket.
    private static bool PeerGone(Exception err) => err switch
    {
        SocketException { SocketErrorCode: SocketError.ConnectionReset or SocketError.ConnectionAborted or SocketError.Shutdown } => true,
        IOException { InnerException: SocketException inner } => PeerGone(inner),
        IOException { HResult: BrokenPipe or PipeClosing } => OperatingSystem.IsWindows(),
        _ => false,
    };

    private bool MayHaveCrashed() => !_exitReported && !_askedToTerminate && _failure is null;

    private async Task ReadFramesAsync()
    {
        var decoder = new FrameDecoder();
        var buffer = new byte[65536];

        try
        {
            int n;

            while ((n = await _transport.ReadAsync(buffer)) > 0)
            {
                foreach (var (tag, payload) in decoder.Feed(buffer.AsSpan(0, n)))
                {
                    OnFrame(tag, payload);
                }
            }
        }
        catch (Exception err)
        {
            OnTransportError(err);
        }

        OnClose();
    }

    private void OnFrame(byte tag, byte[] payload)
    {
        if (tag == Frame.User)
        {
            lock (_lock)
            {
                if (_failure is null && payload.Length > 0)
                {
                    _data.Writer.TryWrite(payload);
                }
            }

            return;
        }

        if (tag != Frame.Control || Frame.DecodeControl(payload) is not { } message)
        {
            return;
        }

        switch (Text(message, "type"))
        {
            case "ready":
                SettleReady(null);
                break;

            case "exit":
                var code = message["code"] is JsonValue value && value.TryGetValue<int>(out var number) ? number : (int?)null;

                bool faulted;

                lock (_lock)
                {
                    _exitReported = true;
                    _exitCode = code;
                    faulted = _failure is not null;
                }

                // An error frame precedes the exit frame of a worker that faulted.
                if (!faulted)
                {
                    SettleReady(new WorkerException($"Worker exited with code {code ?? -1} before ready", "WORKER_EXITED", exitCode: code));
                }

                break;

            case "error":
                Fail(new WorkerException(Text(message, "message") ?? "", "WORKER_ERROR", stack: Text(message, "stack")));
                break;
        }
    }

    // A worker whose transport ends without it saying why has crashed, so the end is held back
    // until its exit is reported for readers to fail with. It can no longer time out, as it
    // already ended.
    private void OnClose()
    {
        lock (_lock)
        {
            _ended = true;

            if (MayHaveCrashed())
            {
                _timer?.Dispose();

                return;
            }
        }

        _data.Writer.TryComplete();
        _closed.TrySetResult();

        SettleReady(new WorkerException("Worker closed before ready", "WORKER_CLOSED"));
    }

    private void Fail(WorkerException error)
    {
        bool ended;

        lock (_lock)
        {
            if (_failure is not null)
            {
                return;
            }

            _failure = error;
            ended = _ended;
        }

        _data.Writer.TryComplete();

        if (!SettleReady(error))
        {
            _ = CloseAsync();
        }

        if (ended)
        {
            _closed.TrySetResult();
        }
    }

    // A worker whose transport has ended or failed is settled by how it ended.
    private void TimeOut()
    {
        lock (_lock)
        {
            if (!_ended && _transportFailure is null)
            {
                SettleReady(new WorkerException("Worker timed out before ready", "WORKER_TIMEOUT"));
            }
        }
    }

    // Settling with an error terminates the worker.
    private bool SettleReady(WorkerException? error)
    {
        var settled = error is null ? _ready.TrySetResult() : _ready.TrySetException(error);

        if (!settled)
        {
            return false;
        }

        _timer?.Dispose();

        if (error is not null)
        {
            _ = CloseAsync();
        }

        return true;
    }

    private async Task TerminateCoreAsync()
    {
        using var cancellation = new CancellationTokenSource();

        var grace = Task.Delay(_grace, cancellation.Token);

        await SendAsync(Frame.EncodeControl("terminate"));

        if (await Task.WhenAny(_closed.Task, grace) == grace)
        {
            _kill();
        }

        await _closed.Task;

        await cancellation.CancelAsync();
    }

    private static string? Text(JsonObject message, string key) =>
        message[key] is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;
}
