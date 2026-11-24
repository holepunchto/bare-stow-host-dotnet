namespace Holepunch.BareStowHost;

/// <summary>A fault reported by or observed in the worker.</summary>
public sealed class WorkerException(
    string message,
    string code,
    string? stack = null,
    int? exitCode = null,
    int? signal = null,
    Exception? innerException = null)
    : Exception(message, innerException)
{
    /// <summary>
    /// The kind of fault: <c>WORKER_ERROR</c> for a fault the worker reported,
    /// <c>WORKER_EXITED</c> for a worker that exited, <c>WORKER_KILLED</c> for a worker killed by a
    /// signal, <c>WORKER_CLOSED</c> for a transport that closed before ready,
    /// <c>WORKER_TIMEOUT</c> for a worker that timed out before ready, and
    /// <c>TRANSPORT_ERROR</c> for a transport that failed, which is kept as
    /// <see cref="Exception.InnerException"/>.
    /// </summary>
    public string Code { get; } = code;

    /// <summary>The stack of the error reported by the worker, if any.</summary>
    public string? Stack { get; } = stack;

    /// <summary>The exit code of a worker that exited, if known.</summary>
    public int? ExitCode { get; } = exitCode;

    /// <summary>The signal that killed a worker that exited without saying why.</summary>
    public int? Signal { get; } = signal;
}
