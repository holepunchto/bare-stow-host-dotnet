using System.Net.Sockets;
using System.Text;
using Xunit;

namespace Holepunch.BareStowHost.Tests;

public class IpcTests
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(10);

    private static Ipc Wrap(End end, TimeSpan? timeout = null, TimeSpan? grace = null) =>
        new(end, end.Close, timeout, grace);

    // There is no signal for the end being held back, so give a release some time to show.
    private static async Task AssertPendingAsync(Task task)
    {
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.False(task.IsCompleted);
    }

    private static async Task<string> ReadToEndAsync(Ipc ipc)
    {
        using var reader = new StreamReader(ipc, Encoding.UTF8, leaveOpen: true);

        return await reader.ReadToEndAsync();
    }

    [Fact]
    public async Task ResolvesReadyWhenTheWorkerSignals()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);

        worker.Send("ready");

        await ipc.Ready;

        worker.Send("exit", ("code", 0));
        worker.Close();

        await ipc.WaitForExitAsync();
    }

    [Fact]
    public async Task SendsTheGraceToTheWorker()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left, grace: TimeSpan.FromMilliseconds(2500));
        var worker = new Worker(right);

        var message = await worker.ReceiveAsync("configure");

        Assert.Equal(2500, (int?)message["grace"]);

        worker.Send("exit", ("code", 0));
        worker.Close();

        await ipc.WaitForExitAsync();
    }

    [Fact]
    public async Task RejectsReadyWhenTheWorkerErrorsBeforeReady()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);
        var closing = worker.CloseOnTerminateAsync();

        worker.SendJson("{\"type\":\"error\",\"message\":\"boom\",\"stack\":\"at core.js\"}");

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("boom", err.Message);
        Assert.Equal("WORKER_ERROR", err.Code);
        Assert.Equal("at core.js", err.Stack);

        await closing;
    }

    [Fact]
    public async Task RejectsReadyWhenTheWorkerExitsBeforeReady()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);
        var closing = worker.CloseOnTerminateAsync();

        worker.Send("exit", ("code", 1));

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_EXITED", err.Code);
        Assert.Equal(1, err.ExitCode);

        await closing;
    }

    [Fact]
    public async Task RejectsReadyWhenClosedBeforeReady()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var closing = new Worker(right).CloseOnTerminateAsync();
        var terminated = ipc.CloseAsync();

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_CLOSED", err.Code);

        await closing;
        await terminated;
    }

    [Fact]
    public async Task IgnoresExitAfterReady()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);

        worker.Send("ready");

        await ipc.Ready;

        worker.Send("exit", ("code", 1));
        worker.Close();

        await ipc.WaitForExitAsync();
        await ipc.Ready;
    }

    [Fact]
    public async Task ReportsTheExitCode()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);

        worker.Send("ready");
        worker.Send("exit", ("code", 7));
        worker.Close();

        Assert.Equal(7, await ipc.WaitForExitAsync());
        Assert.Equal(7, ipc.ExitCode);
    }

    [Fact]
    public async Task CarriesUserData()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);

        worker.Write("payload");

        var received = new byte[7];

        await ipc.ReadExactlyAsync(received, TestContext.Current.CancellationToken);

        Assert.Equal("payload", Encoding.UTF8.GetString(received));

        await ipc.WriteAsync("reply"u8.ToArray(), TestContext.Current.CancellationToken);

        Assert.Equal("reply", await worker.DataAsync());

        worker.Send("exit", ("code", 0));
        worker.Close();

        Assert.Equal("", await ReadToEndAsync(ipc));
    }

    [Fact]
    public async Task ReadsUserDataInParts()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);

        worker.Write("hello");
        worker.Write("");
        worker.Write(" world");
        worker.Send("exit", ("code", 0));
        worker.Close();

        var part = new byte[3];

        Assert.Equal(3, await ipc.ReadAsync(part, TestContext.Current.CancellationToken));
        Assert.Equal("hel", Encoding.UTF8.GetString(part));
        Assert.Equal("lo world", await ReadToEndAsync(ipc));
    }

    [Fact]
    public async Task TerminateReturnsTheExitCode()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);

        worker.Send("ready");

        await ipc.Ready;

        var exiting = Task.Run(async () =>
        {
            await worker.ReceiveAsync("terminate");

            worker.Send("exit", ("code", 0));
            worker.Close();
        }, TestContext.Current.CancellationToken);

        Assert.Equal(0, await ipc.TerminateAsync());

        await exiting;
    }

    [Fact]
    public async Task DisposeSendsTerminateToTheWorker()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var closing = new Worker(right).CloseOnTerminateAsync();

        await ipc.DisposeAsync();
        await closing;
    }

    [Fact]
    public async Task RejectsReadyAndTerminatesTheWorkerOnTimeout()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left, timeout: Short);
        var closing = new Worker(right).CloseOnTerminateAsync();

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_TIMEOUT", err.Code);

        await closing;
    }

    [Fact]
    public async Task TerminateCallsKillAfterTheGracePeriod()
    {
        var (left, right) = End.Pair();
        var killed = false;

        var ipc = new Ipc(left, () =>
        {
            killed = true;
            left.Close();
        }, grace: Short);

        new Worker(right).Send("ready");

        await ipc.Ready;

        Assert.Null(await ipc.TerminateAsync());
        Assert.True(killed);
    }

    [Fact]
    public async Task TerminateDisposesTheTransportByDefault()
    {
        var (left, right) = End.Pair();
        var ipc = new Ipc(left, grace: Short);

        new Worker(right).Send("ready");

        await ipc.Ready;

        Assert.Null(await ipc.TerminateAsync());
        Assert.True(left.Closed);
        Assert.Equal("", await ReadToEndAsync(ipc));

        var late = await Assert.ThrowsAsync<WorkerException>(() => ipc.WriteAsync("late"u8.ToArray(), TestContext.Current.CancellationToken).AsTask());

        Assert.Equal("WORKER_CLOSED", late.Code);
    }

    [Fact]
    public async Task ExitedBeforeReadyRejectsReadyWithTheExitCode()
    {
        var (left, _) = End.Pair();
        var ipc = Wrap(left, grace: Short);

        ipc.Exited(3);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_EXITED", err.Code);
        Assert.Equal(3, err.ExitCode);

        await ipc.CloseAsync();
    }

    [Fact]
    public async Task ExitedAfterReadyFailsReadsWithTheSignal()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left, grace: Short);

        new Worker(right).Send("ready");

        await ipc.Ready;

        ipc.Exited(null, signal: 9);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ReadToEndAsync(ipc));

        Assert.Equal("WORKER_KILLED", err.Code);
        Assert.Equal(9, err.Signal);

        await ipc.CloseAsync();
    }

    [Fact]
    public async Task ExitedIsIgnoredAfterAnExitFrame()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);

        worker.Send("ready");
        worker.Send("exit", ("code", 0));
        worker.Close();

        await ipc.WaitForExitAsync();

        ipc.Exited(0);

        Assert.Equal("", await ReadToEndAsync(ipc));
    }

    [Fact]
    public async Task ExitedIsIgnoredWhileTerminating()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left, grace: Short);

        new Worker(right).Send("ready");

        await ipc.Ready;

        var terminated = ipc.TerminateAsync();

        ipc.Exited(null, signal: 9);

        Assert.Null(await terminated);
        Assert.Equal("", await ReadToEndAsync(ipc));
    }

    [Fact]
    public async Task HoldsTheEndOfAWorkerThatCrashesAfterReady()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);
        var worker = new Worker(right);

        worker.Send("ready");

        await ipc.Ready;

        worker.Close();

        await left.Drained;

        var reading = ReadToEndAsync(ipc);
        var closing = ipc.CloseAsync();

        await AssertPendingAsync(reading);
        await AssertPendingAsync(closing);

        ipc.Exited(null, signal: 11);

        var err = await Assert.ThrowsAsync<WorkerException>(() => reading);

        Assert.Equal("WORKER_KILLED", err.Code);
        Assert.Equal(11, err.Signal);

        await closing;
        await ipc.WaitForExitAsync();
    }

    [Fact]
    public async Task HoldsTheEndOfAWorkerThatCrashesBeforeReady()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left);

        new Worker(right).Close();

        await left.Drained;

        await AssertPendingAsync(ipc.Ready);
        await AssertPendingAsync(ipc.WaitForExitAsync());

        ipc.Exited(1);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_EXITED", err.Code);
        Assert.Equal(1, err.ExitCode);
        Assert.Null(await ipc.WaitForExitAsync());
        Assert.Null(await ipc.TerminateAsync());
    }

    [Fact]
    public async Task FailsACrashedWorkerWithItsExitRatherThanATimeout()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left, timeout: TimeSpan.FromMilliseconds(50));

        new Worker(right).Close();

        await left.Drained;
        await Task.Delay(100, TestContext.Current.CancellationToken);

        ipc.Exited(1);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_EXITED", err.Code);
    }

    [Fact]
    public async Task ExitedWithACodeAndASignalCarriesBoth()
    {
        var (left, _) = End.Pair();
        var ipc = Wrap(left, grace: Short);

        ipc.Exited(1, signal: 9);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_KILLED", err.Code);
        Assert.Equal(1, err.ExitCode);
        Assert.Equal(9, err.Signal);

        await ipc.CloseAsync();
    }

    [Fact]
    public async Task FailsWritesWhenTheTransportFails()
    {
        var (left, right) = End.Pair();
        var ipc = Wrap(left, grace: Short);

        new Worker(right).Send("ready");

        await ipc.Ready;

        var cause = new IOException("broken");

        left.WriteError = cause;

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.WriteAsync("data"u8.ToArray(), TestContext.Current.CancellationToken).AsTask());

        Assert.Equal("TRANSPORT_ERROR", err.Code);
        Assert.Same(cause, err.InnerException);

        var read = await Assert.ThrowsAsync<WorkerException>(() => ReadToEndAsync(ipc));

        Assert.Same(err, read);
        Assert.True(left.Closed);
    }

    [Fact]
    public async Task RejectsReadyWhenAControlFrameFailsToSend()
    {
        var (left, _) = End.Pair();
        var cause = new IOException("broken");

        left.WriteError = cause;

        var ipc = Wrap(left, timeout: TimeSpan.FromSeconds(5), grace: Short);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("TRANSPORT_ERROR", err.Code);
        Assert.Same(cause, err.InnerException);
        Assert.True(left.Closed);
    }

    [Fact]
    public async Task FailsACrashedWorkerWithItsExitRatherThanATransportFailure()
    {
        var (left, _) = End.Pair();
        var ipc = Wrap(left);
        var cause = new IOException("broken");

        left.Break(cause);

        await left.Drained;
        await AssertPendingAsync(ipc.Ready);

        ipc.Exited(null, signal: 11);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_KILLED", err.Code);

        var failure = Assert.IsType<WorkerException>(err.InnerException);

        Assert.Equal("TRANSPORT_ERROR", failure.Code);
        Assert.Same(cause, failure.InnerException);
    }

    [Fact]
    public async Task KillsAWorkerWhoseTransportFailsAndThatDoesNotExit()
    {
        var (left, _) = End.Pair();
        var cause = new IOException("broken");
        Ipc? ipc = null;
        var killedBeforeFailing = false;

        ipc = new Ipc(left, () => killedBeforeFailing = !ipc!.Ready.IsCompleted, grace: Short);

        left.Break(cause);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("TRANSPORT_ERROR", err.Code);
        Assert.Same(cause, err.InnerException);
        Assert.True(killedBeforeFailing);

        ipc.Exited(1);

        Assert.Same(err, await Assert.ThrowsAsync<WorkerException>(() => ReadToEndAsync(ipc)));
    }

    [Fact]
    public async Task TreatsAResetTransportAsClosed()
    {
        var (left, _) = End.Pair();
        var ipc = Wrap(left);

        left.Break(new IOException("reset", new SocketException((int)SocketError.ConnectionReset)));

        await left.Drained;
        await AssertPendingAsync(ipc.Ready);

        var write = await Assert.ThrowsAsync<WorkerException>(() => ipc.WriteAsync("data"u8.ToArray(), TestContext.Current.CancellationToken).AsTask());

        Assert.Equal("WORKER_CLOSED", write.Code);

        ipc.Exited(1);

        var err = await Assert.ThrowsAsync<WorkerException>(() => ipc.Ready);

        Assert.Equal("WORKER_EXITED", err.Code);
        Assert.Null(err.InnerException);
    }
}
