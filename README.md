# bare-stow-host-dotnet

Host side of the [bare-stow](https://github.com/holepunchto/bare-stow) lifecycle for .NET. The .NET counterpart of [bare-stow-host](https://github.com/holepunchto/bare-stow-host): it multiplexes a control channel and a user data channel over a byte transport to a stowed bundle, and layers readiness, termination, and exit reporting on top.

```
dotnet add package Holepunch.BareStowHost
```

## Usage

```csharp
using Holepunch.BareSidecar;
using Holepunch.BareStowHost;

using var sidecar = Sidecar.Start("index.bundle");

await using var ipc = new Ipc(sidecar, kill: sidecar.Process.Kill);

_ = sidecar.Process.WaitForExitAsync().ContinueWith(_ => ipc.Exited(sidecar.Process.ExitCode));

await ipc.Ready;

await ipc.WriteAsync("hello"u8.ToArray());

var code = await ipc.TerminateAsync();
```

Any `Stream` can be wrapped, such as a `Sidecar` from [bare-sidecar-dotnet](https://github.com/holepunchto/bare-sidecar-dotnet) or a `Worklet` from [bare-kit-dotnet](https://github.com/holepunchto/bare-kit-dotnet). The `Ipc` is itself a `Stream`: Reading from it yields the user data from the worker, and writing to it sends user data to the worker. `kill` defaults to disposing the transport.

Whoever creates the `Ipc` must call `Exited()` once the worker exits, as observed by the transport, such as the process of a sidecar. A transport that ends or fails without the worker saying why is held back until then, so that the handle fails with the reason.

## Protocol

Every frame is a 4-byte little-endian length followed by that many bytes: A 1-byte tag and the payload. The tag is `0x00` for a control frame, whose payload is a UTF-8 JSON object carrying its `type`, and `0x01` for a user data frame. See [bare-stow-protocol](https://github.com/holepunchto/bare-stow-protocol) for the control frames.

## License

Apache-2.0
