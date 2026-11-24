using System.Text;
using Xunit;

namespace Holepunch.BareStowHost.Tests;

public class FrameTests
{
    private static byte[] User(string data) => Frame.Encode(Frame.User, Encoding.UTF8.GetBytes(data));

    private static string[] Decode(params byte[][] chunks)
    {
        var decoder = new FrameDecoder();

        return [.. chunks.SelectMany(chunk => decoder.Feed(chunk)).Select(frame => $"{frame.Tag}:{Encoding.UTF8.GetString(frame.Payload)}")];
    }

    // Written by the JavaScript bare-stow-protocol for the same frames.
    [Fact]
    public void EncodesFramesLikeTheJavaScriptImplementation()
    {
        Assert.Equal(
            Convert.FromHexString("22000000007b2274797065223a22636f6e666967757265222c226772616365223a353030307d"),
            Frame.EncodeControl("configure", ("grace", 5000)));
        Assert.Equal(
            Convert.FromHexString("11000000007b2274797065223a227265616479227d"),
            Frame.EncodeControl("ready"));
        Assert.Equal(Convert.FromHexString("060000000168656c6c6f"), User("hello"));
    }

    [Fact]
    public void DecodesFramesSplitAcrossChunks()
    {
        byte[] data = [.. User("hello"), .. Frame.EncodeControl("ready")];

        Assert.Equal(["1:hello", "0:{\"type\":\"ready\"}"], Decode([.. data.Select(b => new[] { b })]));
    }

    [Fact]
    public void DecodesSeveralFramesFromOneChunk()
    {
        Assert.Equal(["1:a", "1:", "1:b"], Decode([.. User("a"), .. User(""), .. User("b")]));
    }

    [Fact]
    public void SkipsEmptyFrames()
    {
        Assert.Equal(["1:a"], Decode([0, 0, 0, 0, .. User("a")]));
    }

    [Fact]
    public void IgnoresInvalidControlPayloads()
    {
        Assert.Null(Frame.DecodeControl("not json"u8));
        Assert.Null(Frame.DecodeControl("[1, 2]"u8));
        Assert.Equal("ready", (string?)Frame.DecodeControl("{\"type\":\"ready\"}"u8)?["type"]);
    }
}
