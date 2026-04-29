using System;
using System.Collections.Generic;
using LiveKit.Internal.FFIClients.Requests;
using System.Linq;
using System.Net.Mime;
using System.Runtime.InteropServices.JavaScript;
using System.Threading.Channels;
using System.Threading.Tasks;
using LiveKit.Internal;
using LiveKit.Proto;

namespace LiveKit
{
    /// <summary>
    /// Information about a data stream.
    /// </summary>
    public class StreamInfo
    {
        /// <summary>
        /// Unique identifier of the stream.
        /// </summary>
        public string Id { get; }

        /// <summary>
        /// Topic name used to route the stream to the appropriate handler.
        /// </summary>
        public string Topic { get; }

        /// <summary>
        /// When the stream was created.
        /// </summary>
        public DateTime Timestamp { get; }

        /// <summary>
        /// Total expected size in bytes, if known.
        /// </summary>
        public ulong? TotalLength { get; }

        /// <summary>
        /// Additional attributes as needed for your application.
        /// </summary>
        public IReadOnlyDictionary<string, string> Attributes { get; }

        /// <summary>
        /// The MIME type of the stream data.
        /// </summary>
        public string MimeType { get; }

        internal StreamInfo(
            string id,
            string topic,
            long timestamp,
            ulong? totalLength,
            Google.Protobuf.Collections.MapField<string, string> attributes,
            string mimeType)
        {
            Id = id;
            Topic = topic;
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timestamp).DateTime;
            TotalLength = totalLength;
            Attributes = attributes.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            MimeType = mimeType;
        }
    }

    /// <summary>
    /// Information about a text data stream.
    /// </summary>
    public sealed class TextStreamInfo : StreamInfo
    {
        /// <summary>
        /// Operation type for text streams.
        /// </summary>
        public enum OperationType
        {
            Create = 0,
            Update = 1,
            Delete = 2,
            Reaction = 3
        }

        public OperationType Operation { get; }
        public int Version { get; }
        public string ReplyToStreamId { get; }
        public IReadOnlyList<string> AttachedStreamIds { get; }
        public bool Generated { get; }

        internal TextStreamInfo(Proto.TextStreamInfo proto) : base(
            proto.StreamId,
            proto.Topic,
            proto.Timestamp,
            proto.TotalLength,
            proto.Attributes,
            proto.MimeType)
        {
            Operation = (OperationType)proto.OperationType;
            Version = proto.Version;
            ReplyToStreamId = proto.ReplyToStreamId;
            AttachedStreamIds = proto.AttachedStreamIds;
            Generated = proto.Generated;
        }
    }

    /// <summary>
    /// Information about a byte data stream.
    /// </summary>
    public sealed class ByteStreamInfo : StreamInfo
    {
        public string Name { get; }

        internal ByteStreamInfo(Proto.ByteStreamInfo proto) : base(
            proto.StreamId,
            proto.Topic,
            proto.Timestamp,
            proto.TotalLength,
            proto.Attributes,
            proto.MimeType)
        {
            Name = proto.Name;
        }
    }

    /// <summary>
    /// Delegate for handling incoming text data streams.
    /// </summary>
    public delegate void TextStreamHandler(TextStreamReader reader, string identity);

    /// <summary>
    /// Delegate for handling incoming byte data streams.
    /// </summary>
    public delegate void ByteStreamHandler(ByteStreamReader reader, string identity);

    /// <summary>
    /// Error for data stream operations.
    /// </summary>
    public sealed class StreamError : Exception
    {
        public StreamError(string message) : base(message) { }

        internal StreamError(Proto.StreamError proto) : base(proto.Description) { }
    }

    /// <summary>
    /// Reader for an incoming text data stream.
    /// </summary>
    public sealed class TextStreamReader
    {
        private readonly FfiHandle _handle;
        private readonly TextStreamInfo _info;

        internal TextStreamReader(OwnedTextStreamReader info)
        {
            _handle = FfiHandle.FromOwnedHandle(info.Handle);
            _info = new TextStreamInfo(info.Info);
        }

        public TextStreamInfo Info => _info;

        // Convert to Task<string>
        /// <summary>
        /// Reads all incoming chunks from the stream, concatenating them into a single value
        /// once the stream closes normally.
        /// </summary>
        /// <remarks>Calling this method consumes the stream reader.</remarks>
        /// <returns>
        /// A <see cref="ReadAllInstruction"/> that completes when the stream is complete or errors.
        /// Check <see cref="ReadAllInstruction.IsError"/> and access <see cref="MediaTypeNames.Text"/>
        /// properties to handle the result.
        /// </returns>
        public Task<string> ReadAllAsync()
        {
            using var request = FFIBridge.Instance.NewRequest<TextStreamReaderReadAllRequest>();
            var readAllReq = request.request;
            readAllReq.ReaderHandle = (ulong)_handle.DangerousGetHandle();

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.TextReadAll.AsyncId;
            
            var tcs = new TaskCompletionSource<string>();
            
            TextStreamReaderReadAllReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                
                FfiClient.Instance.TextStreamReaderReadAllReceived -= handler;

                if (e.ResultCase == TextStreamReaderReadAllCallback.ResultOneofCase.Error)
                    tcs.TrySetException(new StreamError(e.Error.Description));
                else
                    tcs.TrySetResult(e.Content);
            };

            FfiClient.Instance.TextStreamReaderReadAllReceived += handler;
            return tcs.Task;
        }

        // Removed 'ReadAllInstruction'

        /// <summary>
        /// Reads incoming chunks from the stream incrementally.
        /// </summary>
        /// <returns>
        /// A <see cref="ReadIncrementalInstruction"/> that allows reading the stream incrementally.
        /// </returns>
        public async IAsyncEnumerable<string> ReadIncrementalAsync()
        {
            var channel = Channel.CreateUnbounded<string>();

            TextStreamReaderEventReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.ReaderHandle != (ulong)_handle.DangerousGetHandle()) return;

                if (e.DetailCase == TextStreamReaderEvent.DetailOneofCase.ChunkReceived)
                {
                    channel.Writer.TryWrite(e.ChunkReceived.Content);
                }
                else if (e.DetailCase == TextStreamReaderEvent.DetailOneofCase.Eos)
                {
                    FfiClient.Instance.TextStreamReaderEventReceived -= handler;
                    
                    if (e.Eos.Error != null)
                        channel.Writer.TryComplete(new StreamError(e.Eos.Error.Description));
                    else
                        channel.Writer.TryComplete();
                }
            };

            FfiClient.Instance.TextStreamReaderEventReceived += handler;

            using var request = FFIBridge.Instance.NewRequest<TextStreamReaderReadIncrementalRequest>();
            request.request.ReaderHandle = (ulong)_handle.DangerousGetHandle();
            request.Send();

            await foreach (var chunk in channel.Reader.ReadAllAsync())
            {
                yield return chunk;
            }
        }

        // Removed ReadIncrementalInstruction
    }

    /// <summary>
    /// Reader for an incoming byte data stream.
    /// </summary>
    public sealed class ByteStreamReader
    {
        private FfiHandle _handle;
        private readonly ByteStreamInfo _info;

        internal ByteStreamReader(OwnedByteStreamReader info)
        {
            _handle = FfiHandle.FromOwnedHandle(info.Handle);
            _info = new ByteStreamInfo(info.Info);
        }

        public ByteStreamInfo Info => _info;

        /// <summary>
        /// Reads all incoming chunks from the stream, concatenating them into a single value
        /// once the stream closes normally.
        /// </summary>
        /// <remarks>Calling this method consumes the stream reader.</remarks>
        /// <returns>
        /// A <see cref="ReadAllInstruction"/> that completes when the stream is complete or errors.
        /// Check <see cref="ReadAllInstruction.IsError"/> and access <see cref="ReadAllInstruction.Bytes"/>
        /// properties to handle the result.
        /// </returns>
        public Task<byte[]> ReadAllAsync()
        {
            using var request = FFIBridge.Instance.NewRequest<ByteStreamReaderReadAllRequest>();
            var readAllReq = request.request;
            readAllReq.ReaderHandle = (ulong)_handle.DangerousGetHandle();

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.ByteReadAll.AsyncId;

            var tcs = new TaskCompletionSource<byte[]>();

            ByteStreamReaderReadAllReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;

                FfiClient.Instance.ByteStreamReaderReadAllReceived -= handler;

                if (e.ResultCase == ByteStreamReaderReadAllCallback.ResultOneofCase.Error)
                    tcs.TrySetException(new StreamError(e.Error.Description));
                else
                    tcs.TrySetResult(e.Content.ToByteArray());
            };

            FfiClient.Instance.ByteStreamReaderReadAllReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Reads incoming chunks from the stream incrementally.
        /// </summary>
        /// <returns>
        /// A <see cref="ReadIncrementalInstruction"/> that allows reading the stream incrementally.
        /// </returns>
        public async IAsyncEnumerable<byte[]> ReadIncrementalAsync()
        {
            var channel = Channel.CreateUnbounded<byte[]>();

            ByteStreamReaderEventReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.ReaderHandle != (ulong)_handle.DangerousGetHandle()) return;

                if (e.DetailCase == ByteStreamReaderEvent.DetailOneofCase.ChunkReceived)
                {
                    channel.Writer.TryWrite(e.ChunkReceived.Content.ToByteArray());
                }
                else if (e.DetailCase == ByteStreamReaderEvent.DetailOneofCase.Eos)
                {
                    FfiClient.Instance.ByteStreamReaderEventReceived -= handler;

                    if (e.Eos.Error != null)
                        channel.Writer.TryComplete(new StreamError(e.Eos.Error.Description));
                    else
                        channel.Writer.TryComplete();
                }
            };

            FfiClient.Instance.ByteStreamReaderEventReceived += handler;

            using var request = FFIBridge.Instance.NewRequest<ByteStreamReaderReadIncrementalRequest>();
            request.request.ReaderHandle = (ulong)_handle.DangerousGetHandle();
            request.Send();

            await foreach (var chunk in channel.Reader.ReadAllAsync())
            {
                yield return chunk;
            }
        }

        /// <summary>
        /// Reads incoming chunks from the byte stream, writing them to a file as they are received.
        /// </summary>
        /// <param name="directory">The directory to write the file in. The system temporary directory is used if not specified.</param>
        /// <param name="nameOverride">The name to use for the written file, overriding stream name.</param>
        /// <remarks>
        /// Calling this method consumes the stream reader.
        /// </remarks>
        /// <returns>
        /// A <see cref="WriteToFileInstruction"/> that completes when the stream is complete or errors.
        /// Check <see cref="WriteToFileInstruction.IsError"/> and access <see cref="WriteToFileInstruction.FilePath"/>
        /// properties to handle the result.
        /// </returns>
        public Task<string> WriteToFileAsync(string directory = null, string nameOverride = null)
        {
            using var request = FFIBridge.Instance.NewRequest<ByteStreamReaderWriteToFileRequest>();
            var writeToFileReq = request.request;
            writeToFileReq.ReaderHandle = (ulong)_handle.DangerousGetHandle();
            if (directory != null) writeToFileReq.Directory = directory;
            if (nameOverride != null) writeToFileReq.NameOverride = nameOverride;

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.ByteWriteToFile.AsyncId;

            var tcs = new TaskCompletionSource<string>();

            ByteStreamReaderWriteToFileReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;

                FfiClient.Instance.ByteStreamReaderWriteToFileReceived -= handler;

                if (e.ResultCase == ByteStreamReaderWriteToFileCallback.ResultOneofCase.Error)
                    tcs.TrySetException(new StreamError(e.Error.Description));
                else
                    tcs.TrySetResult(e.FilePath);
            };

            FfiClient.Instance.ByteStreamReaderWriteToFileReceived += handler;
            return tcs.Task;
        }

        // Removed ReadAllInstruction

        // Removed ReadIncrementalInstruction
        
        // Remove WriteToFileInstruction
    }

    /// <summary>
    /// Options used when opening an outgoing data stream.
    /// </summary>
    public class StreamOptions
    {
        public string Topic { get; set; }
        public IDictionary<string, string> Attributes { get; set; } = new Dictionary<string, string>();
        public List<string> DestinationIdentities { get; set; } = new List<string>();
        public string Id { get; set; }
    }

    /// <summary>
    /// Options used when opening an outgoing text data stream.
    /// </summary>
    public class StreamTextOptions : StreamOptions
    {
        public TextStreamInfo.OperationType? OperationType { get; set; }
        public int? Version { get; set; }
        public string ReplyToStreamId { get; set; }
        public List<string> AttachedStreamIds { get; set; } = new List<string>();
        public bool? Generated { get; set; }

        internal Proto.StreamTextOptions ToProto()
        {
            var proto = new Proto.StreamTextOptions();
            if (Topic == null)
            {
                throw new InvalidOperationException("Topic field is required");
            }
            proto.Topic = Topic;
            proto.Attributes.Add(Attributes);
            proto.DestinationIdentities.AddRange(DestinationIdentities);

            // TODO: these fields are optional, but the generated proto is not allowing null values
            if (Id != null) proto.Id = Id;
            if (OperationType != null) proto.OperationType = (Proto.TextStreamInfo.Types.OperationType)OperationType;
            if (Version != null) proto.Version = Version.Value;
            if (ReplyToStreamId != null) proto.ReplyToStreamId = ReplyToStreamId;
            proto.AttachedStreamIds.AddRange(AttachedStreamIds);
            if (Generated != null) proto.Generated = Generated.Value;
            return proto;
        }
    }

    /// <summary>
    /// Options used when opening an outgoing byte data stream.
    /// </summary>
    public class StreamByteOptions : StreamOptions
    {
        public string MimeType { get; set; }
        public string Name { get; set; }
        public ulong? TotalLength { get; set; }

        internal Proto.StreamByteOptions ToProto()
        {
            var proto = new Proto.StreamByteOptions();
            if (Topic == null)
            {
                throw new InvalidOperationException("Topic field is required");
            }
            proto.Topic = Topic;
            proto.Attributes.Add(Attributes);
            proto.DestinationIdentities.AddRange(DestinationIdentities);
            // TODO: these fields are optional, but the generated proto is not allowing null values
            if (Id != null) proto.Id = Id;
            if (MimeType != null) proto.MimeType = MimeType;
            if (Name != null) proto.Name = Name;
            if (TotalLength != null) proto.TotalLength = TotalLength.Value;
            return proto;
        }
    }

    /// <summary>
    /// Writer for an outgoing text data stream.
    /// </summary>
    public class TextStreamWriter
    {
        private FfiHandle _handle;
        private readonly TextStreamInfo _info;

        internal TextStreamWriter(OwnedTextStreamWriter info)
        {
            _handle = FfiHandle.FromOwnedHandle(info.Handle);
            _info = new TextStreamInfo(info.Info);
        }

        public TextStreamInfo Info => _info;

        /// <summary>
        /// Writes text to the stream.
        /// </summary>
        /// <param name="text">The text to write.</param>
        /// <returns>
        /// A <see cref="WriteInstruction"/> that completes when the write operation is complete or errors.
        /// Check <see cref="JSType.Error"/> to see if the operation was successful.
        /// </returns>
        public Task WriteAsync(string text)
        {
            using var request = FFIBridge.Instance.NewRequest<TextStreamWriterWriteRequest>();
            var writeReq = request.request;
            writeReq.WriterHandle = (ulong)_handle.DangerousGetHandle();
            writeReq.Text = text;

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.TextStreamWrite.AsyncId;

            var tcs = new TaskCompletionSource();

            TextStreamWriterWriteReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;

                FfiClient.Instance.TextStreamWriterWriteReceived -= handler;

                if (e.Error != null)
                    tcs.TrySetException(new StreamError(e.Error.Description));
                else
                    tcs.TrySetResult();
            };

            FfiClient.Instance.TextStreamWriterWriteReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Closes the stream.
        /// </summary>
        /// <param name="reason">A string specifying the reason for closure, if the stream is not being closed normally.</param>
        /// <returns>
        /// A <see cref="CloseInstruction"/> that completes when the close operation is complete or errors.
        /// Check <see cref="JSType.Error"/> to see if the operation was successful.
        /// </returns>
        public Task CloseAsync(string reason = null)
        {
            using var request = FFIBridge.Instance.NewRequest<TextStreamWriterCloseRequest>();
            var closeReq = request.request;
            closeReq.WriterHandle = (ulong)_handle.DangerousGetHandle();
            if (reason != null) closeReq.Reason = reason;

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.TextStreamWrite.AsyncId;

            var tcs = new TaskCompletionSource();

            TextStreamWriterCloseReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;

                FfiClient.Instance.TextStreamWriterCloseReceived -= handler;

                if (e.Error != null)
                    tcs.TrySetException(new StreamError(e.Error.Description));
                else
                    tcs.TrySetResult();
            };

            FfiClient.Instance.TextStreamWriterCloseReceived += handler;
            return tcs.Task;
        }
        
        // Removed WriteInstruction
        // Removed CloseInstruction
    }

    /// <summary>
    /// Writer for an outgoing byte data stream.
    /// </summary>
    public class ByteStreamWriter
    {
        private FfiHandle _handle;
        private readonly ByteStreamInfo _info;

        internal ByteStreamWriter(OwnedByteStreamWriter info)
        {
            _handle = FfiHandle.FromOwnedHandle(info.Handle);
            _info = new ByteStreamInfo(info.Info);
        }

        public ByteStreamInfo Info => _info;

        /// <summary>
        /// Writes bytes to the stream.
        /// </summary>
        /// <param name="bytes">The bytes to write.</param>
        /// <returns>
        /// A <see cref="WriteInstruction"/> that completes when the write operation is complete or errors.
        /// </returns>
        public Task WriteAsync(byte[] bytes)
        {
            using var request = FFIBridge.Instance.NewRequest<ByteStreamWriterWriteRequest>();
            var writeReq = request.request;
            writeReq.WriterHandle = (ulong)_handle.DangerousGetHandle();
            writeReq.Bytes = Google.Protobuf.ByteString.CopyFrom(bytes);

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.ByteStreamWrite.AsyncId;

            var tcs = new TaskCompletionSource();

            ByteStreamWriterWriteReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;

                FfiClient.Instance.ByteStreamWriterWriteReceived -= handler;

                if (e.Error != null)
                    tcs.TrySetException(new StreamError(e.Error.Description));
                else
                    tcs.TrySetResult();
            };

            FfiClient.Instance.ByteStreamWriterWriteReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Closes the stream.
        /// </summary>
        /// <param name="reason">A string specifying the reason for closure, if the stream is not being closed normally.</param>
        /// <returns>
        /// A <see cref="CloseInstruction"/> that completes when the close operation is complete or errors.
        /// </returns>
        public Task CloseAsync(string reason = null)
        {
            using var request = FFIBridge.Instance.NewRequest<ByteStreamWriterCloseRequest>();
            var closeReq = request.request;
            closeReq.WriterHandle = (ulong)_handle.DangerousGetHandle();
            if (reason != null) closeReq.Reason = reason;

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.ByteStreamWrite.AsyncId;

            var tcs = new TaskCompletionSource();

            ByteStreamWriterCloseReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;

                FfiClient.Instance.ByteStreamWriterCloseReceived -= handler;

                if (e.Error != null)
                    tcs.TrySetException(new StreamError(e.Error.Description));
                else
                    tcs.TrySetResult();
            };

            FfiClient.Instance.ByteStreamWriterCloseReceived += handler;
            return tcs.Task;
        }
        
        // Removed WriteInstruction
        // Removed CloseInstruction
    }

    internal sealed class StreamHandlerRegistry
    {
        private readonly Dictionary<string, TextStreamHandler> _textStreamHandlers = new();
        private readonly Dictionary<string, ByteStreamHandler> _byteStreamHandlers = new();

        internal void RegisterTextStreamHandler(string topic, TextStreamHandler handler)
        {
            if (!_textStreamHandlers.TryAdd(topic, handler))
            {
                throw new StreamError($"Text stream handler already registered for topic: {topic}");
            }
        }

        internal void RegisterByteStreamHandler(string topic, ByteStreamHandler handler)
        {
            if (!_byteStreamHandlers.TryAdd(topic, handler))
            {
                throw new StreamError($"Byte stream handler already registered for topic: {topic}");
            }
        }

        internal void UnregisterTextStreamHandler(string topic) => _textStreamHandlers.Remove(topic);
        internal void UnregisterByteStreamHandler(string topic) => _byteStreamHandlers.Remove(topic);

        internal bool Dispatch(TextStreamReader reader, string participantIdentity)
        {
            if (_textStreamHandlers.TryGetValue(reader.Info.Topic, out var handler))
            {
                handler(reader, participantIdentity);
                return true;
            }
            return false;
        }

        internal bool Dispatch(ByteStreamReader reader, string participantIdentity)
        {
            if (_byteStreamHandlers.TryGetValue(reader.Info.Topic, out var handler))
            {
                handler(reader, participantIdentity);
                return true;
            }
            return false;
        }
    }
}