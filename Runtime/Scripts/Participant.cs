using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using LiveKit.Internal;
using LiveKit.Proto;
using LiveKit.Internal.FFIClients.Requests;
using System.Diagnostics;

namespace LiveKit
{
    public delegate Task<string> RpcHandler(RpcInvocationData data);

    public class Participant
    {
        public delegate void PublishDelegate(RemoteTrackPublication publication);

        internal ParticipantInfo _info; // Can be updated by the server through room events.
        internal readonly Dictionary<string, TrackPublication> _tracks = new();
        public FfiHandle Handle;
        public string Sid => _info.Sid;
        public string Identity => _info.Identity;
        public string Name => _info.Name;
        public string Metadata => _info.Metadata;
        public IReadOnlyDictionary<string, string> Attributes => _info.Attributes;

        public ConnectionQuality ConnectionQuality { internal set; get; }
        public event PublishDelegate TrackPublished;
        public event PublishDelegate TrackUnpublished;

        public readonly WeakReference<Room> Room;
        public IReadOnlyDictionary<string, TrackPublication> Tracks => _tracks;

        protected Dictionary<string, RpcHandler> _rpcHandlers = new();

        protected Participant(OwnedParticipant participant, Room room)
        {
            Room = new WeakReference<Room>(room);
            Handle = FfiHandle.FromOwnedHandle(participant.Handle);
            _info = participant.Info;
        }

        [Obsolete("Use SetMetadata on LocalParticipant instead; this method has no effect")]
        public void SetMeta(string meta) {}

        [Obsolete("Use SetName on LocalParticipant instead; this method has no effect")]
        public void SetName(string name) {}

        internal void OnTrackPublished(RemoteTrackPublication publication)
        {
            TrackPublished?.Invoke(publication);
        }

        internal void OnTrackUnpublished(RemoteTrackPublication publication)
        {
            TrackUnpublished?.Invoke(publication);
        }
    }

    public sealed class LocalParticipant : Participant
    {
        public new IReadOnlyDictionary<string, LocalTrackPublication> Tracks =>
            base.Tracks.ToDictionary(p => p.Key, p => (LocalTrackPublication)p.Value);

        internal LocalParticipant(OwnedParticipant participant, Room room) : base(participant, room) { }

        public Task<LocalTrackPublication> PublishTrackAsync(ILocalTrack localTrack, TrackPublishOptions options)
        {
            var track = (Track)localTrack;
            using var request = FFIBridge.Instance.NewRequest<PublishTrackRequest>();
            var publish = request.request;
            publish.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            publish.TrackHandle = (ulong)track.Handle.DangerousGetHandle();
            publish.Options = options;

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.PublishTrack.AsyncId;

            var tcs = new TaskCompletionSource<LocalTrackPublication>();

            PublishTrackDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.PublishTrackReceived -= handler;

                if (!string.IsNullOrEmpty(e.Error))
                {
                    tcs.TrySetException(new Exception(e.Error));
                }
                else
                {
                    var publication = new LocalTrackPublication(e.Publication.Info);
                    publication.UpdateTrack(track);
                    localTrack.UpdateSid(publication.Sid);
                    _tracks.Add(e.Publication.Info.Sid, publication);
                    tcs.TrySetResult(publication);
                }
            };

            FfiClient.Instance.PublishTrackReceived += handler;
            return tcs.Task;
        }

        public Task UnpublishTrackAsync(ILocalTrack localTrack, bool stopOnUnpublish)
        {
            using var request = FFIBridge.Instance.NewRequest<UnpublishTrackRequest>();
            var unpublish = request.request;
            unpublish.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            unpublish.TrackSid = localTrack.Sid;

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.UnpublishTrack.AsyncId;

            var tcs = new TaskCompletionSource();

            UnpublishTrackDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.UnpublishTrackReceived -= handler;

                if (!string.IsNullOrEmpty(e.Error))
                    tcs.TrySetException(new Exception(e.Error));
                else
                {
                    _tracks.Remove(localTrack.Sid);
                    tcs.TrySetResult();
                }
            };

            FfiClient.Instance.UnpublishTrackReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Set the metadata for the local participant.
        /// </summary>
        /// <remarks>
        /// This requires `canUpdateOwnMetadata` permission.
        /// </remarks>
        /// <param name="metadata">The new metadata.</param>
        public Task SetMetadataAsync(string metadata)
        {
            using var request = FFIBridge.Instance.NewRequest<SetLocalMetadataRequest>();
            var setReq = request.request;
            setReq.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            setReq.Metadata = metadata;

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.SetLocalMetadata.AsyncId;

            var tcs = new TaskCompletionSource();
            SetLocalMetadataReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.SetLocalMetadataReceived -= handler;
                if (!string.IsNullOrEmpty(e.Error)) tcs.TrySetException(new Exception(e.Error));
                else tcs.TrySetResult();
            };
            FfiClient.Instance.SetLocalMetadataReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Set the name for the local participant.
        /// </summary>
        /// <remarks>
        /// This requires `canUpdateOwnMetadata` permission.
        /// </remarks>
        /// <param name="name">The new name.</param>
        public Task SetNameAsync(string name)
        {
            using var request = FFIBridge.Instance.NewRequest<SetLocalNameRequest>();
            var setReq = request.request;
            setReq.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            setReq.Name = name;

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.SetLocalName.AsyncId;

            var tcs = new TaskCompletionSource();
            SetLocalNameReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.SetLocalNameReceived -= handler;
                if (!string.IsNullOrEmpty(e.Error)) tcs.TrySetException(new Exception(e.Error));
                else tcs.TrySetResult();
            };
            FfiClient.Instance.SetLocalNameReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Set custom attributes for the local participant.
        /// </summary>
        /// <remarks>
        /// This requires `canUpdateOwnMetadata` permission.
        /// </remarks>
        /// <param name="attributes">The new attributes. Existing attributes that
        /// are not overridden will remain unchanged.</param>
        public Task SetAttributes(IDictionary<string, string> attributes)
        {
            using var request = FFIBridge.Instance.NewRequest<SetLocalAttributesRequest>();
            var setReq = request.request;
            setReq.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();

            // Lógica de mezcla de atributos (se mantiene igual)
            var newAttributes = new Dictionary<string, string>(Attributes);
            foreach (var kvp in attributes) newAttributes[kvp.Key] = kvp.Value;

            foreach (var kvp in newAttributes)
            {
                setReq.Attributes.Add(new AttributesEntry { Key = kvp.Key, Value = kvp.Value });
            }

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.SetLocalAttributes.AsyncId;

            var tcs = new TaskCompletionSource();
            SetLocalAttributesReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.SetLocalAttributesReceived -= handler;
                if (!string.IsNullOrEmpty(e.Error)) tcs.TrySetException(new Exception(e.Error));
                else tcs.TrySetResult();
            };

            FfiClient.Instance.SetLocalAttributesReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Performs RPC on another participant in the room.
        /// This allows you to execute a custom method on a remote participant and await their response.
        /// </summary>
        /// <param name="rpcParams">Parameters for the RPC call including:
        /// - DestinationIdentity: The identity of the participant to call
        /// - Method: Name of the method to call (up to 64 bytes UTF-8)
        /// - Payload: String payload (max 15KiB UTF-8)
        /// - ResponseTimeout: Maximum time to wait for response (defaults to 15 seconds)
        ///   If a value less than 8 seconds is provided, it will be automatically clamped to 8 seconds
        ///   to ensure sufficient time for round-trip latency buffering.</param>
        /// <returns>
        /// A <see cref="PerformRpcInstruction"/> that completes when the RPC call receives a response or errors.
        /// Check <see cref="PerformRpcInstruction.IsError"/> and access <see cref="PerformRpcInstruction.Payload"/>/<see cref="PerformRpcInstruction.Error"/> properties to handle the result.
        /// </returns>
        /// <remarks>
        /// See https://docs.livekit.io/home/client/data/rpc/#errors for a list of possible error codes.
        /// </remarks>
        public Task<string> PerformRpcAsync(PerformRpcParams rpcParams)
        {
            using var request = FFIBridge.Instance.NewRequest<PerformRpcRequest>();
            var rpcReq = request.request;
            rpcReq.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            rpcReq.DestinationIdentity = rpcParams.DestinationIdentity;
            rpcReq.Method = rpcParams.Method;
            rpcReq.Payload = rpcParams.Payload;
            rpcReq.ResponseTimeoutMs = (uint)(rpcParams.ResponseTimeout * 1000);

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.PerformRpc.AsyncId;

            var tcs = new TaskCompletionSource<string>();
            PerformRpcReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.PerformRpcReceived -= handler;
                if (e.Error != null) tcs.TrySetException(new Exception(e.Error.Message));
                else tcs.TrySetResult(e.Payload);
            };
            FfiClient.Instance.PerformRpcReceived += handler;
            return tcs.Task;
        }
        
        public void PublishData(byte[] data, IReadOnlyCollection<string> destination_identities = null, bool reliable = true, string topic = null)
        {
            unsafe
            {
                fixed (byte* pointer = data)
                {
                    if (!Room.TryGetTarget(out _)) throw new Exception("room is invalid");
                    using var request = FFIBridge.Instance.NewRequest<PublishDataRequest>();
                    var publish = request.request;
                    publish.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
                    publish.Reliable = reliable;
                    if (destination_identities != null) publish.DestinationIdentities.AddRange(destination_identities);
                    if (topic != null) publish.Topic = topic;
                    publish.DataLen = (ulong)data.Length;
                    publish.DataPtr = (ulong)pointer;
                    request.Send();
                }
            }
        }

        /// <summary>
        /// Unregisters a previously registered RPC method handler.
        /// </summary>
        /// <param name="method">The name of the RPC method to unregister</param>
        public void UnregisterRpcMethod(string method)
        {
            _rpcHandlers.Remove(method);

            using var request = FFIBridge.Instance.NewRequest<UnregisterRpcMethodRequest>();
            var unregisterReq = request.request;
            unregisterReq.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            unregisterReq.Method = method;
            var resp = request.Send();
        }

        internal async void HandleRpcMethodInvocation(
            ulong invocationId,
            string method,
            string requestId,
            string callerIdentity,
            string payload,
            float responseTimeout)
        {
            if (!_rpcHandlers.TryGetValue(method, out var handler))
            {
                SendRpcResponse(invocationId, null, RpcError.BuiltIn(RpcError.ErrorCode.UNSUPPORTED_METHOD));
                return;
            }

            try
            {
                var invocationData = new RpcInvocationData
                {
                    RequestId = requestId,
                    CallerIdentity = callerIdentity,
                    Payload = payload,
                    ResponseTimeout = responseTimeout
                };

                var result = await handler(invocationData);
                if (result == null)
                {
                    Utils.Error("RPC handler must return a string result");
                    SendRpcResponse(invocationId, null, RpcError.BuiltIn(RpcError.ErrorCode.APPLICATION_ERROR));
                    return;
                }

                SendRpcResponse(invocationId, result, null);
            }
            catch (RpcError rpcError)
            {
                SendRpcResponse(invocationId, null, rpcError);
            }
            catch (Exception e)
            {
                Utils.Error($"Uncaught error in RPC handler: {e}");
                SendRpcResponse(invocationId, null, RpcError.BuiltIn(RpcError.ErrorCode.APPLICATION_ERROR));
            }
        }


        private unsafe void PublishData(byte* data, int len, IReadOnlyCollection<string> destination_identities = null, bool reliable = true, string topic = null)
        {
            if (!Room.TryGetTarget(out var room))
                throw new Exception("room is invalid");

            using var request = FFIBridge.Instance.NewRequest<PublishDataRequest>();

            var publish = request.request;
            publish.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();

            // Clear previous values of conditional fields
            publish.DestinationIdentities.Clear();
            publish.ClearTopic();

            publish.Reliable = reliable;

            if (destination_identities is not null)
            {
                publish.DestinationIdentities.AddRange(destination_identities);
            }

            if (topic is not null)
            {
                publish.Topic = topic;
            }

            unsafe
            {
                publish.DataLen = (ulong)len;
                publish.DataPtr = (ulong)data;
            }
            Utils.Debug("Sending message: " + topic);
            var response = request.Send();
        }

        /// <summary>
        /// Send text to participants in the room.
        /// </summary>
        /// <param name="text">The text content to send.</param>
        /// <param name="options">Configuration options for the text stream, including topic and
        /// destination participants.</param>
        /// <returns>
        /// A <see cref="SendTextInstruction"/> that completes when the text is sent or errors.
        /// Check <see cref="SendTextInstruction.IsError"/> and access <see cref="SendTextInstruction.Info"/>
        /// properties to handle the result.
        /// </returns>
        ///
        public Task<TextStreamInfo> SendTextAsync(string text, StreamTextOptions options)
        {
            using var request = FFIBridge.Instance.NewRequest<StreamSendTextRequest>();
            var sendTextReq = request.request;
            sendTextReq.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            sendTextReq.Text = text;
            sendTextReq.Options = options.ToProto();

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.SendText.AsyncId;

            var tcs = new TaskCompletionSource<TextStreamInfo>();
            SendTextReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.SendTextReceived -= handler;
                if (e.ResultCase == StreamSendTextCallback.ResultOneofCase.Error)
                    tcs.TrySetException(new Exception(e.Error.Description));
                else
                    tcs.TrySetResult(new TextStreamInfo(e.Info));
            };

            FfiClient.Instance.SendTextReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Send text to participants in the room.
        /// </summary>
        /// <param name="text">The text content to send.</param>
        /// <param name="topic">Topic identifier used to route the stream to appropriate handlers.</param>
        /// <remarks>
        /// Use the <see cref="SendText(string, StreamTextOptions)"/> overload to set custom stream options.
        /// </remarks>
        /// <returns>
        /// A <see cref="SendTextInstruction"/> that completes when the text is sent or errors.
        /// Check <see cref="SendTextInstruction.IsError"/> and access <see cref="SendTextInstruction.Info"/>
        /// properties to handle the result.
        /// </returns>
        ///
        public Task<TextStreamInfo> SendTextAsync(string text, string topic)
        {
            var options = new StreamTextOptions();
            options.Topic = topic;
            return SendTextAsync(text, options);
        }

        /// <summary>
        /// Send a file on disk to participants in the room.
        /// </summary>
        /// <param name="path">Path to the file to be sent.</param>
        /// <param name="options">Configuration options for the byte stream, including topic and
        /// destination participants.</param>
        /// <returns>
        /// A <see cref="SendFileInstruction"/> that completes when the file is sent or errors.
        /// Check <see cref="SendFileInstruction.IsError"/> and access <see cref="SendFileInstruction.Info"/>
        /// properties to handle the result.
        /// </returns>
        ///
        public Task<ByteStreamInfo> SendFileAsync(string path, StreamByteOptions options)
        {
            using var request = FFIBridge.Instance.NewRequest<StreamSendFileRequest>();
            var sendFileReq = request.request;
            sendFileReq.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            sendFileReq.FilePath = path;
            sendFileReq.Options = options.ToProto();

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.SendFile.AsyncId;

            var tcs = new TaskCompletionSource<ByteStreamInfo>();
            SendFileReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.SendFileReceived -= handler;
                if (e.ResultCase == StreamSendFileCallback.ResultOneofCase.Error)
                    tcs.TrySetException(new Exception(e.Error.Description));
                else
                    tcs.TrySetResult(new ByteStreamInfo(e.Info));
            };

            FfiClient.Instance.SendFileReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Send a file on disk to participants in the room.
        /// </summary>
        /// <param name="path">Path to the file to be sent.</param>
        /// <param name="topic">Topic identifier used to route the stream to appropriate handlers.</param>
        /// <remarks>
        /// Use the <see cref="SendFile(string, StreamByteOptions)"/> overload to set custom stream options.
        /// </remarks>
        /// <returns>
        /// A <see cref="SendFileInstruction"/> that completes when the file is sent or errors.
        /// Check <see cref="SendFileInstruction.IsError"/> and access <see cref="SendFileInstruction.Info"/>
        /// properties to handle the result.
        /// </returns>
        ///
        public Task<ByteStreamInfo> SendFileAsync(string path, string topic)
        {
            var options = new StreamByteOptions();
            options.Topic = topic;
            return SendFileAsync(path, options);
        }

        /// <summary>
        /// Stream bytes incrementally to participants in the room.
        /// </summary>
        /// <remarks>
        /// This method allows sending byte data in chunks as it becomes available.
        /// Unlike <see cref="SendFile"/>, which sends the entire file at once, this method allows
        /// using a writer to send byte data incrementally.
        /// </remarks>
        /// <param name="options">Configuration options for the byte stream, including topic and
        /// destination participants.</param>
        /// <returns>
        /// A <see cref="StreamBytesInstruction"/> that completes once the stream is open or errors.
        /// Check <see cref="StreamBytesInstruction.IsError"/> and access <see cref="StreamBytesInstruction.Writer"/>
        /// to access the writer for the opened stream.
        /// </returns>
        public Task<ByteStreamWriter> StreamBytesAsync(StreamByteOptions options)
        {
            using var request = FFIBridge.Instance.NewRequest<ByteStreamOpenRequest>();
            request.request.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            request.request.Options = options.ToProto();

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.ByteStreamOpen.AsyncId;

            var tcs = new TaskCompletionSource<ByteStreamWriter>();
            ByteStreamOpenReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.ByteStreamOpenReceived -= handler;
                if (e.ResultCase == ByteStreamOpenCallback.ResultOneofCase.Error)
                    tcs.TrySetException(new Exception(e.Error.Description));
                else
                    tcs.TrySetResult(new ByteStreamWriter(e.Writer));
            };

            FfiClient.Instance.ByteStreamOpenReceived += handler;
            return tcs.Task;
        }

        /// <summary>
        /// Stream bytes to participants in the room.
        /// </summary>
        /// <remarks>
        /// Use the <see cref="StreamBytes(StreamByteOptions)"/> overload to set custom stream options.
        /// </remarks>
        /// <param name="topic">Topic identifier used to route the stream to appropriate handlers.</param>
        /// <returns>
        /// A <see cref="StreamBytesInstruction"/> that completes once the stream is open or errors.
        /// Check <see cref="StreamBytesInstruction.IsError"/> and access <see cref="StreamBytesInstruction.Writer"/>
        /// to access the writer for the opened stream.
        /// </returns>
        public Task<ByteStreamWriter> StreamBytesAsync(string topic)
        {
            var options = new StreamByteOptions { Topic = topic };
            return StreamBytesAsync(options);
        }

        /// <summary>
        /// Stream text to participants in the room.
        /// </summary>
        /// <remarks>
        /// Use the <see cref="StreamText(StreamTextOptions)"/> overload to set custom stream options.
        /// </remarks>
        /// <param name="topic">Topic identifier used to route the stream to appropriate handlers.</param>
        /// <returns>
        /// A <see cref="StreamTextInstruction"/> that completes once the stream is open or errors.
        /// Check <see cref="StreamTextInstruction.IsError"/> and access <see cref="StreamTextInstruction.Writer"/>
        /// to access the writer for the opened stream.
        /// </returns>
        public Task<TextStreamWriter> StreamTextAsync(StreamTextOptions options)
        {
            using var request = FFIBridge.Instance.NewRequest<TextStreamOpenRequest>();
            request.request.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            request.request.Options = options.ToProto();

            using var responseWrap = request.Send();
            FfiResponse res = responseWrap;
            var asyncId = res.TextStreamOpen.AsyncId;

            var tcs = new TaskCompletionSource<TextStreamWriter>();
            TextStreamOpenReceivedDelegate handler = null!;
            handler = e =>
            {
                if (e.AsyncId != asyncId) return;
                FfiClient.Instance.TextStreamOpenReceived -= handler;
                if (e.ResultCase == TextStreamOpenCallback.ResultOneofCase.Error)
                    tcs.TrySetException(new Exception(e.Error.Description));
                else
                    tcs.TrySetResult(new TextStreamWriter(e.Writer));
            };
            FfiClient.Instance.TextStreamOpenReceived += handler;
            return tcs.Task;
        }
        
        public Task<TextStreamWriter> StreamTextAsync(string topic)
        {
            var options = new StreamTextOptions { Topic = topic };
            return StreamTextAsync(options);
        }
        
        /// <summary>
        /// Registers a new RPC method handler.
        /// </summary>
        /// <param name="method">The name of the RPC method to register</param>
        /// <param name="handler">The async callback that handles incoming RPC requests. It receives an RpcInvocationData object
        /// containing the caller's identity, payload (up to 15KiB UTF-8), and response timeout. Must return a string response or throw
        /// an RpcError. Any other exceptions will be converted to a generic APPLICATION_ERROR (1500).</param>
        public void RegisterRpcMethod(string method, RpcHandler handler)
        {
            _rpcHandlers[method] = handler;
            using var request = FFIBridge.Instance.NewRequest<RegisterRpcMethodRequest>();
            request.request.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            request.request.Method = method;
            request.Send();
        }

        /// <summary>
        /// Unregisters a previously registered RPC method handler.
        /// </summary>
        /// <param name="method">The name of the RPC method to unregister</param>
        private void SendRpcResponse(ulong invocationId, string responsePayload, RpcError responseError)
        {
            using var request = FFIBridge.Instance.NewRequest<RpcMethodInvocationResponseRequest>();
            var rpcResp = request.request;
            rpcResp.LocalParticipantHandle = (ulong)Handle.DangerousGetHandle();
            rpcResp.InvocationId = invocationId;
            if (responseError != null) rpcResp.Error = responseError.ToProto();
            if (responsePayload != null) rpcResp.Payload = responsePayload;
            request.Send();
        }
    }

    public sealed class RemoteParticipant : Participant
    {
        public new IReadOnlyDictionary<string, RemoteTrackPublication> Tracks =>
            base.Tracks.ToDictionary(p => p.Key, p => (RemoteTrackPublication)p.Value);

        internal RemoteParticipant(OwnedParticipant participant, Room room) : base(participant, room) { }
    }
}
