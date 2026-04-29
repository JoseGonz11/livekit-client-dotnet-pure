using System;
using LiveKit.Proto;
using Google.Protobuf;
using System.Threading;
using LiveKit.Internal.FFIClients;
using LiveKit.Internal.FFIClients.Pools;
using LiveKit.Internal.FFIClients.Pools.Memory;
using LiveKit.Internal.FFIClients.Pools.ObjectPool;

namespace LiveKit.Internal
{
    internal sealed class FfiClient : IFFIClient
    {
        private static bool initialized = false;
        private static readonly Lazy<FfiClient> instance = new(() => new FfiClient());
        public static FfiClient Instance => instance.Value;

        internal SynchronizationContext? _context;

        private static bool _isDisposed = false;

        private readonly ThreadSafeObjectPool<FfiResponse> ffiResponsePool;
        private readonly MessageParser<FfiResponse> responseParser;
        private readonly IMemoryPool memoryPool;

        public event PublishTrackDelegate? PublishTrackReceived;
        public event UnpublishTrackDelegate? UnpublishTrackReceived;
        public event ConnectReceivedDelegate? ConnectReceived;
        public event DisconnectReceivedDelegate? DisconnectReceived;
        public event GetSessionStatsDelegate? GetSessionStatsReceived;
        public event RoomEventReceivedDelegate? RoomEventReceived;
        public event TrackEventReceivedDelegate? TrackEventReceived;
        public event RpcMethodInvocationReceivedDelegate? RpcMethodInvocationReceived;
        public event SetLocalMetadataReceivedDelegate? SetLocalMetadataReceived;
        public event SetLocalNameReceivedDelegate? SetLocalNameReceived;
        public event SetLocalAttributesReceivedDelegate? SetLocalAttributesReceived;

        // participant events are not allowed in the fii protocol public event ParticipantEventReceivedDelegate ParticipantEventReceived;
        public event VideoStreamEventReceivedDelegate? VideoStreamEventReceived;
        public event AudioStreamEventReceivedDelegate? AudioStreamEventReceived;
        public event CaptureAudioFrameReceivedDelegate? CaptureAudioFrameReceived;

        public event PerformRpcReceivedDelegate? PerformRpcReceived;

        public event ByteStreamReaderEventReceivedDelegate? ByteStreamReaderEventReceived;
        public event ByteStreamReaderReadAllReceivedDelegate? ByteStreamReaderReadAllReceived;
        public event ByteStreamReaderWriteToFileReceivedDelegate? ByteStreamReaderWriteToFileReceived;
        public event ByteStreamOpenReceivedDelegate? ByteStreamOpenReceived;
        public event ByteStreamWriterWriteReceivedDelegate? ByteStreamWriterWriteReceived;
        public event ByteStreamWriterCloseReceivedDelegate? ByteStreamWriterCloseReceived;
        public event SendFileReceivedDelegate? SendFileReceived;
        public event TextStreamReaderEventReceivedDelegate? TextStreamReaderEventReceived;
        public event TextStreamReaderReadAllReceivedDelegate? TextStreamReaderReadAllReceived;
        public event TextStreamOpenReceivedDelegate? TextStreamOpenReceived;
        public event TextStreamWriterWriteReceivedDelegate? TextStreamWriterWriteReceived;
        public event TextStreamWriterCloseReceivedDelegate? TextStreamWriterCloseReceived;
        public event SendTextReceivedDelegate? SendTextReceived;

        public FfiClient() : this(Pools.NewFfiResponsePool(), new ArrayMemoryPool())
        {
        }

        public FfiClient(
            ThreadSafeObjectPool<FfiResponse> ffiResponsePool,
            IMemoryPool memoryPool
        ) : this(
            ffiResponsePool,
            new MessageParser<FfiResponse>(ffiResponsePool.Get), memoryPool)
        {
        }

        public FfiClient(
            ThreadSafeObjectPool<FfiResponse> ffiResponsePool,
            MessageParser<FfiResponse> responseParser,
            IMemoryPool memoryPool
        )
        {
            this.responseParser = responseParser;
            this.memoryPool = memoryPool;
            this.ffiResponsePool = ffiResponsePool;
        }

        public void Initialize()
        {
            if (initialized) return;

            _context = SynchronizationContext.Current;
            
            if (_context == null) 
            {
                _context = new SynchronizationContext();
                Utils.Debug("SynchronizationContext was NULL, using a generic one.");
            }

            bool captureLogs = true; 
            NativeMethods.LiveKitInitialize(FFICallback, captureLogs, "dotnet", "1.0.0"); 

            Utils.Debug("FFIServer - Initialized");
            initialized = true;
        }

        public bool Initialized() => initialized;

        public void Dispose()
        {
#if NO_LIVEKIT_MODE
            return;
#endif
            if (_isDisposed) return;

            _isDisposed = true;

            // Stop all rooms synchronously
            // The rust lk implementation should also correctly dispose WebRTC
            try 
            {
                SendRequest(new FfiRequest { Dispose = new DisposeRequest() });
                Utils.Debug("FFIServer - Disposed");
            }
            catch (Exception ex)
            {
                Utils.Error($"Error disposing FFIServer: {ex.Message}");
            }
        }

        public void Release(FfiResponse response)
        {
            ffiResponsePool.Release(response);
        }

        public FfiResponse SendRequest(FfiRequest request)
        {
            try
            {
                unsafe
                {
                    using var memory = memoryPool.Memory(request);
                    var data = memory.Span();
                    request.WriteTo(data);
                    fixed (byte* requestDataPtr = data)
                    {
                        var handle = NativeMethods.FfiNewRequest(
                            requestDataPtr,
                            data.Length,
                            out byte* dataPtr,
                            out UIntPtr dataLen
                        );
                        var dataSpan = new Span<byte>(dataPtr, (int)dataLen.ToUInt64());
                        var response = responseParser.ParseFrom(dataSpan)!;
                        NativeMethods.FfiDropHandle(handle);
                        return response;
                    }
                }
            }
            catch (Exception e)
            {
                // Since we are in a thread I want to make sure we catch and log
                Utils.Error(e);
                // But we aren't actually handling this exception so we should re-throw here
                throw new Exception("Cannot send request", e);
            }
        }

        static unsafe void FFICallback(UIntPtr data, UIntPtr size)
        {
#if NO_LIVEKIT_MODE
            return;
#endif

            if (_isDisposed) return;

            var respData = new Span<byte>(data.ToPointer()!, (int)size.ToUInt64());
            var response = FfiEvent.Parser!.ParseFrom(respData);

            // If context fails, execute it directly
            if (Instance._context != null)
            {
                Instance._context.Post(ProcessEvent!, response);
            }
            else
            {
                ProcessEvent(response);
            }
        }
        
        // Extract switch for easier reading
        private static void ProcessEvent(object state)
        {
            var r = state as FfiEvent;
            
            if (r?.MessageCase != FfiEvent.MessageOneofCase.Logs)
                Utils.Debug("Callback: " + r?.MessageCase);

            switch (r?.MessageCase)
            {
                case FfiEvent.MessageOneofCase.Logs:
                    Utils.HandleLogBatch(r.Logs);
                    break;
                case FfiEvent.MessageOneofCase.Connect:
                    Instance.ConnectReceived?.Invoke(r.Connect!);
                    break;
                case FfiEvent.MessageOneofCase.PublishTrack:
                    Instance.PublishTrackReceived?.Invoke(r.PublishTrack!);
                    break;
                case FfiEvent.MessageOneofCase.UnpublishTrack:
                    Instance.UnpublishTrackReceived?.Invoke(r.UnpublishTrack!);
                    break;
                case FfiEvent.MessageOneofCase.RoomEvent:
                    Instance.RoomEventReceived?.Invoke(r.RoomEvent);
                    break;
                case FfiEvent.MessageOneofCase.SetLocalName:
                    Instance.SetLocalNameReceived?.Invoke(r.SetLocalName!);
                    break;
                case FfiEvent.MessageOneofCase.SetLocalMetadata:
                    Instance.SetLocalMetadataReceived?.Invoke(r.SetLocalMetadata!);
                    break;
                case FfiEvent.MessageOneofCase.SetLocalAttributes:
                    Instance.SetLocalAttributesReceived?.Invoke(r.SetLocalAttributes!);
                    break;
                case FfiEvent.MessageOneofCase.TrackEvent:
                    Instance.TrackEventReceived?.Invoke(r.TrackEvent!);
                    break;
                case FfiEvent.MessageOneofCase.RpcMethodInvocation:
                    Instance.RpcMethodInvocationReceived?.Invoke(r.RpcMethodInvocation);
                    break;
                case FfiEvent.MessageOneofCase.Disconnect:
                    Instance.DisconnectReceived?.Invoke(r.Disconnect!);
                    break;
                case FfiEvent.MessageOneofCase.GetStats:
                    Instance.GetSessionStatsReceived?.Invoke(r.GetStats);
                    break;
                case FfiEvent.MessageOneofCase.VideoStreamEvent:
                    Instance.VideoStreamEventReceived?.Invoke(r.VideoStreamEvent!);
                    break;
                case FfiEvent.MessageOneofCase.AudioStreamEvent:
                    Instance.AudioStreamEventReceived?.Invoke(r.AudioStreamEvent!);
                    break;
                case FfiEvent.MessageOneofCase.CaptureAudioFrame:
                    Instance.CaptureAudioFrameReceived?.Invoke(r.CaptureAudioFrame!);
                    break;
                case FfiEvent.MessageOneofCase.PerformRpc:
                    Instance.PerformRpcReceived?.Invoke(r.PerformRpc!);
                    break;
                case FfiEvent.MessageOneofCase.ByteStreamReaderEvent:
                    Instance.ByteStreamReaderEventReceived?.Invoke(r.ByteStreamReaderEvent!);
                    break;
                case FfiEvent.MessageOneofCase.ByteStreamReaderReadAll:
                    Instance.ByteStreamReaderReadAllReceived?.Invoke(r.ByteStreamReaderReadAll!);
                    break;
                case FfiEvent.MessageOneofCase.ByteStreamReaderWriteToFile:
                    Instance.ByteStreamReaderWriteToFileReceived?.Invoke(r.ByteStreamReaderWriteToFile!);
                    break;
                case FfiEvent.MessageOneofCase.ByteStreamOpen:
                    Instance.ByteStreamOpenReceived?.Invoke(r.ByteStreamOpen!);
                    break;
                case FfiEvent.MessageOneofCase.ByteStreamWriterWrite:
                    Instance.ByteStreamWriterWriteReceived?.Invoke(r.ByteStreamWriterWrite!);
                    break;
                case FfiEvent.MessageOneofCase.ByteStreamWriterClose:
                    Instance.ByteStreamWriterCloseReceived?.Invoke(r.ByteStreamWriterClose!);
                    break;
                case FfiEvent.MessageOneofCase.SendFile:
                    Instance.SendFileReceived?.Invoke(r.SendFile!);
                    break;
                case FfiEvent.MessageOneofCase.TextStreamReaderEvent:
                    Instance.TextStreamReaderEventReceived?.Invoke(r.TextStreamReaderEvent!);
                    break;
                case FfiEvent.MessageOneofCase.TextStreamReaderReadAll:
                    Instance.TextStreamReaderReadAllReceived?.Invoke(r.TextStreamReaderReadAll!);
                    break;
                case FfiEvent.MessageOneofCase.TextStreamOpen:
                    Instance.TextStreamOpenReceived?.Invoke(r.TextStreamOpen!);
                    break;
                case FfiEvent.MessageOneofCase.TextStreamWriterWrite:
                    Instance.TextStreamWriterWriteReceived?.Invoke(r.TextStreamWriterWrite!);
                    break;
                case FfiEvent.MessageOneofCase.TextStreamWriterClose:
                    Instance.TextStreamWriterCloseReceived?.Invoke(r.TextStreamWriterClose!);
                    break;
                case FfiEvent.MessageOneofCase.SendText:
                    Instance.SendTextReceived?.Invoke(r.SendText!);
                    break;
                
                // Ignorados o sin acción específica
                case FfiEvent.MessageOneofCase.PublishData:
                case FfiEvent.MessageOneofCase.PublishTranscription:
                case FfiEvent.MessageOneofCase.Panic:
                case FfiEvent.MessageOneofCase.None:
                    break;
                    
                default:
                    Utils.Error($"Unknown message type: {r?.MessageCase.ToString() ?? "null"}");
                    break;
            }
        }
    }
}