using System;
using LiveKit.Proto;
using LiveKit.Internal;
using LiveKit.Internal.FFIClients.Requests;

namespace LiveKit
{
    public abstract class IRtcSource
    {
        internal FfiHandle Handle { get; set; }
        
        public abstract void SetMute(bool muted);
        public abstract bool Muted { get; }
    }
    
    public interface ITrack
    {
        string Sid { get; protected set; }
        string Name { get; }
        TrackKind Kind { get; }
        StreamState StreamState { get; }
        bool Muted { get; }
        WeakReference<Room> Room { get; }
        WeakReference<Participant> Participant { get; }
        FfiHandle TrackHandle { get; }
    }

    public interface ILocalTrack : ITrack
    {
        IRtcSource source { get; }

        public void UpdateSid (string sid) {
            Sid = sid;
        }

        public void SetMute(bool muted)
        {
            using var request = FFIBridge.Instance.NewRequest<LocalTrackMuteRequest>();
            var createTrack = request.request;
            createTrack.Mute = muted;
            createTrack.TrackHandle = (ulong)TrackHandle.DangerousGetHandle();
            using var resp = request.Send();
            FfiResponse res = resp;
            source.SetMute(muted);
        }
    }

    public interface IRemoteTrack : ITrack
    {
        public void SetEnabled(bool enabled)
        {
            using var request = FFIBridge.Instance.NewRequest<EnableRemoteTrackRequest>();
            var req = request.request;
            req.Enabled = enabled;
            req.TrackHandle = (ulong)TrackHandle.DangerousGetHandle();
            using var resp = request.Send();
            FfiResponse res = resp;
        }
    }

    public interface IAudioTrack : ITrack
    {

    }

    public interface IVideoTrack : ITrack
    {
    }

    public class Track : ITrack
    {
        private TrackInfo _info;
        public TrackInfo Info => _info;

        public string Sid => _info.Sid;
        public ulong Id { get; private set; }
        public string Name => _info.Name;
        public TrackKind Kind => _info.Kind;
        public StreamState StreamState => _info.StreamState;
        public bool Muted => _info.Muted;
        public WeakReference<Room> Room { internal set; get; }
        public WeakReference<Participant> Participant { get; }

        public bool IsOwned => Handle != null && !Handle.IsInvalid;

        public readonly FfiHandle Handle;

        FfiHandle ITrack.TrackHandle => Handle;

        string ITrack.Sid { get => _info.Sid; set => _info.Sid = value; }

        internal Track(OwnedTrack track, Room room, Participant participant)
        {
            Id = track.Handle.Id;
            Handle = FfiHandle.FromOwnedHandle(track.Handle);
            Room = new WeakReference<Room>(room);
            Participant = new WeakReference<Participant>(participant);
            UpdateInfo(track.Info);
        }

        internal void UpdateInfo(TrackInfo info)
        {
            _info = info;
        }

        internal void UpdateMuted(bool muted)
        {
            _info.Muted = muted;
        }
    }

    public sealed class LocalAudioTrack : Track, ILocalTrack, IAudioTrack
    {
        public IRtcSource source { get; }

        internal LocalAudioTrack(OwnedTrack track, Room room, IRtcSource src) : base(track, room, room?.LocalParticipant) {
            source = src;
        }

        public static LocalAudioTrack CreateAudioTrack(string name, IRtcSource src, Room room)
        {
            var request = new FfiRequest {
                CreateAudioTrack = new CreateAudioTrackRequest {
                    Name = name,
                    SourceHandle = (ulong)src.Handle.DangerousGetHandle()
                }
            };

            var res = FfiClient.Instance.SendRequest(request);
            return new LocalAudioTrack(res.CreateAudioTrack.Track, room, src);
        }
    }
    
    public class NativeAudioSource : IRtcSource
    {
        private bool _muted;
        public override bool Muted => _muted;

        public NativeAudioSource(OwnedAudioSource source)
        {
            Handle = FfiHandle.FromOwnedHandle(source.Handle);
        }

        public override void SetMute(bool muted)
        {
            _muted = muted;
        }
    }

    public sealed class LocalVideoTrack : Track, ILocalTrack, IVideoTrack
    {
        public IRtcSource source { get; }

        internal LocalVideoTrack(OwnedTrack track, Room room, IRtcSource src) : base(track, room, room?.LocalParticipant) {
            source = src;
        }

        public static LocalVideoTrack CreateVideoTrack(string name, IRtcSource src, Room room)
        {
            using var requestWrap = FFIBridge.Instance.NewRequest<CreateVideoTrackRequest>();
            requestWrap.request.Name = name;
            requestWrap.request.SourceHandle = (ulong)src.Handle.DangerousGetHandle();

            using var respWrap = requestWrap.Send();
            FfiResponse res = respWrap;
            return new LocalVideoTrack(res.CreateVideoTrack.Track, room, src);
        }
    }

    public sealed class RemoteAudioTrack : Track, IRemoteTrack, IAudioTrack
    {
        internal RemoteAudioTrack(OwnedTrack track, Room room, RemoteParticipant participant) : base(track, room, participant) { }
    }

    public sealed class RemoteVideoTrack : Track, IRemoteTrack, IVideoTrack
    {
        internal RemoteVideoTrack(OwnedTrack track, Room room, RemoteParticipant participant) : base(track, room, participant) { }
    }
}