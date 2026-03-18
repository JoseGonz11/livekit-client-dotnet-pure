using LiveKit.Internal;

namespace LiveKit
{
    /// <summary>
    /// Punto de entrada principal para el SDK de LiveKit.
    /// </summary>
    public static class LiveKitSDK
    {
        /// <summary>
        /// Inicializa el motor nativo de WebRTC y Rust.
        /// Debe llamarse antes de intentar conectar a cualquier sala.
        /// </summary>
        public static void Initialize()
        {
            FfiClient.Instance.Initialize();
        }
    }
}