package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	BrewMode       string
	HTTPListenAddr string
	UserAgent      string
	ReconnectDelay time.Duration

	// WebRoot points the Go server at a built Vue SPA on disk so it can
	// serve / when the binary is not built with -tags web_embed. Ignored
	// in embed builds, where the SPA is baked into the binary.
	WebRoot string

	Server     ServerConfig
	Client     BrewClientConfig
	MQTT       MQTTConfig
	Netstack   NetstackConfig
	WebRadio   WebRadioConfig
	Zello      ZelloConfig
	Echo       EchoConfig
	Proxy      ProxyConfig
	Soundboard SoundboardConfig
	Link       LinkConfig
	Blechelse  BlechelseConfig
	RadioID    RadioIDConfig
	APRS       APRSConfig
	MOTD       MOTDConfig
	SIP        SIPConfig
	Federation FederationConfig
	Operator   OperatorConfig
}

type OperatorConfig struct {
	Name        string // z.B. "DO1XX"
	Contact     string // e.g. "you@example.com" or Matrix handle
	Description string // freie Beschreibung dieses Servers/Clusters
}

type MOTDConfig struct {
	Enabled bool
	DBPath  string
	Text    string
	ISSI    uint32
}

type SIPConfig struct {
	Enabled            bool
	GatewayISSI        uint32
	BrewISSI           uint32
	BindAddr           string
	ServerAddr         string
	Domain             string
	LocalUser          string
	Username           string
	Password           string
	Transport          string
	RTPPortStart       int
	RTPBindAddr        string
	RTPAdvertiseIP     string
	RTPTimestampStep   uint32
	ACELPPayloadType   uint8
	RegisterEnabled    bool
	RegisterExpires    int
	ReconnectDelay     time.Duration
	ReleaseCause       uint8
	InboundDefaultISSI uint32
}

type RadioIDConfig struct {
	AuthEnabled bool          // Auto-authenticate via RadioID API
	SharedKey   string        // Shared password for all RadioID-verified users
	Offline     bool          // Offline mode (no internet) — use only local users.txt
	SyncOnStart bool          // Download full RadioID DB at startup (for offline usage)
	SyncEvery   time.Duration // Re-sync interval (0 = only at start)
	UsersFile   string        // Path to local users.txt (default: ./users.txt)
}

type APRSConfig struct {
	Enabled  bool
	Callsign string
	Passcode string
	Server   string
}

type FederationConfig struct {
	Enabled       bool
	Name          string   // This server's name (shown to peers)
	Key           string   // Shared key for peer authentication
	Peers         []string // Peer RPC targets (host:port or URL with host:port)
	SelfURL       string   // Our own URL for advertising to peers (gossip)
	RPCListenAddr string   // Local protobuf RPC bind address (e.g. :8092)
}

type ServerConfig struct {
	Path         string
	Realm        string
	Username     string
	Password     string
	TokenTTL     time.Duration
	PingInterval time.Duration
	PongTimeout  time.Duration
	WriteTimeout time.Duration
}

type BrewClientConfig struct {
	BaseURL          string
	Path             string
	Username         string
	Password         string
	ReconnectDelay   time.Duration
	DiscoveryTimeout time.Duration
	WriteTimeout     time.Duration
}

type MQTTConfig struct {
	Enabled   bool
	Broker    string
	ClientID  string
	Username  string
	Password  string
	QoS       byte
	TopicUp   string
	TopicDown string
}

type NetstackConfig struct {
	BridgeEnabled    bool
	RouteFile        string
	CallTopic        string
	TrafficTopic     string
	Encoding         string
	ReleaseCause     uint8
	MinTrafficFrames int
	PendingMaxAge    time.Duration
	RouteCallTimeout time.Duration

	EncoderMode    string
	EncoderURL     string
	EncoderTimeout time.Duration
}

type WebRadioConfig struct {
	Enabled          bool
	StreamURL        string
	Talkgroup        uint32
	SourceISSI       uint32
	BrewISSI         uint32
	FFmpegBin        string
	EncoderBin       string
	EncoderArgs      string
	EncoderFrameSize int
	ReconnectDelay   time.Duration
	ReleaseCause     uint8
}

type ZelloConfig struct {
	Enabled                 bool
	WSURL                   string
	Username                string
	Password                string
	Channels                []string
	ChannelGSSIMap          string
	BrewISSI                uint32
	SourceISSIBase          uint32
	CodecSampleRate         uint16
	CodecFrames             int
	CodecFrameSize          int
	CodecDurationMS         int
	TranscodeTraffic        bool
	TrafficDecoderBin       string
	TrafficFFmpegBin        string
	TrafficFFmpegArgs       string
	TrafficEncoderBin       string
	TrafficEncoderArgs      string
	TrafficEncoderFrameSize int
	ListenOnly              bool
	PlatformType            string
	PlatformName            string
	ReconnectDelay          time.Duration
	PingInterval            time.Duration
	ResponseTimeout         time.Duration
}

type EchoConfig struct {
	Talkgroup     uint32
	BrewISSI      uint32
	VirtualISSI   uint32
	SourceISSI    uint32
	PlaybackDelay time.Duration
	FrameInterval time.Duration
	ReleaseCause  uint8
	MaxFrames     int
}

type ProxyConfig struct {
	BridgeISSI    uint32
	TargetISSI    uint32
	DialTimeout   time.Duration
	IdleTimeout   time.Duration
	MaxConcurrent int
}

// LinkConfig configures the TG-linking module. Each pair mirrors traffic
// bidirectionally between two talkgroups: a call on TG A is re-transmitted
// on TG B (using LinkSourceISSI as the source) and vice versa. The bridge
// filters out its own re-broadcasts via SourceISSI match to avoid loops.
type LinkConfig struct {
	// Pairs is a list of TG pairs; each entry links its two GSSIs
	// bidirectionally. Empty means the bridge does nothing.
	Pairs         []LinkPair
	BrewISSI      uint32 // identity used for the brew-client login
	SourceISSI    uint32 // ISSI stamped on mirrored TX frames; falls back to BrewISSI
	ReleaseCause  uint8
	MirrorHangoff time.Duration // wait this long after origin release before closing the mirror, to catch late frames
}

type LinkPair struct {
	A uint32
	B uint32
}

type SoundboardConfig struct {
	Enabled          bool
	ListenAddr       string
	SoundsDir        string
	ManifestPath     string // optional override; defaults to <SoundsDir>/manifest.json
	BrewISSI         uint32 // identity used for the brew-client login
	SourceISSI       uint32 // ISSI stamped on TX frames; falls back to BrewISSI
	FFmpegBin        string
	EncoderBin       string
	EncoderArgs      string
	EncoderFrameSize int
	FrameInterval    time.Duration
	LeadInPadding    time.Duration // silent padding emitted before the audio
	TailOutPadding   time.Duration // silent padding emitted after the audio
	ReconnectDelay   time.Duration
	ReleaseCause     uint8
}

// BlechelseConfig configures the Blechelse announcement-sample module. The
// module serves a small web UI that lets a user assemble a queue of pre-cut
// DB voice samples (words like "Gleis", station names, hours/minutes, etc.)
// and transmit the concatenated result as one TETRA call.
type BlechelseConfig struct {
	Enabled            bool
	ListenAddr         string
	SamplesDir         string   // root that contains dt/, en/, gong/, ...
	Talkgroups         []uint32 // TGs the UI can transmit to; first is the default
	BrewISSI           uint32   // brew-client login identity
	SourceISSI         uint32   // ISSI stamped on TX frames; falls back to BrewISSI
	FFmpegBin          string
	EncoderBin         string
	EncoderArgs        string
	EncoderFrameSize   int
	FrameInterval      time.Duration
	LeadInPadding      time.Duration // silence before the first sample in a play
	TailOutPadding     time.Duration // silence after the last sample in a play
	InterSamplePadding time.Duration // silence inserted between concatenated samples
	MaxQueueLength     int
	ReleaseCause       uint8
	// Languages selects which top-level subfolders under SamplesDir are indexed
	// (e.g. "dt", "en", "gong"). Empty means index everything present.
	Languages []string
}

func LoadFromEnv() (Config, error) {
	_ = loadDotEnv(".env")

	cfg := Config{
		BrewMode:       env("BREW_MODE", "server"),
		HTTPListenAddr: env("HTTP_LISTEN_ADDR", ":8080"),
		UserAgent:      env("USER_AGENT", "tetra-brew-backend/0.1"),
		ReconnectDelay: envDuration("RECONNECT_DELAY", 2*time.Second),
		WebRoot:        env("WEB_ROOT", ""),
		Server: ServerConfig{
			Path:         normalizePath(env("BREW_SERVER_PATH", "/brew")),
			Realm:        env("BREW_SERVER_REALM", "TETRA Homebrew"),
			Username:     env("BREW_SERVER_USERNAME", ""),
			Password:     env("BREW_SERVER_PASSWORD", ""),
			TokenTTL:     envDuration("BREW_SERVER_TOKEN_TTL", 2*time.Minute),
			PingInterval: envDuration("BREW_SERVER_PING_INTERVAL", 5*time.Second),
			PongTimeout:  envDuration("BREW_SERVER_PONG_TIMEOUT", 45*time.Second),
			WriteTimeout: envDuration("BREW_SERVER_WRITE_TIMEOUT", 5*time.Second),
		},
		Client: BrewClientConfig{
			BaseURL:          env("BREW_CLIENT_BASE_URL", "http://127.0.0.1:8080"),
			Path:             normalizePath(env("BREW_CLIENT_PATH", env("BREW_SERVER_PATH", "/brew"))),
			Username:         env("BREW_CLIENT_USERNAME", env("BREW_SERVER_USERNAME", "")),
			Password:         env("BREW_CLIENT_PASSWORD", env("BREW_SERVER_PASSWORD", "")),
			ReconnectDelay:   envDuration("BREW_CLIENT_RECONNECT_DELAY", 3*time.Second),
			DiscoveryTimeout: envDuration("BREW_CLIENT_DISCOVERY_TIMEOUT", 5*time.Second),
			WriteTimeout:     envDuration("BREW_CLIENT_WRITE_TIMEOUT", 5*time.Second),
		},
		MQTT: MQTTConfig{
			Enabled:   envBool("MQTT_ENABLED", true),
			Broker:    env("MQTT_BROKER", "tcp://127.0.0.1:1883"),
			ClientID:  env("MQTT_CLIENT_ID", "brew-backend-dev"),
			Username:  env("MQTT_USERNAME", ""),
			Password:  env("MQTT_PASSWORD", ""),
			QoS:       byte(envInt("MQTT_QOS", 0)),
			TopicUp:   env("MQTT_ACELP_UP_TOPIC", "brew/calls/+/acelp/up"),
			TopicDown: env("MQTT_ACELP_DOWN_TOPIC_TEMPLATE", "brew/calls/%s/acelp/down"),
		},
		Netstack: NetstackConfig{
			BridgeEnabled:    envBool("NETSTACK_BRIDGE_ENABLED", false),
			RouteFile:        env("NETSTACK_ROUTE_FILE", "./netstack-routes.example.json"),
			CallTopic:        env("NETSTACK_CALL_TOPIC", "live/+/+/call/+/+"),
			TrafficTopic:     env("NETSTACK_TRAFFIC_TOPIC", "live/+/+/traffic/+"),
			Encoding:         strings.ToLower(env("NETSTACK_TRAFFIC_ENCODING", "hex")),
			ReleaseCause:     uint8(envInt("NETSTACK_RELEASE_CAUSE", 0)),
			MinTrafficFrames: envInt("NETSTACK_MIN_TRAFFIC_FRAMES", 8),
			PendingMaxAge:    envDuration("NETSTACK_PENDING_MAX_AGE", 2*time.Second),
			RouteCallTimeout: envDuration("NETSTACK_ROUTE_CALL_TIMEOUT", 30*time.Second),
			EncoderMode:      env("NETSTACK_ENCODER_MODE", "passthrough"),
			EncoderURL:       env("NETSTACK_ENCODER_URL", ""),
			EncoderTimeout: envDuration(
				"NETSTACK_ENCODER_TIMEOUT",
				2*time.Second,
			),
		},
		WebRadio: WebRadioConfig{
			Enabled:          envBool("WEBRADIO_ENABLED", false),
			StreamURL:        env("WEBRADIO_STREAM_URL", ""),
			Talkgroup:        uint32(envInt("WEBRADIO_TALKGROUP", 0)),
			SourceISSI:       uint32(envInt("WEBRADIO_SOURCE_ISSI", 900001)),
			BrewISSI:         uint32(envInt("WEBRADIO_BREW_ISSI", 0)),
			FFmpegBin:        env("WEBRADIO_FFMPEG_BIN", "ffmpeg"),
			EncoderBin:       env("WEBRADIO_ENCODER_BIN", "tetra-acelp-stdio"),
			EncoderArgs:      env("WEBRADIO_ENCODER_ARGS", ""),
			EncoderFrameSize: envInt("WEBRADIO_ENCODER_FRAME_SIZE", 18),
			ReconnectDelay:   envDuration("WEBRADIO_RECONNECT_DELAY", 3*time.Second),
			ReleaseCause:     uint8(envInt("WEBRADIO_RELEASE_CAUSE", 0)),
		},
		Zello: ZelloConfig{
			Enabled:          envBool("ZELLO_ENABLED", false),
			WSURL:            env("ZELLO_WS_URL", ""),
			Username:         env("ZELLO_USERNAME", ""),
			Password:         env("ZELLO_PASSWORD", ""),
			Channels:         envCSV("ZELLO_CHANNELS"),
			ChannelGSSIMap:   env("ZELLO_CHANNEL_GSSI_MAP", ""),
			BrewISSI:         uint32(envInt("ZELLO_BREW_ISSI", 899001)),
			SourceISSIBase:   uint32(envInt("ZELLO_SOURCE_ISSI_BASE", 800000)),
			CodecSampleRate:  uint16(envInt("ZELLO_CODEC_SAMPLE_RATE", 8000)),
			CodecFrames:      envInt("ZELLO_CODEC_FRAMES_PER_PACKET", 1),
			CodecFrameSize:   envInt("ZELLO_CODEC_FRAME_SIZE", 60),
			CodecDurationMS:  envInt("ZELLO_CODEC_PACKET_DURATION_MS", 20),
			TranscodeTraffic: envBool("ZELLO_TRANSCODE_TRAFFIC", true),
			TrafficDecoderBin: env(
				"ZELLO_TRAFFIC_DECODER_BIN",
				"./_other_repos_/tetra-acelp/tetra-acelp-stdio-decoder",
			),
			TrafficFFmpegBin:  env("ZELLO_TRAFFIC_FFMPEG_BIN", "ffmpeg"),
			TrafficFFmpegArgs: env("ZELLO_TRAFFIC_FFMPEG_ARGS", ""),
			TrafficEncoderBin: env(
				"ZELLO_TRAFFIC_ENCODER_BIN",
				"./_other_repos_/tetra-acelp/tetra-acelp-stdio",
			),
			TrafficEncoderArgs:      env("ZELLO_TRAFFIC_ENCODER_ARGS", ""),
			TrafficEncoderFrameSize: envInt("ZELLO_TRAFFIC_ENCODER_FRAME_SIZE", 18),
			ListenOnly:              envBool("ZELLO_LISTEN_ONLY", true),
			PlatformType:            env("ZELLO_PLATFORM_TYPE", "tetra-brew"),
			PlatformName:            env("ZELLO_PLATFORM_NAME", "github.com/freetetra/server"),
			ReconnectDelay:          envDuration("ZELLO_RECONNECT_DELAY", 5*time.Second),
			PingInterval:            envDuration("ZELLO_PING_INTERVAL", 10*time.Second),
			ResponseTimeout:         envDuration("ZELLO_RESPONSE_TIMEOUT", 10*time.Second),
		},
		RadioID: RadioIDConfig{
			AuthEnabled: envBool("RADIOID_AUTH_ENABLED", false),
			SharedKey:   env("RADIOID_SHARED_KEY", ""),
			Offline:     envBool("RADIOID_OFFLINE_MODE", false),
			SyncOnStart: envBool("RADIOID_SYNC_ON_START", false),
			SyncEvery:   envDuration("RADIOID_SYNC_EVERY", 0),
			UsersFile:   env("RADIOID_USERS_FILE", "users.txt"),
		},
		APRS: APRSConfig{
			Enabled:  envBool("APRS_ENABLED", false),
			Callsign: env("APRS_CALLSIGN", ""),
			Passcode: env("APRS_PASSCODE", ""),
			Server:   env("APRS_SERVER", "euro.aprs2.net:14580"),
		},
		MOTD: MOTDConfig{
			Enabled: envBool("MOTD_ENABLED", false),
			DBPath:  env("MOTD_DB_PATH", "motd_seen.json"),
			Text:    env("MOTD_TEXT", ""),
			ISSI:    uint32(envInt("MOTD_ISSI", 0)),
		},
		SIP: SIPConfig{
			Enabled:            envBool("SIP_ENABLED", false),
			GatewayISSI:        uint32(envInt("SIP_GATEWAY_ISSI", 0)),
			BrewISSI:           uint32(envInt("SIP_BREW_ISSI", 0)),
			BindAddr:           env("SIP_BIND_ADDR", "0.0.0.0:5060"),
			ServerAddr:         env("SIP_SERVER_ADDR", ""),
			Domain:             env("SIP_DOMAIN", ""),
			LocalUser:          env("SIP_LOCAL_USER", "brew"),
			Username:           env("SIP_USERNAME", ""),
			Password:           env("SIP_PASSWORD", ""),
			Transport:          env("SIP_TRANSPORT", "udp"),
			RTPPortStart:       envInt("SIP_RTP_PORT_START", 10000),
			RTPBindAddr:        env("SIP_RTP_BIND_ADDR", "0.0.0.0"),
			RTPAdvertiseIP:     env("SIP_RTP_ADVERTISE_IP", ""),
			RTPTimestampStep:   uint32(envInt("SIP_RTP_TIMESTAMP_STEP", 160)),
			ACELPPayloadType:   uint8(envInt("SIP_ACELP_PAYLOAD_TYPE", 96)),
			RegisterEnabled:    envBool("SIP_REGISTER_ENABLED", false),
			RegisterExpires:    envInt("SIP_REGISTER_EXPIRES", 3600),
			ReconnectDelay:     envDuration("SIP_RECONNECT_DELAY", 5*time.Second),
			ReleaseCause:       uint8(envInt("SIP_RELEASE_CAUSE", 0)),
			InboundDefaultISSI: uint32(envInt("SIP_INBOUND_DEFAULT_ISSI", 0)),
		},
		Federation: FederationConfig{
			Enabled:       envBool("FEDERATION_ENABLED", false),
			Name:          env("FEDERATION_NAME", ""),
			Key:           env("FEDERATION_KEY", ""),
			Peers:         envCSV("FEDERATION_PEERS"),
			SelfURL:       env("FEDERATION_SELF_URL", ""),
			RPCListenAddr: env("FEDERATION_RPC_LISTEN_ADDR", ":8092"),
		},
		Operator: OperatorConfig{
			Name:        env("OPERATOR_NAME", ""),
			Contact:     env("OPERATOR_CONTACT", ""),
			Description: env("OPERATOR_DESCRIPTION", ""),
		},
		Echo: EchoConfig{
			Talkgroup:     uint32(envInt("ECHO_TALKGROUP", 10002)),
			BrewISSI:      uint32(envInt("ECHO_BREW_ISSI", 899002)),
			VirtualISSI:   uint32(envInt("ECHO_VIRTUAL_ISSI", 0)),
			SourceISSI:    uint32(envInt("ECHO_SOURCE_ISSI", 0)),
			PlaybackDelay: envDuration("ECHO_PLAYBACK_DELAY", 300*time.Millisecond),
			FrameInterval: envDuration("ECHO_FRAME_INTERVAL", 60*time.Millisecond),
			ReleaseCause:  uint8(envInt("ECHO_RELEASE_CAUSE", 0)),
			MaxFrames:     envInt("ECHO_MAX_FRAMES", 2000),
		},
		Proxy: ProxyConfig{
			BridgeISSI:    uint32(envInt("PROXY_BRIDGE_ISSI", 0)),
			TargetISSI:    uint32(envInt("PROXY_TARGET_ISSI", 0)),
			DialTimeout:   envDuration("PROXY_DIAL_TIMEOUT", 10*time.Second),
			IdleTimeout:   envDuration("PROXY_IDLE_TIMEOUT", 60*time.Second),
			MaxConcurrent: envInt("PROXY_MAX_CONCURRENT", 4),
		},
		Blechelse: BlechelseConfig{
			Enabled:            envBool("BLECHELSE_ENABLED", false),
			ListenAddr:         env("BLECHELSE_LISTEN_ADDR", ":8204"),
			SamplesDir:         env("BLECHELSE_SAMPLES_DIR", "/app/blechelse"),
			Talkgroups:         parseUint32CSV(env("BLECHELSE_TALKGROUPS", "10")),
			BrewISSI:           uint32(envInt("BLECHELSE_BREW_ISSI", 899005)),
			SourceISSI:         uint32(envInt("BLECHELSE_SOURCE_ISSI", 0)),
			FFmpegBin:          env("BLECHELSE_FFMPEG_BIN", "ffmpeg"),
			EncoderBin:         env("BLECHELSE_ENCODER_BIN", "tetra-acelp-stdio"),
			EncoderArgs:        env("BLECHELSE_ENCODER_ARGS", ""),
			EncoderFrameSize:   envInt("BLECHELSE_ENCODER_FRAME_SIZE", 18),
			FrameInterval:      envDuration("BLECHELSE_FRAME_INTERVAL", 60*time.Millisecond),
			LeadInPadding:      envDuration("BLECHELSE_LEAD_IN", 240*time.Millisecond),
			TailOutPadding:     envDuration("BLECHELSE_TAIL_OUT", 240*time.Millisecond),
			InterSamplePadding: envDuration("BLECHELSE_INTER_SAMPLE_PAD", 0),
			MaxQueueLength:     envInt("BLECHELSE_MAX_QUEUE", 50),
			ReleaseCause:       uint8(envInt("BLECHELSE_RELEASE_CAUSE", 0)),
			Languages:          envCSV("BLECHELSE_LANGUAGES"),
		},
		Link: LinkConfig{
			Pairs:         parseLinkPairs(env("LINK_PAIRS", "")),
			BrewISSI:      uint32(envInt("LINK_BREW_ISSI", 899004)),
			SourceISSI:    uint32(envInt("LINK_SOURCE_ISSI", 0)),
			ReleaseCause:  uint8(envInt("LINK_RELEASE_CAUSE", 0)),
			MirrorHangoff: envDuration("LINK_MIRROR_HANGOFF", 200*time.Millisecond),
		},
		Soundboard: SoundboardConfig{
			Enabled:          envBool("SOUNDBOARD_ENABLED", false),
			ListenAddr:       env("SOUNDBOARD_LISTEN_ADDR", ":8203"),
			SoundsDir:        env("SOUNDBOARD_SOUNDS_DIR", "/app/sounds"),
			ManifestPath:     env("SOUNDBOARD_MANIFEST_PATH", ""),
			BrewISSI:         uint32(envInt("SOUNDBOARD_BREW_ISSI", 899003)),
			SourceISSI:       uint32(envInt("SOUNDBOARD_SOURCE_ISSI", 0)),
			FFmpegBin:        env("SOUNDBOARD_FFMPEG_BIN", "ffmpeg"),
			EncoderBin:       env("SOUNDBOARD_ENCODER_BIN", "tetra-acelp-stdio"),
			EncoderArgs:      env("SOUNDBOARD_ENCODER_ARGS", ""),
			EncoderFrameSize: envInt("SOUNDBOARD_ENCODER_FRAME_SIZE", 18),
			FrameInterval:    envDuration("SOUNDBOARD_FRAME_INTERVAL", 60*time.Millisecond),
			LeadInPadding:    envDuration("SOUNDBOARD_LEAD_IN", 240*time.Millisecond),
			TailOutPadding:   envDuration("SOUNDBOARD_TAIL_OUT", 240*time.Millisecond),
			ReconnectDelay:   envDuration("SOUNDBOARD_RECONNECT_DELAY", 3*time.Second),
			ReleaseCause:     uint8(envInt("SOUNDBOARD_RELEASE_CAUSE", 0)),
		},
	}

	switch cfg.BrewMode {
	case "server", "hybrid", "client", "router", "webradio", "zello", "echo", "dmrbridge", "proxy", "soundboard", "link", "blechelse":
	default:
		return cfg, fmt.Errorf("invalid BREW_MODE=%q", cfg.BrewMode)
	}

	return cfg, nil
}

func loadDotEnv(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, `"'`)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, value)
	}
	return nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return parsed
}

// parseUint32CSV parses "10,11,12" into a list of uint32 GSSIs. Whitespace,
// empty entries, and unparseable values are silently skipped.
func parseUint32CSV(raw string) []uint32 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]uint32, 0, len(parts))
	seen := make(map[uint32]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseUint(p, 10, 32)
		if err != nil || v == 0 {
			continue
		}
		if _, ok := seen[uint32(v)]; ok {
			continue
		}
		seen[uint32(v)] = struct{}{}
		out = append(out, uint32(v))
	}
	return out
}

// parseLinkPairs parses "10:10000,11:10001,12:10002" into paired GSSIs.
// Whitespace and empty entries are ignored; malformed entries are skipped.
// A pair (X, X) is dropped — linking a TG to itself would just echo.
func parseLinkPairs(raw string) []LinkPair {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]LinkPair, 0, len(parts))
	for _, entry := range parts {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		ab := strings.SplitN(entry, ":", 2)
		if len(ab) != 2 {
			continue
		}
		a, err := strconv.ParseUint(strings.TrimSpace(ab[0]), 10, 32)
		if err != nil || a == 0 {
			continue
		}
		b, err := strconv.ParseUint(strings.TrimSpace(ab[1]), 10, 32)
		if err != nil || b == 0 {
			continue
		}
		if uint32(a) == uint32(b) {
			continue
		}
		out = append(out, LinkPair{A: uint32(a), B: uint32(b)})
	}
	return out
}

func envCSV(key string) []string {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}

func normalizePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/brew"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	path = strings.TrimSuffix(path, "/")
	if path == "" {
		return "/brew"
	}
	return path
}
