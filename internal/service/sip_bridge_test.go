package service

import (
	"errors"
	"net"
	"testing"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"

	"github.com/freetetra/server/internal/brew"
	"github.com/freetetra/server/internal/config"
)

func TestDecodeACELPRTPPayloadPairs18ByteFrames(t *testing.T) {
	frameA := make([]byte, 18)
	frameB := make([]byte, 18)
	frameA[0] = 0x80
	frameB[0] = 0x40

	var pending []byte
	stes := decodeACELPRTPPayload(append(frameA, frameB...), &pending)
	if len(stes) != 1 {
		t.Fatalf("expected 1 STE frame, got %d", len(stes))
	}
	if len(stes[0]) != 36 {
		t.Fatalf("expected STE frame length 36, got %d", len(stes[0]))
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending 18-byte frame")
	}
}

func TestDecodeACELPRTPPayloadCarriesPendingFrame(t *testing.T) {
	frameA := make([]byte, 18)
	frameB := make([]byte, 18)
	frameA[0] = 0x10
	frameB[0] = 0x20

	var pending []byte
	if got := decodeACELPRTPPayload(frameA, &pending); len(got) != 0 {
		t.Fatalf("expected 0 frames on first packet, got %d", len(got))
	}
	if len(pending) != 18 {
		t.Fatalf("expected pending frame length 18, got %d", len(pending))
	}
	if got := decodeACELPRTPPayload(frameB, &pending); len(got) != 1 {
		t.Fatalf("expected 1 frame after second packet, got %d", len(got))
	}
}

func TestParseTargetISSI(t *testing.T) {
	if got := parseTargetISSI("602", 0, 16777186); got != 602 {
		t.Fatalf("expected 602, got %d", got)
	}
	if got := parseTargetISSI("16777186", 777, 16777186); got != 777 {
		t.Fatalf("expected fallback 777 for gateway dialed user, got %d", got)
	}
	if got := parseTargetISSI("", 888, 16777186); got != 888 {
		t.Fatalf("expected fallback 888 for empty user, got %d", got)
	}
}

func TestParseRTPFromSDPPrefersACELPRtpmap(t *testing.T) {
	sdp := []byte(
		"v=0\r\n" +
			"o=- 0 0 IN IP4 192.168.66.10\r\n" +
			"s=test\r\n" +
			"c=IN IP4 192.168.66.10\r\n" +
			"t=0 0\r\n" +
			"m=audio 10028 RTP/AVP 0 8 96\r\n" +
			"a=rtpmap:0 PCMU/8000\r\n" +
			"a=rtpmap:8 PCMA/8000\r\n" +
			"a=rtpmap:96 ACELP/8000\r\n",
	)

	remote, pt, ok := parseRTPFromSDP(sdp, 96)
	if !ok {
		t.Fatalf("expected SDP parse success")
	}
	if pt != 96 {
		t.Fatalf("expected ACELP payload type 96, got %d", pt)
	}
	if remote == nil || !remote.IP.Equal(net.ParseIP("192.168.66.10")) || remote.Port != 10028 {
		t.Fatalf("unexpected RTP remote parsed: %v", remote)
	}
}

func TestNormalizeSIPCallerNumber(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		fallbackISSI uint32
		want         string
	}{
		{
			name:         "numeric label passes through",
			raw:          "1001",
			fallbackISSI: 16777186,
			want:         "1001",
		},
		{
			name:         "non numeric uses fallback",
			raw:          "laptop",
			fallbackISSI: 16777186,
			want:         "16777186",
		},
		{
			name:         "mixed extracts digits",
			raw:          "Desk 601",
			fallbackISSI: 16777186,
			want:         "601",
		},
		{
			name:         "empty uses fallback",
			raw:          "",
			fallbackISSI: 500,
			want:         "500",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeSIPCallerNumber(tc.raw, tc.fallbackISSI)
			if got != tc.want {
				t.Fatalf("normalizeSIPCallerNumber(%q, %d)=%q want %q", tc.raw, tc.fallbackISSI, got, tc.want)
			}
		})
	}
}

func TestFindSessionByDialogRequestFallbacksToCallID(t *testing.T) {
	callID := uuid.New()
	sess := &sipCallSession{
		callID:    callID,
		direction: sipDirectionInbound,
		sipCallID: "abc-123",
	}
	bridge := &SIPBridge{
		cfg:            config.Config{},
		sessionsByCall: map[uuid.UUID]*sipCallSession{callID: sess},
		sessionsByDlg:  map[string]uuid.UUID{},
		sessionsBySIP:  map[string]uuid.UUID{"abc-123": callID},
	}

	req := sip.NewRequest(sip.CANCEL, sip.Uri{})
	req.AppendHeader(sip.NewHeader("Call-ID", "abc-123"))

	gotID, gotSess := bridge.findSessionByDialogRequestLocked(req)
	if gotSess == nil {
		t.Fatalf("expected session from Call-ID fallback")
	}
	if gotID != callID {
		t.Fatalf("expected call ID %s, got %s", callID, gotID)
	}
}

func TestSIPInboundEarlyReleaseStatus(t *testing.T) {
	tests := []struct {
		name      string
		callState uint8
		cause     uint8
		wantCode  int
		wantText  string
	}{
		{
			name:      "setup reject maps to busy",
			callState: brew.CallStateSetupReject,
			cause:     0,
			wantCode:  int(sip.StatusBusyHere),
			wantText:  "Busy Here",
		},
		{
			name:      "call release maps to terminated",
			callState: brew.CallStateCallRelease,
			cause:     1,
			wantCode:  int(sip.StatusRequestTerminated),
			wantText:  "Request Terminated",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotCode, gotText := sipInboundEarlyReleaseStatus(tc.callState, tc.cause)
			if gotCode != tc.wantCode || gotText != tc.wantText {
				t.Fatalf(
					"sipInboundEarlyReleaseStatus(state=%d,cause=%d)=(%d,%q) want (%d,%q)",
					tc.callState,
					tc.cause,
					gotCode,
					gotText,
					tc.wantCode,
					tc.wantText,
				)
			}
		})
	}
}

func TestSIPOutboundRejectCause(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		fallback     uint8
		wantCause    uint8
		wantSIPCode  int
	}{
		{
			name: "busy here maps to user busy cause",
			err: &sipgo.ErrDialogResponse{
				Res: &sip.Response{StatusCode: sip.StatusBusyHere},
			},
			fallback:    0,
			wantCause:   sipCauseUserBusy,
			wantSIPCode: int(sip.StatusBusyHere),
		},
		{
			name: "global busy maps to user busy cause",
			err: &sipgo.ErrDialogResponse{
				Res: &sip.Response{StatusCode: sip.StatusGlobalBusyEverywhere},
			},
			fallback:    0,
			wantCause:   sipCauseUserBusy,
			wantSIPCode: int(sip.StatusGlobalBusyEverywhere),
		},
		{
			name: "other reject uses fallback cause",
			err: &sipgo.ErrDialogResponse{
				Res: &sip.Response{StatusCode: sip.StatusTemporarilyUnavailable},
			},
			fallback:    9,
			wantCause:   9,
			wantSIPCode: int(sip.StatusTemporarilyUnavailable),
		},
		{
			name:        "non dialog error uses fallback",
			err:         errors.New("transport timeout"),
			fallback:    7,
			wantCause:   7,
			wantSIPCode: 0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotCause, gotSIPCode := sipOutboundRejectCause(tc.err, tc.fallback)
			if gotCause != tc.wantCause || gotSIPCode != tc.wantSIPCode {
				t.Fatalf(
					"sipOutboundRejectCause(err=%v,fallback=%d)=(%d,%d) want (%d,%d)",
					tc.err,
					tc.fallback,
					gotCause,
					gotSIPCode,
					tc.wantCause,
					tc.wantSIPCode,
				)
			}
		})
	}
}

func TestOutboundSIPCallerIdentity(t *testing.T) {
	tests := []struct {
		name       string
		sourceISSI uint32
		fallback   string
		want       string
	}{
		{
			name:       "source issi preferred",
			sourceISSI: 605,
			fallback:   "1001",
			want:       "605",
		},
		{
			name:       "fallback digits used when no source",
			sourceISSI: 0,
			fallback:   "Desk 601",
			want:       "601",
		},
		{
			name:       "fallback text kept when no digits",
			sourceISSI: 0,
			fallback:   "radio-a",
			want:       "radio-a",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := outboundSIPCallerIdentity(tc.sourceISSI, tc.fallback)
			if got != tc.want {
				t.Fatalf("outboundSIPCallerIdentity(%d,%q)=%q want %q", tc.sourceISSI, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestAddOutboundCallerIdentityHeaders(t *testing.T) {
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "1001", Host: "pbx.local"})
	addOutboundCallerIdentityHeaders(req, "605", "pbx.local")

	from := req.From()
	if from == nil {
		t.Fatalf("expected From header")
	}
	if from.Address.User != "605" {
		t.Fatalf("expected From user 605, got %q", from.Address.User)
	}
	if from.Address.Host != "pbx.local" {
		t.Fatalf("expected From host pbx.local, got %q", from.Address.Host)
	}
	if !from.Params.Has("tag") {
		t.Fatalf("expected From tag parameter")
	}
	pai := req.GetHeader("P-Asserted-Identity")
	if pai == nil {
		t.Fatalf("expected P-Asserted-Identity header")
	}
	if got := pai.Value(); got != "<sip:605@pbx.local>" {
		t.Fatalf("unexpected P-Asserted-Identity value %q", got)
	}
}
