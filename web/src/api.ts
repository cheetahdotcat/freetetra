// Typed thin wrappers around the Go backend's existing JSON endpoints.
// Paths are root-relative; Vite's dev proxy or the Go static handler routes them.

export interface SiteConfig {
  server_name: string;
  operator: string;
  host: string;
  server_info_html: string;
  lang: "de" | "en";
}

export interface LastHeardEntry {
  call_id: string;
  source_issi: number;
  callsign?: string;
  name?: string;
  dest_gssi: number;
  started_at: string; // RFC3339
  ended_at?: string;
  duration_ms: number;
  origin?: "subscriber" | "injected" | "peer";
}

export interface LastHeardResponse {
  entries: LastHeardEntry[];
}

export interface HexCell {
  h: string; // H3 cell id
  lat: number;
  lon: number;
  n: number; // total samples
  u: number; // unique ISSIs
  t?: number; // unix seconds of most recent sample
  rp?: string[]; // contributing TMO-sites
}

export interface MapDataResponse {
  hexes: HexCell[];
  resolution: number;
  stats: {
    total_samples: number;
    unique_issis: number;
    devices_24h: number;
    tmo_sites_online: number;
    tmo_sites_total: number;
  };
}

export interface Station {
  station_id: string;
  callsign: string;
  type: "hotspot" | "tmo_site" | "bluestation";
  lat: number;
  lon: number;
  dl_freq: number;
  ul_freq: number;
  power_w: number;
  antenna: string;
  notes: string;
  website: string;
  last_seen: number;
  first_seen: number;
  online: boolean;
  owned_issis?: number[];
  origin?: string;
  deleted?: number;
  // Query-time live joins. Set when this station has an active Brew session
  // and/or a connected telemetry feed bound to it on THIS server (not
  // federated). Empty/undefined for federated stations that aren't connected
  // here, which is correct semantics.
  live_brew_session_id?: string;
  live_brew_subscribers?: number[];
  live_telemetry?: boolean;
  live_telemetry_subscribers?: number[];
}

export interface StationsResponse {
  stations: Station[];
}

export interface PublicStatus {
  server: string;
  tmo_sites: number;
  subscribers: number;
  positions: number;
}

export interface Peer {
  name: string;
  direction: "incoming" | "outgoing";
  issis?: number[];
  // ISSI (decimal string) -> originating server name. May differ from
  // `name` when the peer is just relaying ISSIs it learned over a deeper
  // federation hop. Empty/missing for legacy snapshots — callers should
  // fall back to `name`.
  origins?: Record<string, string>;
  gssis?: Record<string, number[]>;
  count?: number;
}

export interface PeersResponse {
  peers: Peer[];
  count?: number;
}

export interface PositionEntry {
  issi: number;
  lat: number;
  lon: number;
  timestamp: string;
}

export interface PositionsResponse {
  positions: PositionEntry[];
}

export interface DashboardSubscriber {
  session: string;
  remote: string;
  issi: number;
  groups: number[];
}

export interface DashboardActivity {
  seq: number;
  time: string;
  kind: string;
  message: string;
  data?: Record<string, unknown>;
}

export interface DashboardSnapshotResponse {
  server_time: string;
  subscribers: DashboardSubscriber[];
  groups?: { gssi: number; subscribers: number; sessions: number }[];
  activity?: DashboardActivity[];
  last_seq?: number;
}

export interface RadioIDLookupResponse {
  callsign?: string;
  name?: string;
}

async function getJSON<T>(path: string): Promise<T> {
  const r = await fetch(path, { credentials: "same-origin", cache: "no-store" });
  if (!r.ok) throw new Error(path + " -> " + r.status);
  return (await r.json()) as T;
}

async function sendJSON<T>(path: string, method: string, body?: unknown): Promise<T> {
  const r = await fetch(path, {
    method,
    credentials: "same-origin",
    cache: "no-store",
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    throw new Error(text || `${path} -> ${r.status}`);
  }
  return (await r.json()) as T;
}

// StationInput is the subset of Station an operator can write. station_id is
// optional for new stations (server accepts whatever the client supplies; the
// UI generates a UUID for new rows so edits target the same row).
export interface StationInput {
  station_id: string;
  callsign: string;
  type: "hotspot" | "tmo_site" | "bluestation";
  lat: number;
  lon: number;
  dl_freq: number;
  ul_freq: number;
  power_w: number;
  antenna: string;
  notes: string;
  website: string;
  owned_issis?: number[];
}

export const api = {
  siteConfig: () => getJSON<SiteConfig>("/api/site/config"),
  publicStatus: () => getJSON<PublicStatus>("/api/public/status"),
  liveLastHeard: () => getJSON<LastHeardResponse>("/api/live/last-heard"),
  map: (res: number, days: number) => getJSON<MapDataResponse>(`/api/map?res=${res}&days=${days}`),
  stations: () => getJSON<StationsResponse>("/api/stations"),
  pushStation: (input: StationInput) =>
    sendJSON<{ ok: boolean; station: Station }>("/api/stations/push", "POST", input),
  deleteStation: (stationID: string) =>
    sendJSON<{ ok: boolean; station: Station }>(
      `/api/stations/${encodeURIComponent(stationID)}`,
      "DELETE",
    ),
  positions: () => getJSON<PositionsResponse>("/api/positions"),
  peers: () => getJSON<PeersResponse>("/api/peers"),
  telemetryClients: () => getJSON<{ clients: unknown[] }>("/api/telemetry/clients"),
  dashboardSnapshot: (sinceSeq = 0) =>
    getJSON<DashboardSnapshotResponse>(`/api/dashboard/snapshot?since_seq=${sinceSeq}`),
  radioIDLookup: (issi: number) =>
    getJSON<RadioIDLookupResponse>(`/api/radioid/lookup?issi=${issi}`),
};
