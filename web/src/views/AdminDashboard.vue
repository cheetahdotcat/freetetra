<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from "vue";
import { useI18n } from "vue-i18n";
import { useSiteConfig } from "../composables/useSiteConfig";
import {
  api,
  type DashboardActivity,
  type Peer,
  type PositionEntry,
  type PublicStatus,
  type Station,
  type StationInput,
} from "../api";

const { t } = useI18n();
const { config } = useSiteConfig();

// Stats card values come from /api/public/status.
const publicStatus = ref<PublicStatus | null>(null);

// Lists rendered in the dashboard cards.
const stations = ref<Station[]>([]);
const peers = ref<Peer[]>([]);
const positions = ref<PositionEntry[]>([]);
const activity = ref<DashboardActivity[]>([]);

// ISSI -> resolved callsign from /api/radioid/lookup. Filled in lazily.
const callsigns = reactive<Record<number, string>>({});

let updateTimer: ReturnType<typeof setInterval> | null = null;
let activityTimer: ReturnType<typeof setInterval> | null = null;

interface SubscriberRow {
  issi: number;
  source: "local" | string;
  gssis: number[];
}
const subscriberRows = ref<SubscriberRow[]>([]);

interface TalkgroupRow {
  gssi: number;
  issis: number[];
}
const talkgroupRows = computed<TalkgroupRow[]>(() => {
  const map = new Map<number, number[]>();
  for (const s of subscriberRows.value) {
    for (const g of s.gssis) {
      if (!map.has(g)) map.set(g, []);
      map.get(g)!.push(s.issi);
    }
  }
  return Array.from(map.entries())
    .map(([gssi, issis]) => ({ gssi, issis }))
    .sort((a, b) => a.gssi - b.gssi);
});

const serverName = computed(() => publicStatus.value?.server || config.value?.server_name || "FreeTetra");
const statTMOSites = computed(() => publicStatus.value?.tmo_sites ?? 0);
const statSubscribers = computed(() => publicStatus.value?.subscribers ?? 0);
const statPeers = computed(() => (peers.value.length || 0));
const statPositions = computed(() => publicStatus.value?.positions ?? 0);

function fmtTime(ts: string | undefined | null): string {
  if (!ts) return "-";
  return new Date(ts).toLocaleTimeString("de-DE");
}
function fmtDate(ts: string | undefined | null): string {
  if (!ts) return "-";
  return new Date(ts).toLocaleString("de-DE");
}
function fmtUnixDate(secs: number | undefined): string {
  if (!secs) return "—";
  return new Date(secs * 1000).toLocaleString();
}

function isServiceIssi(i: number): boolean {
  return i >= 900000 && i <= 999999;
}

async function update() {
  try {
    const [pubStatus, , peersResp, posResp, snapshot, stationsResp] = await Promise.all([
      api.publicStatus().catch(() => null),
      api.telemetryClients().catch(() => ({ clients: [] })),
      api.peers().catch(() => ({ peers: [] })),
      api.positions().catch(() => ({ positions: [] })),
      api.dashboardSnapshot(0).catch(() => ({ subscribers: [], activity: [] }) as never),
      api.stations().catch(() => ({ stations: [] })),
    ]);

    if (pubStatus) publicStatus.value = pubStatus;
    peers.value = peersResp.peers ?? [];
    positions.value = posResp.positions ?? [];

    // Stations: online first, then most-recent last_seen.
    stations.value = [...(stationsResp.stations ?? [])].sort((a, b) => {
      if (Number(b.online) !== Number(a.online)) return Number(b.online) - Number(a.online);
      return (b.last_seen || 0) - (a.last_seen || 0);
    });

    // Subscribers: local from dashboard snapshot, remote from peers. Both
    // outgoing and incoming peers contribute — federation propagation is
    // symmetric, and a peer we accept inbound carries the same authoritative
    // ISSI list as one we dial. subsByIssi.has() keeps local first-wins and
    // also dedupes the case where the same peer briefly has both an outgoing
    // and an incoming record during the handshake. Service ISSIs
    // (900000-999999) are filtered.
    //
    // For peer-sourced rows we label by *origin* server (the node where the
    // subscriber is physically attached), not by the direct peer that fed
    // it to us. The hub stores origin from Control.origin per ISSI; falling
    // back to peer.name preserves the previous behaviour on old payloads.
    const subsByIssi = new Map<number, SubscriberRow>();
    for (const s of snapshot.subscribers ?? []) {
      if (isServiceIssi(s.issi)) continue;
      subsByIssi.set(s.issi, { issi: s.issi, source: "local", gssis: s.groups ?? [] });
    }
    for (const p of peersResp.peers ?? []) {
      for (const issi of p.issis ?? []) {
        if (isServiceIssi(issi)) continue;
        if (subsByIssi.has(issi)) continue;
        const gssis: number[] = [];
        for (const [g, members] of Object.entries(p.gssis ?? {})) {
          if ((members ?? []).includes(issi)) gssis.push(parseInt(g, 10));
        }
        const origin = p.origins?.[String(issi)];
        subsByIssi.set(issi, { issi, source: origin || p.name, gssis });
      }
    }
    subscriberRows.value = Array.from(subsByIssi.values()).sort((a, b) => a.issi - b.issi);

    // Resolve missing callsigns lazily.
    for (const s of subscriberRows.value) {
      if (callsigns[s.issi] !== undefined) continue;
      callsigns[s.issi] = "...";
      api
        .radioIDLookup(s.issi)
        .then((d) => {
          if (d.callsign) callsigns[s.issi] = d.callsign;
          else callsigns[s.issi] = "";
        })
        .catch(() => {
          callsigns[s.issi] = "";
        });
    }
  } catch (e) {
    console.error(e);
  }
}

async function updateActivity() {
  try {
    const d = await api.dashboardSnapshot(0);
    activity.value = (d.activity ?? []).slice(-30).reverse();
  } catch {
    /* ignore */
  }
}

onMounted(() => {
  void update();
  void updateActivity();
  updateTimer = setInterval(update, 5000);
  activityTimer = setInterval(updateActivity, 3000);
});
onUnmounted(() => {
  if (updateTimer) clearInterval(updateTimer);
  if (activityTimer) clearInterval(activityTimer);
});

function gssiBadgeClass(g: number) {
  return g >= 91 ? "badge-orange" : "badge-gray";
}
function gssiLabel(g: number) {
  return g >= 91 ? "TG " + g : String(g);
}

// --- station editor --------------------------------------------------------
// `editing` doubles as the modal open flag (null = closed). For a new station
// it's a blank draft with a freshly generated UUID; for an existing row it's
// a copy so cancelling the edit doesn't mutate the table.
interface StationDraft extends StationInput {
  owned_issis_text: string; // raw comma-separated input; parsed on save
}

const editing = ref<StationDraft | null>(null);
const editingIsNew = ref(false);
const saving = ref(false);
const saveError = ref<string>("");

function emptyDraft(): StationDraft {
  return {
    station_id: typeof crypto !== "undefined" && "randomUUID" in crypto ? crypto.randomUUID() : "",
    callsign: "",
    type: "hotspot",
    lat: 0,
    lon: 0,
    dl_freq: 0,
    ul_freq: 0,
    power_w: 0,
    antenna: "",
    notes: "",
    website: "",
    owned_issis_text: "",
  };
}

function openCreate() {
  editing.value = emptyDraft();
  editingIsNew.value = true;
  saveError.value = "";
}

function openEdit(st: Station) {
  editing.value = {
    station_id: st.station_id,
    callsign: st.callsign,
    type: st.type,
    lat: st.lat,
    lon: st.lon,
    dl_freq: st.dl_freq,
    ul_freq: st.ul_freq,
    power_w: st.power_w,
    antenna: st.antenna,
    notes: st.notes,
    website: st.website,
    owned_issis_text: (st.owned_issis ?? []).join(", "),
  };
  editingIsNew.value = false;
  saveError.value = "";
}

function closeEditor() {
  editing.value = null;
  saveError.value = "";
}

async function saveEditing() {
  if (!editing.value) return;
  saving.value = true;
  saveError.value = "";
  try {
    const issis: number[] = [];
    for (const part of editing.value.owned_issis_text.split(",")) {
      const trimmed = part.trim();
      if (!trimmed) continue;
      const n = Number(trimmed);
      if (!Number.isInteger(n) || n <= 0) {
        throw new Error(`invalid ISSI "${trimmed}"`);
      }
      issis.push(n);
    }
    const payload: StationInput = {
      station_id: editing.value.station_id,
      callsign: editing.value.callsign.trim().toUpperCase(),
      type: editing.value.type,
      lat: Number(editing.value.lat),
      lon: Number(editing.value.lon),
      dl_freq: Number(editing.value.dl_freq),
      ul_freq: Number(editing.value.ul_freq),
      power_w: Number(editing.value.power_w),
      antenna: editing.value.antenna,
      notes: editing.value.notes,
      website: editing.value.website,
      owned_issis: issis.length ? issis : undefined,
    };
    await api.pushStation(payload);
    closeEditor();
    await update();
  } catch (e) {
    saveError.value = e instanceof Error ? e.message : String(e);
  } finally {
    saving.value = false;
  }
}

async function deleteStation(st: Station) {
  const ok = window.confirm(
    t("admin.station.confirm_delete", { callsign: st.callsign }),
  );
  if (!ok) return;
  try {
    await api.deleteStation(st.station_id);
    await update();
  } catch (e) {
    window.alert(e instanceof Error ? e.message : String(e));
  }
}
</script>

<template>
  <div class="admin-shell">
    <div class="container">
      <div class="header">
        <h1>Free<span>Tetra</span> Admin</h1>
        <div class="server-name"><span class="live" />{{ serverName }}</div>
      </div>

      <div class="stats">
        <div class="stat">
          <div class="stat-value">{{ statTMOSites }}</div>
          <div class="stat-label">{{ t("admin.tmo_site") }}</div>
        </div>
        <div class="stat">
          <div class="stat-value">{{ statSubscribers }}</div>
          <div class="stat-label">{{ t("admin.subscriber") }}</div>
        </div>
        <div class="stat">
          <div class="stat-value">{{ statPeers }}</div>
          <div class="stat-label">{{ t("admin.peers") }}</div>
        </div>
        <div class="stat">
          <div class="stat-value">{{ statPositions }}</div>
          <div class="stat-label">{{ t("admin.positions") }}</div>
        </div>
      </div>

      <div class="card">
        <h2>
          <span>
            {{ t("admin.tmo_site") }}
            <span class="count">({{ stations.length }})</span>
          </span>
          <button class="btn btn-primary" type="button" @click="openCreate">
            {{ t("admin.station.add") }}
          </button>
        </h2>
        <div v-if="stations.length === 0" class="empty">{{ t("admin.empty.tmo_sites") }}</div>
        <div v-else class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>{{ t("admin.col.callsign") }}</th>
                <th>{{ t("admin.col.status") }}</th>
                <th>{{ t("admin.col.type") }}</th>
                <th>{{ t("admin.col.last_act") }}</th>
                <th class="actions-col">{{ t("admin.col.actions") }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="st in stations" :key="st.station_id">
                <td>
                  <span class="badge badge-green">{{ st.callsign }}</span>
                  <span v-if="st.origin" class="badge badge-blue small mono">{{ st.origin }}</span>
                  <span
                    v-if="st.live_brew_session_id"
                    class="badge badge-orange small mono"
                    :title="t('admin.station.live_brew_tip')"
                  >
                    BREW · {{ (st.live_brew_subscribers ?? []).length }}
                  </span>
                  <span
                    v-if="st.live_telemetry"
                    class="badge badge-blue small mono"
                    :title="t('admin.station.live_telemetry_tip')"
                  >
                    TLM · {{ (st.live_telemetry_subscribers ?? []).length }}
                  </span>
                  <span v-if="st.notes" class="notes"> — {{ st.notes }}</span>
                </td>
                <td>
                  <span :class="['badge', st.online ? 'badge-green' : 'badge-gray']">
                    {{ st.online ? "online" : "offline" }}
                  </span>
                </td>
                <td class="mono dim">{{ st.type || "—" }}</td>
                <td class="dim">{{ fmtUnixDate(st.last_seen) }}</td>
                <td class="actions-col">
                  <button class="btn btn-ghost" type="button" @click="openEdit(st)">
                    {{ t("admin.station.edit") }}
                  </button>
                  <button class="btn btn-danger" type="button" @click="deleteStation(st)">
                    {{ t("admin.station.delete") }}
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>
          {{ t("admin.subscriber") }}
          <span class="count">({{ subscriberRows.length }})</span>
        </h2>
        <div v-if="subscriberRows.length === 0" class="empty">{{ t("admin.empty.subs") }}</div>
        <div v-else class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>{{ t("admin.col.issi") }}</th>
                <th>{{ t("admin.col.callsign") }}</th>
                <th>{{ t("admin.col.source") }}</th>
                <th>{{ t("admin.col.gssis") }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="s in subscriberRows" :key="s.issi">
                <td class="mono">{{ s.issi }}</td>
                <td class="dim">{{ callsigns[s.issi] || "..." }}</td>
                <td>
                  <span
                    :class="['badge', s.source === 'local' ? 'badge-green' : 'badge-blue']"
                  >{{ s.source }}</span>
                </td>
                <td>
                  <template v-if="s.gssis.length">
                    <span
                      v-for="g in [...s.gssis].sort((a, b) => a - b)"
                      :key="g"
                      :class="['badge', gssiBadgeClass(g)]"
                    >{{ gssiLabel(g) }}</span>
                  </template>
                  <span v-else class="dim">—</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>
          {{ t("admin.h.talkgroups") }}
          <span class="count">({{ talkgroupRows.length }})</span>
        </h2>
        <div v-if="talkgroupRows.length === 0" class="empty">{{ t("admin.empty.tgs") }}</div>
        <div v-else class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>{{ t("admin.col.tg") }}</th>
                <th>{{ t("admin.col.count") }}</th>
                <th>{{ t("admin.col.subs") }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in talkgroupRows" :key="row.gssi">
                <td class="mono">
                  <span :class="['tg-label', row.gssi >= 91 ? 'tg-orange' : 'tg-blue']">
                    {{ gssiLabel(row.gssi) }}
                  </span>
                </td>
                <td class="mono">{{ row.issis.length }}</td>
                <td>
                  <span
                    v-for="i in row.issis"
                    :key="i"
                    class="badge badge-gray mono small"
                  >{{ i }}</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>
          {{ t("admin.peers") }}
          <span class="count">({{ peers.length }})</span>
        </h2>
        <div v-if="peers.length === 0" class="empty">{{ t("admin.empty.peers") }}</div>
        <div v-else class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>{{ t("admin.col.server") }}</th>
                <th>{{ t("admin.col.direction") }}</th>
                <th>{{ t("admin.col.remote_subs") }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="p in peers" :key="p.name + '-' + p.direction">
                <td><span class="badge badge-blue">{{ p.name }}</span></td>
                <td><span class="badge badge-gray">{{ p.direction || "-" }}</span></td>
                <td class="mono">{{ p.issis?.length ?? 0 }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>
          {{ t("admin.h.last_positions") }}
          <span class="count">({{ positions.length }})</span>
        </h2>
        <div v-if="positions.length === 0" class="empty">{{ t("admin.empty.positions") }}</div>
        <div v-else class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>{{ t("admin.col.issi") }}</th>
                <th>{{ t("admin.col.lat") }}</th>
                <th>{{ t("admin.col.lon") }}</th>
                <th>{{ t("admin.col.time") }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="p in positions" :key="p.issi + '-' + p.timestamp">
                <td class="mono">{{ p.issi }}</td>
                <td class="mono">{{ p.lat.toFixed(5) }}</td>
                <td class="mono">{{ p.lon.toFixed(5) }}</td>
                <td class="dim">{{ fmtDate(p.timestamp) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>{{ t("admin.h.activity") }} <span class="live" /></h2>
        <div class="activity">
          <div v-if="activity.length === 0" class="empty">{{ t("admin.empty.activity") }}</div>
          <div v-for="a in activity" :key="a.seq" class="activity-item">
            <div class="activity-time">{{ fmtTime(a.time) }}</div>
            <div class="activity-text">
              <span class="mono">{{ a.kind }}</span>
              {{ a.message }}
            </div>
          </div>
        </div>
      </div>
    </div>

    <div v-if="editing" class="modal-backdrop" @click.self="closeEditor">
      <div class="modal">
        <h3>
          {{ editingIsNew ? t("admin.station.add") : t("admin.station.edit") }}
        </h3>
        <form @submit.prevent="saveEditing">
          <div class="form-grid">
            <label>
              <span>{{ t("admin.col.callsign") }} *</span>
              <input
                v-model="editing.callsign"
                type="text"
                required
                maxlength="16"
                autocapitalize="characters"
              />
            </label>
            <label>
              <span>{{ t("admin.col.type") }} *</span>
              <select v-model="editing.type" required>
                <option value="hotspot">hotspot</option>
                <option value="tmo_site">tmo_site</option>
                <option value="bluestation">bluestation</option>
              </select>
            </label>
            <label>
              <span>{{ t("admin.col.lat") }} *</span>
              <input
                v-model.number="editing.lat"
                type="number"
                step="0.000001"
                min="-90"
                max="90"
                required
              />
            </label>
            <label>
              <span>{{ t("admin.col.lon") }} *</span>
              <input
                v-model.number="editing.lon"
                type="number"
                step="0.000001"
                min="-180"
                max="180"
                required
              />
            </label>
            <label>
              <span>{{ t("admin.station.dl_freq") }}</span>
              <input v-model.number="editing.dl_freq" type="number" step="0.0001" min="0" />
            </label>
            <label>
              <span>{{ t("admin.station.ul_freq") }}</span>
              <input v-model.number="editing.ul_freq" type="number" step="0.0001" min="0" />
            </label>
            <label>
              <span>{{ t("admin.station.power_w") }}</span>
              <input v-model.number="editing.power_w" type="number" step="0.1" min="0" />
            </label>
            <label>
              <span>{{ t("admin.station.antenna") }}</span>
              <input v-model="editing.antenna" type="text" maxlength="128" />
            </label>
            <label class="span-2">
              <span>{{ t("admin.station.website") }}</span>
              <input v-model="editing.website" type="url" maxlength="256" />
            </label>
            <label class="span-2">
              <span>{{ t("admin.station.owned_issis") }}</span>
              <input
                v-model="editing.owned_issis_text"
                type="text"
                inputmode="numeric"
                :placeholder="t('admin.station.owned_issis_hint')"
              />
            </label>
            <label class="span-2">
              <span>{{ t("admin.station.notes") }}</span>
              <textarea v-model="editing.notes" rows="2" maxlength="512" />
            </label>
          </div>

          <div v-if="saveError" class="form-error">{{ saveError }}</div>

          <div class="form-actions">
            <button class="btn btn-ghost" type="button" :disabled="saving" @click="closeEditor">
              {{ t("admin.station.cancel") }}
            </button>
            <button class="btn btn-primary" type="submit" :disabled="saving">
              {{ saving ? t("admin.station.saving") : t("admin.station.save") }}
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>

<style scoped>
.admin-shell {
  --bg: #0a0d12;
  --bg-card: #111827;
  --bg-input: #1a2030;
  --border: #1f2937;
  --border-hover: #374151;
  --accent: #6ee7b7;
  --accent-dim: rgba(110, 231, 183, 0.1);
  --blue: #60a5fa;
  --purple: #a78bfa;
  --yellow: #fbbf24;
  --red: #f87171;
  --text: #e5e7eb;
  --text-dim: #9ca3af;
  --text-muted: #6b7280;
  background: var(--bg);
  color: var(--text);
  font-family: "Inter", system-ui, sans-serif;
  font-size: 14px;
  line-height: 1.5;
  min-height: 100vh;
}

.container {
  max-width: 1100px;
  margin: 0 auto;
  padding: 24px;
}

.header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  margin-bottom: 24px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--border);
}
.header h1 {
  font-size: 1.5rem;
  font-weight: 700;
}
.header h1 span {
  color: var(--accent);
}
.header .server-name {
  color: var(--text-dim);
  font-family: "JetBrains Mono", monospace;
  font-size: 0.85rem;
}

.stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
  margin-bottom: 24px;
}
.stat {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 16px;
}
.stat-value {
  font-size: 1.8rem;
  font-weight: 700;
  color: var(--accent);
  font-family: "JetBrains Mono", monospace;
  line-height: 1;
}
.stat-label {
  font-size: 0.7rem;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  margin-top: 6px;
}

.card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 20px;
  margin-bottom: 16px;
}
.card h2 {
  font-size: 0.95rem;
  font-weight: 600;
  margin-bottom: 14px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.card h2 .count {
  font-size: 0.75rem;
  color: var(--text-muted);
  font-weight: 400;
  font-family: "JetBrains Mono", monospace;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.82rem;
}
thead tr {
  border-bottom: 1px solid var(--border);
}
th {
  text-align: left;
  padding: 8px 10px;
  color: var(--text-muted);
  font-weight: 500;
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
td {
  padding: 10px;
  border-bottom: 1px solid var(--border);
}
tr:last-child td {
  border-bottom: 0;
}
td.mono {
  font-family: "JetBrains Mono", monospace;
  font-size: 0.78rem;
}
td.dim,
.dim {
  color: var(--text-muted);
}
.notes {
  color: var(--text-muted);
  font-size: 0.85rem;
}
.empty {
  color: var(--text-muted);
  text-align: center;
  padding: 20px;
  font-size: 0.82rem;
}

.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.7rem;
  font-weight: 500;
  margin-right: 4px;
}
.badge.small {
  font-size: 0.72rem;
}
.badge-green {
  background: rgba(110, 231, 183, 0.15);
  color: var(--accent);
}
.badge-blue {
  background: rgba(96, 165, 250, 0.15);
  color: var(--blue);
}
.badge-gray {
  background: rgba(107, 114, 128, 0.15);
  color: var(--text-muted);
}
.badge-orange {
  background: rgba(217, 119, 6, 0.12);
  color: #d97706;
}

.tg-label {
  font-weight: 600;
}
.tg-blue {
  color: #2563eb;
}
.tg-orange {
  color: #d97706;
}

.activity {
  max-height: 400px;
  overflow-y: auto;
}
.activity-item {
  display: grid;
  grid-template-columns: 80px 1fr;
  gap: 12px;
  padding: 6px 0;
  font-size: 0.82rem;
  border-bottom: 1px solid var(--border);
}
.activity-item:last-child {
  border-bottom: 0;
}
.activity-time {
  color: var(--text-muted);
  font-family: "JetBrains Mono", monospace;
  font-size: 0.75rem;
}
.activity-text {
  color: var(--text-dim);
}
.activity-text .mono {
  font-family: "JetBrains Mono", monospace;
  color: var(--text);
}

.live {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--accent);
  margin-right: 6px;
  animation: pulse 2s infinite;
}
@keyframes pulse {
  0%,
  100% {
    opacity: 1;
  }
  50% {
    opacity: 0.4;
  }
}

.table-wrap {
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}

.btn {
  font: inherit;
  font-size: 0.78rem;
  padding: 6px 12px;
  border-radius: 6px;
  border: 1px solid var(--border);
  background: var(--bg-input);
  color: var(--text);
  cursor: pointer;
  transition: border-color 0.12s, background 0.12s;
}
.btn:hover:not(:disabled) {
  border-color: var(--border-hover);
}
.btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.btn-primary {
  background: var(--accent-dim);
  border-color: var(--accent);
  color: var(--accent);
}
.btn-primary:hover:not(:disabled) {
  background: rgba(110, 231, 183, 0.18);
}
.btn-ghost {
  background: transparent;
}
.btn-danger {
  background: transparent;
  color: var(--red);
  border-color: rgba(248, 113, 113, 0.4);
}
.btn-danger:hover:not(:disabled) {
  background: rgba(248, 113, 113, 0.12);
  border-color: var(--red);
}

.actions-col {
  text-align: right;
  white-space: nowrap;
}
.actions-col .btn {
  margin-left: 6px;
}

.modal-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.6);
  display: flex;
  align-items: flex-start;
  justify-content: center;
  z-index: 50;
  padding: 40px 16px;
  overflow-y: auto;
}
.modal {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 22px;
  max-width: 560px;
  width: 100%;
  box-shadow: 0 20px 50px rgba(0, 0, 0, 0.5);
}
.modal h3 {
  font-size: 1rem;
  font-weight: 600;
  margin-bottom: 16px;
}

.form-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.form-grid label {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 0.75rem;
  color: var(--text-dim);
}
.form-grid label.span-2 {
  grid-column: 1 / -1;
}
.form-grid input,
.form-grid select,
.form-grid textarea {
  font: inherit;
  font-size: 0.85rem;
  padding: 7px 9px;
  border-radius: 6px;
  border: 1px solid var(--border);
  background: var(--bg-input);
  color: var(--text);
}
.form-grid input:focus,
.form-grid select:focus,
.form-grid textarea:focus {
  outline: none;
  border-color: var(--accent);
}
.form-grid textarea {
  resize: vertical;
  font-family: inherit;
}

.form-error {
  margin-top: 12px;
  padding: 8px 10px;
  border-radius: 6px;
  background: rgba(248, 113, 113, 0.1);
  color: var(--red);
  font-size: 0.8rem;
}
.form-actions {
  margin-top: 18px;
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

@media (max-width: 700px) {
  .container {
    padding: 14px;
  }
  .header {
    margin-bottom: 16px;
    padding-bottom: 12px;
    flex-wrap: wrap;
    gap: 6px;
  }
  .header h1 {
    font-size: 1.2rem;
  }
  .header .server-name {
    font-size: 0.75rem;
  }
  .stats {
    grid-template-columns: repeat(2, 1fr);
    gap: 8px;
    margin-bottom: 16px;
  }
  .stat {
    padding: 12px;
  }
  .stat-value {
    font-size: 1.4rem;
  }
  .stat-label {
    font-size: 0.65rem;
  }
  .card {
    padding: 14px;
    margin-bottom: 12px;
  }
  .card h2 {
    font-size: 0.9rem;
  }
  table {
    font-size: 0.75rem;
    min-width: 480px;
  }
  th,
  td {
    padding: 6px 8px;
  }
  td.mono {
    font-size: 0.7rem;
    word-break: break-all;
  }
  .activity-item {
    grid-template-columns: 60px 1fr;
    gap: 8px;
    font-size: 0.75rem;
  }
  .activity-time {
    font-size: 0.68rem;
  }
}
</style>
