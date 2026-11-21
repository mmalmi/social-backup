import type { WalletIntent } from './wallet-intent.ts';
import type { WalletRPC, WalletResults, IncomingSummary } from './wallet-actions.ts';
import type { RelayDelivery as Delivery } from 'nostr-pubsub';
import type { XMatch } from './x-identity.ts';
import type { PageReport } from './site-status.ts';
import type { CID } from '@hashtree/core';
import type { DBSchema, IDBPDatabase } from 'idb';
import type { Event, EventTemplate } from 'nostr-tools/pure';

export interface RelayKindFilter {
  mode: 'include' | 'exclude';
  kinds: number[];
}

export interface RelayPoll {
  scope: string;
  at: number;
}

export interface Settings {
  pubkey: string;
  archiveName?: string;
  handle: string;
  capture: boolean;
  social?: Partial<Record<SocialNetwork, { handle: string; capture: boolean }>>;
  skipCrossposts?: boolean;
  /** Show tip buttons on X for people with a verified Nostr profile. Off until enabled. */
  tipButtons?: boolean;
  walletEnabled?: boolean;
  walletAutoReceive?: boolean;
  quickTipSats?: number;
  /** Mark the user's own posts on X, Facebook and Instagram once they are in the backup. On unless turned off. */
  markSaved?: boolean;
  /** The currency tip amounts are shown in. Empty or missing follows the browser's region. */
  tipCurrency?: string;
  relaySync: boolean;
  relayKinds?: RelayKindFilter;
  relays: string[];
  relaysConfigured?: boolean;
  blossomServer?: string;
  blossomServers?: string[];
  mediaMiB?: number;
}

export interface Capture {
  id: string;
  author: string;
  time: number;
  timeMs?: number;
  text: string;
  posters?: string[];
  hasQuoteCard?: boolean;
  /** The X post this reply addresses, from its signed-out original. */
  replyTo?: { author: string; id: string };
  /** The timeline showed Show more. The background reads the full public post instead. */
  truncated?: boolean;
  visibility?: 'public' | 'private' | 'unknown';
}
export type SocialNetwork = 'instagram' | 'facebook';
export interface SocialCapture extends Capture {
  source: SocialNetwork;
  url: string;
  visibility: 'public' | 'private' | 'unknown';
  media?: string[];
  timePrecision?: 'day';
}
export interface Entry {
  event: Event;
  source?: string;
  targets?: string[];
  posters?: string[];
  /** Send this own event to relays even when its kind is not a post, such as a follow list. */
  announce?: boolean;
}
export type CaptureDelivery = Delivery & { network?: SocialNetwork; historySlot?: [string, number, string] };
export type { CaptureDelivery as Delivery };
export interface MediaReference {
  eventId: string;
  url: string;
  mime?: string;
  expected?: string;
  invalidHash: boolean;
  poster: boolean;
  servers: string[];
  fallbacks: string[];
  status: 'pending' | 'saved' | 'unavailable' | 'poster only';
  sha256?: string;
  error?: string;
}
export interface MediaFile {
  sha256: string;
  size: number;
  mime: string;
  cid: { hash: string; key: string };
  blocks: string[];
  copies: string[];
  copyUrls?: Record<string, string>;
}
export interface IndexPointer {
  hash: string;
  localHash?: string;
  server: string;
  event: Event;
  pending: string[];
  acknowledged: string[];
}
export interface BlossomDelivery {
  hash: string;
  server: string;
  check: boolean;
  attempts: number;
  retryAt?: number;
  error?: string;
}
export interface ArchiveSchema extends DBSchema {
  eventHistory: { key: string; value: import('./event-history.ts').HistoryRow; indexes: { pubkey: string } };
  blossomOutbox: { key: [string, string]; value: BlossomDelivery; indexes: { server: string } };
  xDirectory: { key: [string, number]; value: import('./x-directory.ts').DirectoryRow; indexes: { handles: string } };
  blocks: { key: string; value: Uint8Array };
  meta: { key: string | string[]; value: IncomingSummary | WalletIntent | CID | IndexPointer | RelayPoll | number | boolean | string[] | XLink | (Partial<TipTarget> & { at: number }) | { value: number; at: number } };
  events: { key: string; value: Event; indexes: { created_at: number } };
  drafts: { key: string; value: Capture | SocialCapture };
  localCaptures: { key: string; value: Capture | SocialCapture };
  heldEvents: { key: string; value: string };
  xMedia: { key: string; value: {
    hash?: string;
    server: string;
    url?: string;
    retryAt?: number;
    error?: string;
  } };
  xMatches: { key: string; value: Capture };
  sources: { key: string; value: string };
  outbox: { key: [string, string]; value: CaptureDelivery; indexes: { historySlot: [string, number, string] } };
  cursors: {
    key: string[];
    value: { scope?: string; through?: number; started?: number; next?: { until?: number; limit?: number } };
  };
  mediaRefs: { key: [string, string]; value: MediaReference; indexes: { url: string; sha256: string } };
  mediaFiles: { key: string; value: MediaFile; indexes: { rootHash: string } };
}
export type Archive = IDBPDatabase<ArchiveSchema>;
export interface LastRun {
  at: number;
  pubkey?: string;
  reports: Record<string, string>;
  relays?: Record<string, RelayProgress>;
}
export interface RelayProgress {
  phase:
    | 'queued'
    | 'connecting'
    | 'importing'
    | 'broadcasting'
    | 'index'
    | 'finished'
    | 'partial'
    | 'idle'
    | 'retry'
    | 'error';
  imported: number;
  sent: number;
  present: number;
  failed: number;
  ageRejections?: { count: number; oldest: number };
  detail?: string;
}
export interface RelayStatus extends RelayProgress {
  url: string;
  connection: 'connected' | 'connecting' | 'disconnected';
  pending: number;
  retryAt?: number;
  lastSyncedAt?: number;
}
export interface Profile {
  name: string;
  displayName: string;
  picture?: string;
  about?: string;
  lightning?: string;
  hasLightning?: boolean;
}
/** The public profile a new account publishes: a name, and details such as those of its X profile. */
export interface NewProfile {
  name: string;
  about?: string;
  picture?: string;
  banner?: string;
  website?: string;
}
/** An X account's verified Nostr profile, for the toolbar popup. */
export interface NostrCard {
  handle: string;
  pubkey: string;
  profile?: Profile;
  /** Followers in Social Backup's nostr-social-graph snapshot. */
  followers: number;
  following: boolean;
}
/** The X post that proves this profile's X account, published as a NIP-39 claim. */
export interface XLink {
  handle: string;
  proof: string;
}
export interface Status {
  conf: Settings;
  xLink?: XLink;
  /** How many accounts were read from the user's X following list. */
  xFollows: number;
  /** Exact following count last read from the configured X profile, when available. */
  xFollowingTotal?: number;
  profile?: Profile;
  unlocked: boolean;
  events: number;
  drafts: number;
  pending: number;
  running: boolean;
  relays: RelayStatus[];
  x: { media?: number; mediaError?: string; captured: number; signed: number; matched: number; drafts: number };
  social?: Record<SocialNetwork, { saved: number; signed: number }>;
  localX?: number;
  indexHash?: string;
  pointer?: IndexPointer;
  lastRun?: LastRun | null;
  preparingIndex?: boolean;
}
export interface CaptureConfig {
  handle: string;
  capture: boolean;
  pubkey?: string;
  tips?: boolean;
  quickTip?: {account: string; sats: number};
  /** Mark the account's own posts that are in the backup. */
  marks?: boolean;
  tipCurrency?: string;
  /** Whether the configured X account is already linked to this Nostr profile. */
  xLinked?: boolean;
}
/** A Nostr key and the X proof post claimed for it. */
export interface TipTarget {
  pubkey: string;
  proof: string;
}
export type Sign = (template: EventTemplate) => Promise<Event> | Event;
export type ApprovalScope = 'once' | 'kind' | 'signing';
export interface Approval {
  origin: string;
  event?: EventTemplate;
  pubkey: string;
}
export type SettingsForm = Omit<Settings, 'relays' | 'archiveName' | 'mediaMiB' | 'relayKinds'> & {
  relayKinds?: { mode: string; list: string };
  relays: string;
  mediaMiB?: string | number;
};
export interface Payloads extends WalletRPC {
  receivingAddress: { enabled: boolean };
  eventVersions: { kind: 0 | 3; refresh?: boolean };
  editProfile: { baseId: string | null; content: string; tagsFrom?: string; repairFrom?: string; confirmedRemoval?: boolean };
  recoverFollows: { baseId: string; fromId: string; pubkeys: string[] };
  openTipWallet: { invoice?: string; setDefault?: boolean };
  tipBalance: {};
  quickTip: {account: string; handle: string; pubkey: string; post: string; sats: number};
  tipMarks: { targets: {pubkey: string; post?: string}[] };
  resolveImage: { url: string };
  status: {};
  /** A page asks for its settings and says how the backup of the user's own posts on it is going. */
  captureConfig: PageReport;
  pageStatus: PageReport;
  capture: { posts: Capture[] };
  savedPosts: { ids: string[] };
  openSaved: { id: string };
  settings: SettingsForm;
  watch: { pubkey: string };
  unlock: { key: string };
  login: { key: string };
  createProfile: NewProfile;
  detectAccounts: { includeProfile?: boolean };
  suggestAccount: { source: 'x' | SocialNetwork };
  xProfile: { handle: string };
  exportPrivateKey: {};
  listPermissions: {};
  forgetPermissions: { origin: string };
  logout: { pubkey: string };
  deleteAccount: { pubkey: string; confirmed: boolean };
  lock: {};
  sync: {};
  syncBlossom: {};
  rescan: {};
  publishIndex: {};
  broadcast: {};
  registerBridge: { token: string };
  authorizeMediaUpload: { hash: string; server: string };
  nip07: { token: string; method: string; event?: unknown };
  approval: { id: string; decision?: boolean; scope?: ApprovalScope };
  xFollows: { handles: string[]; total?: number };
  tipTargets: { handles: string[] };
  unlinkX: {};
  tipProfile: { handle: string };
  nostrLink: { handle: string };
  nostrCard: { handle: string };
  xProof: { id?: string; url?: string };
  tipInvoice: { handle: string; pubkey: string; lightning: string; sats: number; post?: string };
  tipPost: { handle: string; post?: string; time?: number; text?: string; invoice?: string };
  tipRate: { currency: string };
  findXFollows: { refresh?: boolean };
  followOnNostr: { pubkeys: string[] };
}
export type Action = keyof Payloads;
export type Message<K extends Action = Action> = {
  [T in K]: { type: T; account?: string } & Payloads[T];
}[K];
interface ActionResults extends WalletResults {
  eventVersions: Event[];
  editProfile: Event;
  recoverFollows: Event;
  tipBalance: number;
  quickTip: {state: 'opened' | 'already'};
  tipMarks: number[];
  resolveImage: string;
  exportPrivateKey: string;
  listPermissions: { origin: string; kind: number; allow: boolean; label: string }[];
  status: Status;
  captureConfig: CaptureConfig;
  /** The asked posts that are in the backup, and whether each has a public copy on Nostr. */
  savedPosts: { id: string; onNostr: boolean }[];
  /** Whether the post's public copy was opened. */
  openSaved: boolean;
  authorizeMediaUpload: Event;
  nip07: string | Event;
  approval: Approval | void;
  syncBlossom: number;
  broadcast: number;
  findXFollows: { matches: XMatch[]; unchecked: number; following: string[]; loading?: boolean };
  followOnNostr: number;
  tipTargets: Record<string, TipTarget | null>;
  unlinkX: void;
  tipProfile: { pubkey: string; lightning: string; walletEnabled?: true };
  nostrLink: { pubkey: string };
  nostrCard: NostrCard | null;
  xProof: void;
  detectAccounts: { x?: string; facebook?: string; instagram?: string; instagramProfile?: NewProfile };
  suggestAccount: string | undefined;
  xProfile: Omit<NewProfile, 'name'> & { name: string } | null;
  tipInvoice: {invoice: string; kind: 'zap' | 'lightning'};
  /** The post's Nostr copy by the verified author, and the tips it has received. */
  tipPost: import('./tip-history.ts').TipSummary;
  /** Units of the currency per bitcoin, for showing tips in fiat. */
  tipRate: number | null;
}
export type Result<K extends Action> = K extends keyof ActionResults
  ? ActionResults[K]
  : void;
export type Reply<T> = { ok: true; value: T } | { ok: false; error: string };
export const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error);
