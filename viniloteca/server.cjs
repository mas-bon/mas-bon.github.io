var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// server.ts
var server_exports = {};
__export(server_exports, {
  app: () => app,
  server: () => activeServer,
  startServer: () => startServer
});
module.exports = __toCommonJS(server_exports);
var import_config9 = require("dotenv/config");
var import_express = __toESM(require("express"), 1);
var import_path = __toESM(require("path"), 1);
var import_fs = __toESM(require("fs"), 1);
var import_os = __toESM(require("os"), 1);
var import_http = __toESM(require("http"), 1);
var import_child_process = require("child_process");
var import_stream = require("stream");
var import_genai = require("@google/genai");

// server/metadataManager/providers/base.ts
var BaseProvider = class {
  constructor(rateLimiter, timeoutMs) {
    this.timeoutMs = 8500;
    this.rateLimiter = rateLimiter;
    if (timeoutMs) this.timeoutMs = timeoutMs;
  }
  getStatus() {
    const stats = this.rateLimiter.getStats();
    return {
      name: this.name,
      type: this.type,
      enabled: this.isConfigured(),
      hasApiKey: !this.isFreeOrPublic,
      isFreeOrPublic: this.isFreeOrPublic,
      rateLimit: `Min interval ${stats.minIntervalMs}ms`,
      lastRequestAt: stats.lastRequestTimestamp ? new Date(stats.lastRequestTimestamp).toISOString() : void 0,
      totalRequests: stats.totalRequests,
      totalErrors: stats.totalErrors,
      description: this.description
    };
  }
  /**
   * Performs an HTTP GET request with Rate Limiting, timeout, and in-memory caching.
   */
  async fetchJson(url, options = {}, cacheKey = null, ttlMs) {
    const key = cacheKey ?? url;
    return this.rateLimiter.execute(
      key,
      async () => {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
        try {
          const res = await fetch(url, {
            ...options,
            signal: controller.signal
          });
          if (!res.ok) {
            const err = new Error(
              `[${this.name}] HTTP ${res.status}: ${res.statusText} for ${url}`
            );
            err.status = res.status;
            err.statusCode = res.status;
            err.headers = res.headers;
            throw err;
          }
          const data = await res.json();
          return data;
        } finally {
          clearTimeout(timeout);
        }
      },
      ttlMs
    );
  }
};

// server/metadataManager/rateLimiter.ts
var RateLimiter = class {
  constructor(options) {
    this.lastRequestTimestamp = 0;
    this.queue = [];
    this.isProcessing = false;
    this.cache = /* @__PURE__ */ new Map();
    this.totalRequests = 0;
    this.totalErrors = 0;
    this.name = options.name;
    this.minIntervalMs = Math.max(10, options.minIntervalMs);
    this.maxRetries = options.maxRetries ?? 3;
    this.defaultTtlMs = options.defaultTtlMs ?? 15 * 60 * 1e3;
  }
  getStats() {
    return {
      name: this.name,
      minIntervalMs: this.minIntervalMs,
      totalRequests: this.totalRequests,
      totalErrors: this.totalErrors,
      queueLength: this.queue.length,
      cachedEntries: this.cache.size,
      lastRequestTimestamp: this.lastRequestTimestamp
    };
  }
  /**
   * Acquire a turn in the rate-limited queue.
   */
  async acquireSlot() {
    return new Promise((resolve) => {
      this.queue.push(resolve);
      this.processQueue();
    });
  }
  async processQueue() {
    if (this.isProcessing || this.queue.length === 0) return;
    this.isProcessing = true;
    while (this.queue.length > 0) {
      const now = Date.now();
      const timeSinceLast = now - this.lastRequestTimestamp;
      const waitTime = Math.max(0, this.minIntervalMs - timeSinceLast);
      if (waitTime > 0) {
        await new Promise((r) => setTimeout(r, waitTime));
      }
      this.lastRequestTimestamp = Date.now();
      const next = this.queue.shift();
      if (next) {
        next();
      }
    }
    this.isProcessing = false;
  }
  /**
   * Check cache or execute a rate-limited fetch with automatic retry on 429 / 503.
   */
  async execute(cacheKey, fn, customTtlMs) {
    if (cacheKey) {
      const cached = this.cache.get(cacheKey);
      if (cached && cached.expiresAt > Date.now()) {
        return cached.data;
      }
    }
    let attempt = 0;
    let lastError = null;
    while (attempt <= this.maxRetries) {
      attempt++;
      await this.acquireSlot();
      this.totalRequests++;
      try {
        const result = await fn();
        if (cacheKey && result !== null && result !== void 0) {
          if (this.cache.size > 500) {
            const oldestKey = this.cache.keys().next().value;
            if (oldestKey) this.cache.delete(oldestKey);
          }
          this.cache.set(cacheKey, {
            data: result,
            expiresAt: Date.now() + (customTtlMs ?? this.defaultTtlMs)
          });
        }
        return result;
      } catch (err) {
        lastError = err;
        this.totalErrors++;
        const status = err?.status || err?.statusCode || 0;
        const isRateLimited = status === 429;
        const isTransient = status === 503 || status === 502 || status === 504 || err?.code === "ECONNRESET";
        if ((isRateLimited || isTransient) && attempt <= this.maxRetries) {
          const retryAfterHeader = err?.headers?.get?.("retry-after");
          let delayMs = isRateLimited ? 2500 * attempt : 1500 * attempt;
          if (retryAfterHeader) {
            const parsedSeconds = parseInt(retryAfterHeader, 10);
            if (!isNaN(parsedSeconds) && parsedSeconds > 0) {
              delayMs = parsedSeconds * 1e3 + 200;
            }
          }
          console.warn(
            `[RateLimiter:${this.name}] HTTP ${status} encountered on attempt ${attempt}/${this.maxRetries}. Backing off for ${delayMs}ms...`
          );
          await new Promise((r) => setTimeout(r, delayMs));
          continue;
        }
        throw err;
      }
    }
    throw lastError;
  }
  clearCache() {
    this.cache.clear();
  }
};

// server/metadataManager/config.ts
var import_dotenv = __toESM(require("dotenv"), 1);
import_dotenv.default.config();
var config = {
  musicBrainz: {
    baseUrl: "https://musicbrainz.org/ws/2",
    userAgent: process.env.MUSICBRAINZ_USER_AGENT || "Viniloteca/2.0.0 ( contact@viniloteca.app ; https://github.com/mass/viniloteca )",
    minIntervalMs: 1050
    // MusicBrainz strict 1 req/sec policy
  },
  discogs: {
    baseUrl: "https://api.discogs.com",
    token: process.env.DISCOGS_TOKEN ? process.env.DISCOGS_TOKEN.trim() : void 0,
    userAgent: "Viniloteca/2.0.0",
    minIntervalMs: process.env.DISCOGS_TOKEN ? 250 : 1e3
  },
  theAudioDb: {
    baseUrl: "https://www.theaudiodb.com/api/v1/json",
    apiKey: process.env.THEAUDIODB_API_KEY ? process.env.THEAUDIODB_API_KEY.trim() : "2",
    minIntervalMs: 500
  },
  coverArtArchive: {
    baseUrl: "https://coverartarchive.org",
    minIntervalMs: 200
  },
  spotify: {
    clientId: process.env.SPOTIFY_CLIENT_ID ? process.env.SPOTIFY_CLIENT_ID.trim() : void 0,
    clientSecret: process.env.SPOTIFY_CLIENT_SECRET ? process.env.SPOTIFY_CLIENT_SECRET.trim() : void 0,
    tokenUrl: "https://accounts.spotify.com/api/token",
    baseUrl: "https://api.spotify.com/v1"
  },
  appleMusic: {
    baseUrl: "https://itunes.apple.com",
    minIntervalMs: 250
  },
  lyrics: {
    geniusToken: process.env.GENIUS_ACCESS_TOKEN ? process.env.GENIUS_ACCESS_TOKEN.trim() : void 0,
    musixmatchKey: process.env.MUSIXMATCH_API_KEY ? process.env.MUSIXMATCH_API_KEY.trim() : void 0,
    lrclibBaseUrl: "https://lrclib.net/api"
  },
  acoustid: {
    baseUrl: "https://api.acoustid.org/v2",
    clientKey: process.env.ACOUSTID_CLIENT_KEY ? process.env.ACOUSTID_CLIENT_KEY.trim() : "cXBgfA20Ii"
  }
};

// server/metadataManager/providers/musicbrainz.ts
var MusicBrainzProvider = class extends BaseProvider {
  constructor() {
    super(
      new RateLimiter({
        name: "MusicBrainz",
        minIntervalMs: config.musicBrainz.minIntervalMs,
        // 1050ms
        maxRetries: 3,
        defaultTtlMs: 30 * 60 * 1e3
        // 30 min cache
      })
    );
    this.name = "MusicBrainz";
    this.type = "core";
    this.isFreeOrPublic = true;
    this.description = "Primary source for structured music metadata, relations, and universal MBID identifiers.";
  }
  isConfigured() {
    return true;
  }
  getHeaders() {
    return {
      "User-Agent": config.musicBrainz.userAgent,
      Accept: "application/json"
    };
  }
  /**
   * Search releases by artist, title, barcode or query string.
   */
  async searchReleases(query) {
    const parts = [];
    if (query.barcode) {
      parts.push(`barcode:"${query.barcode.trim()}"`);
    } else {
      if (query.title) {
        const cleanTitle = query.title.replace(/[":*?]/g, " ").trim();
        parts.push(`release:"${cleanTitle}"`);
      }
      if (query.artist) {
        const cleanArtist = query.artist.replace(/[":*?]/g, " ").trim();
        parts.push(`artist:"${cleanArtist}"`);
      }
      if (query.catalogNumber) {
        parts.push(`catno:"${query.catalogNumber.trim()}"`);
      }
      if (!query.title && !query.artist && query.query) {
        const cleanQ = query.query.replace(/[":*?]/g, " ").trim();
        parts.push(cleanQ);
      }
    }
    if (parts.length === 0) return [];
    const luceneQuery = parts.join(" AND ");
    const url = `${config.musicBrainz.baseUrl}/release/?query=${encodeURIComponent(
      luceneQuery
    )}&limit=10&fmt=json`;
    try {
      const data = await this.fetchJson(url, {
        headers: this.getHeaders()
      });
      if (!data || !Array.isArray(data.releases)) return [];
      return data.releases.map((rel) => this.mapReleaseToUnified(rel));
    } catch (err) {
      console.warn("[MusicBrainz] Search failed:", err);
      return [];
    }
  }
  /**
   * Fetch full release details including media, tracklist, and labels by MBID.
   */
  async getReleaseDetails(mbid) {
    if (!mbid) return null;
    const url = `${config.musicBrainz.baseUrl}/release/${mbid}?inc=recordings+artist-credits+labels+media+release-groups&fmt=json`;
    try {
      const data = await this.fetchJson(url, {
        headers: this.getHeaders()
      });
      if (!data || data.id !== mbid) return null;
      const unified = this.mapReleaseToUnified(data);
      const tracks = [];
      if (Array.isArray(data.media)) {
        data.media.forEach((medium) => {
          const mediumFormat = medium.format || "Vinyl";
          const mediumPosition = medium.position || 1;
          if (Array.isArray(medium.tracks)) {
            medium.tracks.forEach((tr) => {
              const trackPos = tr.number || `${mediumPosition}.${tr.position}`;
              const durationMs = tr.length || tr.recording?.length || 0;
              const mins = Math.floor(durationMs / 6e4);
              const secs = Math.floor(durationMs % 6e4 / 1e3);
              const formattedDuration = durationMs > 0 ? `${mins}:${secs < 10 ? "0" : ""}${secs}` : void 0;
              tracks.push({
                position: String(trackPos),
                title: tr.title || tr.recording?.title || "Traccia senza titolo",
                duration: formattedDuration,
                durationMs: durationMs > 0 ? durationMs : void 0,
                mbid: tr.recording?.id,
                artist: tr["artist-credit"]?.[0]?.name || unified.artist
              });
            });
          }
        });
      }
      unified.tracks = tracks;
      return unified;
    } catch (err) {
      console.warn(`[MusicBrainz] Failed to fetch details for ${mbid}:`, err);
      return null;
    }
  }
  mapReleaseToUnified(rel) {
    const artist = rel["artist-credit"]?.[0]?.name || rel["artist-credit"]?.[0]?.artist?.name || "Artista Sconosciuto";
    const title = rel.title || "Senza Titolo";
    const year = rel.date ? rel.date.substring(0, 4) : void 0;
    const country = rel.country || void 0;
    const barcode = rel.barcode || void 0;
    let label;
    let catalogNumber;
    if (Array.isArray(rel["label-info"]) && rel["label-info"].length > 0) {
      label = rel["label-info"][0]?.label?.name;
      catalogNumber = rel["label-info"][0]?.["catalog-number"];
    }
    let format = "LP, Album";
    if (Array.isArray(rel.media) && rel.media.length > 0) {
      format = rel.media[0]?.format || "Vinyl";
    }
    return {
      id: `mb_${rel.id}`,
      source: "musicbrainz",
      mbid: rel.id,
      artist,
      title,
      year,
      releasedDate: rel.date,
      label,
      catalogNumber,
      country,
      barcode,
      format,
      score: rel.score ? parseInt(rel.score, 10) : void 0
    };
  }
};

// server/metadataManager/providers/discogs.ts
var DiscogsProvider = class extends BaseProvider {
  constructor() {
    super(
      new RateLimiter({
        name: "Discogs",
        minIntervalMs: config.discogs.minIntervalMs,
        // 250ms with token or 1000ms
        maxRetries: 3,
        defaultTtlMs: 30 * 60 * 1e3
      })
    );
    this.name = "Discogs";
    this.type = "core";
    this.isFreeOrPublic = false;
    this.description = "Specialized physical release catalog for identifying vinyl pressings, matrix numbers, catalog codes, and rare editions.";
  }
  isConfigured() {
    return true;
  }
  getHeaders() {
    const headers = {
      "User-Agent": config.discogs.userAgent,
      Accept: "application/vnd.discogs.v2.discogs+json"
    };
    if (config.discogs.token) {
      headers["Authorization"] = `Discogs token=${config.discogs.token}`;
    }
    return headers;
  }
  /**
   * Search Discogs database with vinyl prioritization.
   */
  async searchReleases(query) {
    const params = new URLSearchParams();
    params.set("type", "release");
    params.set("per_page", "10");
    if (query.barcode) {
      params.set("barcode", query.barcode.trim());
    } else if (query.catalogNumber) {
      params.set("catno", query.catalogNumber.trim());
    } else {
      if (query.artist) params.set("artist", query.artist.trim());
      if (query.title) params.set("release_title", query.title.trim());
      if (query.format) {
        params.set("format", query.format);
      } else {
        params.set("format", "Vinyl");
      }
    }
    const url = `${config.discogs.baseUrl}/database/search?${params.toString()}`;
    try {
      const data = await this.fetchJson(url, {
        headers: this.getHeaders()
      });
      if (!data || !Array.isArray(data.results)) return [];
      return data.results.map((r) => this.mapResultToUnified(r));
    } catch (err) {
      console.warn("[Discogs Provider] Search failed:", err);
      return [];
    }
  }
  /**
   * Fetch full release details including tracks, vinyl sides, and extra artwork.
   */
  async getReleaseDetails(discogsId) {
    const id = parseInt(String(discogsId), 10);
    if (!id) return null;
    const url = `${config.discogs.baseUrl}/releases/${id}`;
    try {
      const data = await this.fetchJson(url, {
        headers: this.getHeaders()
      });
      if (!data || data.id !== id) return null;
      const tracks = [];
      if (Array.isArray(data.tracklist)) {
        data.tracklist.forEach((t) => {
          if (t.type_ === "track" || !t.type_) {
            tracks.push({
              position: t.position || "",
              title: t.title || "Traccia senza titolo",
              duration: t.duration || void 0
            });
          }
        });
      }
      const covers = [];
      if (Array.isArray(data.images)) {
        data.images.forEach((img) => {
          if (img.uri || img.resource_url) {
            covers.push({
              url: img.uri || img.resource_url,
              source: "discogs",
              type: img.type === "primary" ? "front" : "other",
              width: img.width,
              height: img.height
            });
          }
        });
      }
      const artist = Array.isArray(data.artists) && data.artists.length > 0 ? data.artists.map((a) => a.name.replace(/\s*\(\d+\)$/, "")).join(", ") : "Artista Sconosciuto";
      return {
        id: `discogs_${data.id}`,
        source: "discogs",
        discogsId: data.id,
        artist,
        title: data.title || "Senza Titolo",
        year: data.year ? String(data.year) : void 0,
        releasedDate: data.released || void 0,
        label: Array.isArray(data.labels) && data.labels.length > 0 ? data.labels[0]?.name : void 0,
        catalogNumber: Array.isArray(data.labels) && data.labels.length > 0 ? data.labels[0]?.catno : void 0,
        country: data.country || void 0,
        format: Array.isArray(data.formats) && data.formats.length > 0 ? data.formats.map((f) => [f.name, ...f.descriptions || []].join(", ")).join("; ") : "Vinyl",
        genre: Array.isArray(data.genres) ? data.genres.join(", ") : void 0,
        styles: Array.isArray(data.styles) ? data.styles : void 0,
        coverUrl: covers[0]?.url || data.thumb || void 0,
        covers,
        tracks,
        description: data.notes || void 0
      };
    } catch (err) {
      console.warn(`[Discogs Provider] Failed to fetch release ${id}:`, err);
      return null;
    }
  }
  mapResultToUnified(r) {
    let artist = "Artista Sconosciuto";
    let title = r.title || "Senza Titolo";
    if (r.title && r.title.includes(" - ")) {
      const parts = r.title.split(" - ");
      artist = parts[0].trim().replace(/\s*\(\d+\)$/, "");
      title = parts.slice(1).join(" - ").trim();
    }
    return {
      id: `discogs_${r.id}`,
      source: "discogs",
      discogsId: r.id,
      artist,
      title,
      year: r.year ? String(r.year) : void 0,
      country: r.country || void 0,
      catalogNumber: r.catno || void 0,
      barcode: Array.isArray(r.barcode) ? r.barcode[0] : void 0,
      format: Array.isArray(r.format) ? r.format.join(", ") : "Vinyl",
      genre: Array.isArray(r.genre) ? r.genre.join(", ") : void 0,
      coverUrl: r.cover_image || r.thumb || void 0
    };
  }
};

// server/metadataManager/providers/theaudiodb.ts
var TheAudioDbProvider = class extends BaseProvider {
  constructor() {
    super(
      new RateLimiter({
        name: "TheAudioDB",
        minIntervalMs: config.theAudioDb.minIntervalMs,
        // 500ms
        maxRetries: 2,
        defaultTtlMs: 60 * 60 * 1e3
        // 1 hour cache
      })
    );
    this.name = "TheAudioDB";
    this.type = "core";
    this.isFreeOrPublic = true;
    this.description = "Community music database specializing in multi-language artist biographies (Italian/English), album reviews, and high-res promotional fanart.";
  }
  isConfigured() {
    return Boolean(config.theAudioDb.apiKey);
  }
  /**
   * Get artist biography, banners, fanart, logo, formed year in Italian and English.
   */
  async getArtistBio(artistName) {
    if (!artistName || !this.isConfigured()) return null;
    const cleanName = artistName.replace(/\s*\(\d+\)\s*$/g, "").trim();
    const url = `${config.theAudioDb.baseUrl}/${config.theAudioDb.apiKey}/search.php?s=${encodeURIComponent(
      cleanName
    )}`;
    try {
      const data = await this.fetchJson(url, {}, `tadb_artist_${cleanName.toLowerCase()}`);
      if (!data || !Array.isArray(data.artists) || data.artists.length === 0) {
        return null;
      }
      const a = data.artists[0];
      const bioIT = a.strBiographyIT ? a.strBiographyIT.trim() : void 0;
      const bioEN = a.strBiographyEN ? a.strBiographyEN.trim() : void 0;
      return {
        artistName: a.strArtist || cleanName,
        biographyIT: bioIT,
        biographyEN: bioEN,
        biography: bioIT || bioEN || "Nessuna biografia disponibile.",
        formedYear: a.intFormedYear || void 0,
        bornYear: a.intBornYear || void 0,
        country: a.strCountry || void 0,
        genre: a.strGenre || void 0,
        style: a.strStyle || void 0,
        mood: a.strMood || void 0,
        bannerUrl: a.strArtistBanner || void 0,
        fanartUrl: a.strArtistFanart || void 0,
        logoUrl: a.strArtistLogo || void 0,
        thumbnailUrl: a.strArtistThumb || void 0,
        source: "theaudiodb"
      };
    } catch (err) {
      console.warn(`[TheAudioDB] Failed to fetch artist bio for ${cleanName}:`, err);
      return null;
    }
  }
  /**
   * Search album by artist and title to retrieve description, genre, release year and album artwork.
   */
  async searchAlbum(query) {
    if (!query.artist || !query.title || !this.isConfigured()) return null;
    const cleanArtist = query.artist.replace(/\s*\(\d+\)\s*$/g, "").trim();
    const cleanAlbum = query.title.replace(/[":*?]/g, " ").trim();
    const url = `${config.theAudioDb.baseUrl}/${config.theAudioDb.apiKey}/searchalbum.php?s=${encodeURIComponent(
      cleanArtist
    )}&a=${encodeURIComponent(cleanAlbum)}`;
    try {
      const data = await this.fetchJson(
        url,
        {},
        `tadb_album_${cleanArtist.toLowerCase()}__${cleanAlbum.toLowerCase()}`
      );
      if (!data || !Array.isArray(data.album) || data.album.length === 0) {
        return null;
      }
      const alb = data.album[0];
      const descIT = alb.strDescriptionIT ? alb.strDescriptionIT.trim() : void 0;
      const descEN = alb.strDescriptionEN ? alb.strDescriptionEN.trim() : void 0;
      const covers = [];
      if (alb.strAlbumThumb) {
        covers.push({
          url: alb.strAlbumThumb,
          source: "theaudiodb",
          type: "front",
          isHighRes: true
        });
      }
      if (alb.strAlbumCDart) {
        covers.push({
          url: alb.strAlbumCDart,
          source: "theaudiodb",
          type: "medium"
        });
      }
      return {
        id: `tadb_${alb.idAlbum}`,
        source: "theaudiodb",
        theAudioDbId: alb.idAlbum,
        artist: alb.strArtist || cleanArtist,
        title: alb.strAlbum || cleanAlbum,
        year: alb.intYearReleased || void 0,
        genre: alb.strGenre || void 0,
        styles: alb.strStyle ? [alb.strStyle] : void 0,
        coverUrl: alb.strAlbumThumb || void 0,
        covers,
        description: descIT || descEN || void 0
      };
    } catch (err) {
      console.warn("[TheAudioDB] Album search failed:", err);
      return null;
    }
  }
};

// server/metadataManager/providers/coverArtArchive.ts
var CoverArtArchiveProvider = class extends BaseProvider {
  constructor() {
    super(
      new RateLimiter({
        name: "CoverArtArchive",
        minIntervalMs: config.coverArtArchive.minIntervalMs,
        // 200ms
        maxRetries: 2,
        defaultTtlMs: 60 * 60 * 1e3
        // 1 hour cache
      })
    );
    this.name = "CoverArtArchive";
    this.type = "visual";
    this.isFreeOrPublic = true;
    this.description = "Official high-resolution artwork repository connected directly to MusicBrainz releases via MBID.";
  }
  isConfigured() {
    return true;
  }
  /**
   * Fetch all images (front, back, booklet, vinyl media) associated with a MusicBrainz MBID.
   */
  async getCoversByMbid(mbid) {
    if (!mbid) return [];
    const url = `${config.coverArtArchive.baseUrl}/release/${mbid}`;
    try {
      const data = await this.fetchJson(
        url,
        {
          headers: {
            Accept: "application/json",
            "User-Agent": config.musicBrainz.userAgent
          }
        },
        `caa_${mbid}`
      );
      if (!data || !Array.isArray(data.images)) return [];
      const results = [];
      for (const img of data.images) {
        let type = "other";
        if (img.front) type = "front";
        else if (img.back) type = "back";
        else if (Array.isArray(img.types)) {
          if (img.types.includes("Booklet")) type = "booklet";
          else if (img.types.includes("Medium")) type = "medium";
          else if (img.types.includes("Tray")) type = "tray";
          else if (img.types.includes("Liner")) type = "liner";
        }
        const bestUrl = img.thumbnails?.["1200"] || img.thumbnails?.large || img.thumbnails?.["500"] || img.image;
        if (bestUrl) {
          results.push({
            url: bestUrl,
            source: "coverartarchive",
            type,
            isHighRes: Boolean(img.thumbnails?.["1200"] || img.image)
          });
        }
      }
      return results;
    } catch (err) {
      if (err?.status !== 404) {
        console.warn(`[CoverArtArchive] Failed to fetch covers for ${mbid}:`, err?.message || err);
      }
      return [];
    }
  }
  /**
   * Get direct front image URL if available.
   */
  getDirectFrontUrl(mbid) {
    return `${config.coverArtArchive.baseUrl}/release/${mbid}/front-500`;
  }
};

// server/metadataManager/providers/spotify.ts
var SpotifyProvider = class extends BaseProvider {
  constructor() {
    super(
      new RateLimiter({
        name: "Spotify",
        minIntervalMs: 200,
        maxRetries: 2,
        defaultTtlMs: 60 * 60 * 1e3
      })
    );
    this.name = "Spotify";
    this.type = "visual";
    this.isFreeOrPublic = false;
    this.description = "Commercial streaming database providing pristine 640x640 album artwork and catalog IDs via Client Credentials flow.";
    this.accessToken = null;
    this.tokenExpiresAt = 0;
  }
  isConfigured() {
    return Boolean(config.spotify.clientId && config.spotify.clientSecret);
  }
  /**
   * Acquire or refresh client credentials bearer token.
   */
  async getValidToken() {
    if (!this.isConfigured()) return null;
    if (this.accessToken && Date.now() < this.tokenExpiresAt - 6e4) {
      return this.accessToken;
    }
    try {
      const creds = Buffer.from(
        `${config.spotify.clientId}:${config.spotify.clientSecret}`
      ).toString("base64");
      const res = await fetch(config.spotify.tokenUrl, {
        method: "POST",
        headers: {
          Authorization: `Basic ${creds}`,
          "Content-Type": "application/x-www-form-urlencoded"
        },
        body: "grant_type=client_credentials"
      });
      if (!res.ok) {
        console.warn(`[Spotify] Token request failed with HTTP ${res.status}`);
        return null;
      }
      const data = await res.json();
      if (data && data.access_token) {
        this.accessToken = data.access_token;
        this.tokenExpiresAt = Date.now() + (data.expires_in || 3600) * 1e3;
        return this.accessToken;
      }
    } catch (err) {
      console.warn("[Spotify] Error obtaining access token:", err);
    }
    return null;
  }
  /**
   * Search albums and retrieve 640x640 commercial artwork.
   */
  async searchAlbumCovers(query) {
    const token = await this.getValidToken();
    if (!token) return [];
    let q = "";
    if (query.title && query.artist) {
      q = `album:${query.title} artist:${query.artist}`;
    } else if (query.title) {
      q = query.title;
    } else if (query.query) {
      q = query.query;
    }
    if (!q) return [];
    const url = `${config.spotify.baseUrl}/search?q=${encodeURIComponent(
      q
    )}&type=album&limit=5`;
    try {
      const data = await this.fetchJson(
        url,
        {
          headers: {
            Authorization: `Bearer ${token}`
          }
        },
        `spotify_${q.toLowerCase()}`
      );
      if (!data || !data.albums || !Array.isArray(data.albums.items)) return [];
      const covers = [];
      for (const item of data.albums.items) {
        if (Array.isArray(item.images)) {
          for (const img of item.images) {
            if (img.url) {
              covers.push({
                url: img.url,
                source: "spotify",
                type: "front",
                width: img.width || 640,
                height: img.height || 640,
                isHighRes: (img.width || 0) >= 600
              });
            }
          }
        }
      }
      return covers;
    } catch (err) {
      console.warn("[Spotify] Album search failed:", err);
      return [];
    }
  }
};

// server/metadataManager/providers/appleMusic.ts
var AppleMusicProvider = class extends BaseProvider {
  constructor() {
    super(
      new RateLimiter({
        name: "AppleMusic",
        minIntervalMs: config.appleMusic.minIntervalMs,
        // 250ms
        maxRetries: 2,
        defaultTtlMs: 60 * 60 * 1e3
      })
    );
    this.name = "AppleMusic";
    this.type = "visual";
    this.isFreeOrPublic = true;
    this.description = "Commercial artwork provider offering up to 1400x1400 high-res album covers and 30-second audio stream previews without requiring an API key.";
  }
  isConfigured() {
    return true;
  }
  /**
   * Search albums and retrieve high-res artwork (up to 1400x1400).
   */
  async searchAlbumCovers(query) {
    const term = [query.artist, query.title].filter(Boolean).join(" ") || query.query || "";
    if (!term.trim()) return [];
    const url = `${config.appleMusic.baseUrl}/search?term=${encodeURIComponent(
      term
    )}&entity=album&limit=6`;
    try {
      const data = await this.fetchJson(url, {}, `itunes_${term.toLowerCase()}`);
      if (!data || !Array.isArray(data.results)) return [];
      const covers = [];
      for (const item of data.results) {
        if (item.artworkUrl100) {
          const hiResUrl = item.artworkUrl100.replace(/\/\d+x\d+bb\./, "/1400x1400bb.").replace(/\/\d+x\d+\./, "/1400x1400.");
          covers.push({
            url: hiResUrl,
            source: "apple",
            type: "front",
            width: 1400,
            height: 1400,
            isHighRes: true
          });
        }
      }
      return covers;
    } catch (err) {
      console.warn("[AppleMusic] Search failed:", err);
      return [];
    }
  }
  /**
   * Resolve 30-second audio preview for turntable player.
   */
  async getAudioPreview(artist, title) {
    const term = `${artist} ${title}`.trim();
    if (!term) return null;
    const url = `${config.appleMusic.baseUrl}/search?term=${encodeURIComponent(
      term
    )}&entity=song&limit=3`;
    try {
      const data = await this.fetchJson(url, {}, `itunes_song_${term.toLowerCase()}`);
      if (!data || !Array.isArray(data.results) || data.results.length === 0) {
        return null;
      }
      const match = data.results[0];
      return {
        previewUrl: match.previewUrl,
        trackTitle: match.trackName
      };
    } catch (err) {
      return null;
    }
  }
};

// server/metadataManager/providers/lyrics.ts
var LyricsProvider = class _LyricsProvider extends BaseProvider {
  constructor() {
    super(
      new RateLimiter({
        name: "Lyrics",
        minIntervalMs: 250,
        maxRetries: 2,
        defaultTtlMs: 24 * 60 * 60 * 1e3
        // cache lyrics for 24h
      })
    );
    this.name = "Lyrics";
    this.type = "lyrics";
    this.isFreeOrPublic = true;
    this.description = "Multi-source lyrics provider integrating LRCLIB (synchronized timestamps), Genius API, and Musixmatch.";
  }
  isConfigured() {
    return true;
  }
  /**
   * Parse standard LRC format string into timestamped lines.
   */
  static parseLrc(lrcText) {
    if (!lrcText) return [];
    const lines = lrcText.split(/\r?\n/);
    const parsed = [];
    const timeRegex = /\[(\d{2}):(\d{2})\.?(\d{2,3})?\]/g;
    for (const line of lines) {
      const match = timeRegex.exec(line);
      if (match) {
        const mins = parseInt(match[1], 10);
        const secs = parseInt(match[2], 10);
        const millis = match[3] ? parseInt(match[3].padEnd(3, "0").slice(0, 3), 10) : 0;
        const totalSeconds = mins * 60 + secs + millis / 1e3;
        const text = line.replace(/\[\d{2}:\d{2}\.?\d{2,3}?\]/g, "").trim();
        if (text) {
          parsed.push({
            seconds: totalSeconds,
            text,
            timeTag: `[${match[1]}:${match[2]}]`
          });
        }
      }
      timeRegex.lastIndex = 0;
    }
    return parsed.sort((a, b) => a.seconds - b.seconds);
  }
  /**
   * Retrieve lyrics with fallback: LRCLIB (synced) -> Genius -> Musixmatch.
   */
  async getLyrics(artist, trackTitle, albumTitle) {
    if (!artist || !trackTitle) return null;
    const cleanArtist = artist.replace(/\s*\(\d+\)$/, "").trim();
    const cleanTitle = trackTitle.replace(/\s*-\s*Remastered.*$/i, "").replace(/\s*\(Remastered.*\)$/i, "").replace(/\s*\[.*\]/g, "").trim();
    try {
      const lrclibRes = await this.fetchFromLrclib(cleanArtist, cleanTitle, albumTitle);
      if (lrclibRes) return lrclibRes;
    } catch (e) {
    }
    if (config.lyrics.geniusToken) {
      try {
        const geniusRes = await this.fetchFromGenius(cleanArtist, cleanTitle);
        if (geniusRes) return geniusRes;
      } catch (e) {
      }
    }
    if (config.lyrics.musixmatchKey) {
      try {
        const musixmatchRes = await this.fetchFromMusixmatch(cleanArtist, cleanTitle);
        if (musixmatchRes) return musixmatchRes;
      } catch (e) {
      }
    }
    return null;
  }
  /**
   * Fetch from LRCLIB
   */
  async fetchFromLrclib(artist, title, album) {
    const params = new URLSearchParams({
      artist_name: artist,
      track_name: title
    });
    if (album) params.set("album_name", album);
    const url = `${config.lyrics.lrclibBaseUrl}/get?${params.toString()}`;
    try {
      const data = await this.fetchJson(
        url,
        {
          headers: {
            "User-Agent": config.musicBrainz.userAgent
          }
        },
        `lrclib_${artist.toLowerCase()}__${title.toLowerCase()}`
      );
      if (!data || !data.plainLyrics && !data.syncedLyrics) {
        return null;
      }
      const syncedLines = data.syncedLyrics ? _LyricsProvider.parseLrc(data.syncedLyrics) : void 0;
      return {
        trackTitle: data.trackName || title,
        artistName: data.artistName || artist,
        albumTitle: data.albumName || album,
        durationMs: data.duration ? Math.round(data.duration * 1e3) : void 0,
        plainLyrics: data.plainLyrics || void 0,
        syncedLyrics: syncedLines && syncedLines.length > 0 ? syncedLines : void 0,
        rawLrc: data.syncedLyrics || void 0,
        source: "lrclib"
      };
    } catch (err) {
      return null;
    }
  }
  /**
   * Fetch track info from Genius API
   */
  async fetchFromGenius(artist, title) {
    if (!config.lyrics.geniusToken) return null;
    const q = `${artist} ${title}`;
    const url = `https://api.genius.com/search?q=${encodeURIComponent(q)}`;
    try {
      const data = await this.fetchJson(
        url,
        {
          headers: {
            Authorization: `Bearer ${config.lyrics.geniusToken}`
          }
        },
        `genius_${q.toLowerCase()}`
      );
      const hits = data?.response?.hits;
      if (!Array.isArray(hits) || hits.length === 0) return null;
      const hit = hits[0].result;
      return {
        trackTitle: hit.title || title,
        artistName: hit.primary_artist?.name || artist,
        url: hit.url,
        source: "genius",
        plainLyrics: hit.lyrics_state === "complete" ? `Testo visualizzabile su Genius: ${hit.url}` : void 0
      };
    } catch (err) {
      return null;
    }
  }
  /**
   * Fetch from Musixmatch API
   */
  async fetchFromMusixmatch(artist, title) {
    if (!config.lyrics.musixmatchKey) return null;
    const url = `https://api.musixmatch.com/ws/1.1/matcher.lyrics.get?q_track=${encodeURIComponent(
      title
    )}&q_artist=${encodeURIComponent(artist)}&apikey=${config.lyrics.musixmatchKey}`;
    try {
      const data = await this.fetchJson(
        url,
        {},
        `musixmatch_${artist.toLowerCase()}__${title.toLowerCase()}`
      );
      const body = data?.message?.body?.lyrics;
      if (!body || !body.lyrics_body) return null;
      return {
        trackTitle: title,
        artistName: artist,
        plainLyrics: body.lyrics_body,
        copyright: body.lyrics_copyright,
        url: body.backlink_url,
        source: "musixmatch"
      };
    } catch (err) {
      return null;
    }
  }
};

// server/metadataManager/providers/acoustid.ts
var AcoustIdProvider = class extends BaseProvider {
  constructor() {
    super(
      new RateLimiter({
        name: "AcoustID",
        minIntervalMs: 350,
        maxRetries: 2,
        defaultTtlMs: 2 * 60 * 60 * 1e3
      })
    );
    this.name = "AcoustID";
    this.type = "fingerprint";
    this.isFreeOrPublic = false;
    this.description = "Audio acoustic fingerprinting service powered by Chromaprint. Identifies untagged audio and links to MusicBrainz recording MBIDs.";
  }
  isConfigured() {
    return Boolean(config.acoustid.clientKey && config.acoustid.clientKey.trim().length > 0);
  }
  /**
   * Lookup recording by audio duration (seconds) and Chromaprint raw fingerprint string.
   */
  async lookupFingerprint(durationSeconds, fingerprint) {
    if (!durationSeconds || !fingerprint) {
      throw new Error("Parametri incompleti: specifica sia la durata in secondi che l'impronta Chromaprint.");
    }
    if (!this.isConfigured()) {
      throw new Error(
        "Chiave API AcoustID non configurata. Inserisci la tua ACOUSTID_CLIENT_KEY nel file .env (puoi registrarne una gratuita su https://acoustid.org/api-key)."
      );
    }
    const params = new URLSearchParams({
      client: config.acoustid.clientKey,
      meta: "recordings+releasegroups+releases+tracks",
      duration: String(Math.round(durationSeconds)),
      fingerprint: fingerprint.trim()
    });
    const url = `${config.acoustid.baseUrl}/lookup`;
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "User-Agent": config.musicBrainz.userAgent
        },
        body: params.toString()
      });
      let data = null;
      try {
        data = await res.json();
      } catch {
      }
      if (!res.ok || data?.status === "error") {
        const errorMsg = data?.error?.message;
        const errorCode = data?.error?.code;
        if (errorMsg === "invalid API key" || errorCode === 4) {
          throw new Error(
            `API Key non valida (invalid API key). La chiave "${config.acoustid.clientKey}" impostata in ACOUSTID_CLIENT_KEY non \xE8 valida. Attenzione: AcoustID richiede una "Application API Key" (Client Key) da https://acoustid.org/applications/register e non la User Key personale. Registra la tua app su quella pagina e copia l'Application API Key.`
          );
        }
        if (errorMsg) {
          throw new Error(`Errore AcoustID: ${errorMsg} (codice ${errorCode || res.status})`);
        }
        throw new Error(`Errore di rete AcoustID: HTTP ${res.status} ${res.statusText}`);
      }
      if (!data || data.status !== "ok" || !Array.isArray(data.results)) {
        return [];
      }
      const matches = [];
      for (const item of data.results) {
        const score = typeof item.score === "number" ? Math.round(item.score * 100) : 100;
        if (Array.isArray(item.recordings)) {
          for (const rec of item.recordings) {
            const artist = Array.isArray(rec.artists) && rec.artists.length > 0 ? rec.artists.map((a) => a.name).join(", ") : "Artista Sconosciuto";
            let album;
            let releaseMbid;
            if (Array.isArray(rec.releasegroups) && rec.releasegroups.length > 0) {
              const rg = rec.releasegroups[0];
              album = rg.title;
              if (Array.isArray(rg.releases) && rg.releases.length > 0) {
                releaseMbid = rg.releases[0].id;
              }
            }
            matches.push({
              id: item.id,
              score,
              recordingMbid: rec.id,
              releaseMbid,
              artist,
              title: rec.title || "Senza Titolo",
              album,
              duration: rec.duration,
              source: "acoustid"
            });
          }
        }
      }
      return matches.sort((a, b) => b.score - a.score);
    } catch (err) {
      console.warn("[AcoustID] Fingerprint lookup error:", err);
      if (err instanceof Error) {
        throw err;
      }
      throw new Error(`Errore durante l'interrogazione AcoustID: ${String(err)}`);
    }
  }
};

// server/metadataManager/index.ts
var MetadataManager = class {
  constructor() {
    this.musicBrainz = new MusicBrainzProvider();
    this.discogs = new DiscogsProvider();
    this.theAudioDb = new TheAudioDbProvider();
    this.coverArtArchive = new CoverArtArchiveProvider();
    this.spotify = new SpotifyProvider();
    this.appleMusic = new AppleMusicProvider();
    this.lyrics = new LyricsProvider();
    this.acoustId = new AcoustIdProvider();
  }
  /**
   * Return real-time status and diagnostics of all integrated providers.
   */
  getProvidersStatus() {
    return [
      this.musicBrainz.getStatus(),
      this.discogs.getStatus(),
      this.theAudioDb.getStatus(),
      this.coverArtArchive.getStatus(),
      this.spotify.getStatus(),
      this.appleMusic.getStatus(),
      this.lyrics.getStatus(),
      this.acoustId.getStatus()
    ];
  }
  /**
   * Search albums across Core providers with automatic fallback:
   * 1. MusicBrainz (Primary for structured metadata)
   * 2. Fallback to Discogs (for specific vinyl pressings)
   * 3. Fallback to TheAudioDB
   */
  async searchAlbums(query) {
    try {
      const mbResults = await this.musicBrainz.searchReleases(query);
      if (mbResults.length > 0) {
        return mbResults;
      }
    } catch (err) {
      console.warn("[MetadataManager] MusicBrainz search failed, proceeding to fallback:", err);
    }
    try {
      const discogsResults = await this.discogs.searchReleases(query);
      if (discogsResults.length > 0) {
        return discogsResults;
      }
    } catch (err) {
      console.warn("[MetadataManager] Discogs search fallback failed:", err);
    }
    try {
      const tadbResult = await this.theAudioDb.searchAlbum(query);
      if (tadbResult) {
        return [tadbResult];
      }
    } catch (err) {
      console.warn("[MetadataManager] TheAudioDB fallback failed:", err);
    }
    return [];
  }
  /**
   * Get full album details by ID or MBID.
   */
  async getAlbumDetails(id, source, mbid) {
    const targetMbid = mbid || (id.startsWith("mb_") ? id.replace("mb_", "") : void 0);
    if (targetMbid) {
      const mbDetail = await this.musicBrainz.getReleaseDetails(targetMbid);
      if (mbDetail) {
        if (!mbDetail.covers || mbDetail.covers.length === 0) {
          const caaCovers = await this.coverArtArchive.getCoversByMbid(targetMbid);
          if (caaCovers.length > 0) {
            mbDetail.covers = caaCovers;
            mbDetail.coverUrl = caaCovers.find((c) => c.type === "front")?.url || caaCovers[0].url;
          }
        }
        return mbDetail;
      }
    }
    if (id.startsWith("discogs_") || source === "discogs") {
      const discogsId = id.replace("discogs_", "");
      return this.discogs.getReleaseDetails(discogsId);
    }
    return null;
  }
  /**
   * Fetch artwork in parallel from Cover Art Archive, Spotify, Apple Music, and Discogs.
   * Merges and deduplicates images, sorting by resolution and front/back priority.
   */
  async getCovers(query, mbid) {
    const promises = [];
    if (mbid) {
      promises.push(this.coverArtArchive.getCoversByMbid(mbid));
    }
    if (this.spotify.isConfigured()) {
      promises.push(this.spotify.searchAlbumCovers(query));
    }
    promises.push(this.appleMusic.searchAlbumCovers(query));
    const settled = await Promise.allSettled(promises);
    const allCovers = [];
    const seenUrls = /* @__PURE__ */ new Set();
    for (const res of settled) {
      if (res.status === "fulfilled" && Array.isArray(res.value)) {
        for (const img of res.value) {
          if (img.url && !seenUrls.has(img.url)) {
            seenUrls.add(img.url);
            allCovers.push(img);
          }
        }
      }
    }
    return allCovers.sort((a, b) => {
      if (a.type === "front" && b.type !== "front") return -1;
      if (b.type === "front" && a.type !== "front") return 1;
      if (a.isHighRes && !b.isHighRes) return -1;
      if (b.isHighRes && !a.isHighRes) return 1;
      return (b.width || 0) - (a.width || 0);
    });
  }
  /**
   * Fetch artist biography, styles, fanart and banners from TheAudioDB.
   */
  async getArtistBio(artistName) {
    return this.theAudioDb.getArtistBio(artistName);
  }
  /**
   * Fetch plain and synchronized LRC lyrics for a track.
   */
  async getLyrics(artist, trackTitle, albumTitle) {
    return this.lyrics.getLyrics(artist, trackTitle, albumTitle);
  }
  /**
   * Identify untagged audio via AcoustID Chromaprint fingerprint.
   * If matched, automatically enriches with MusicBrainz and Cover Art Archive details!
   */
  async identifyAudio(durationSeconds, fingerprint) {
    return this.acoustId.lookupFingerprint(durationSeconds, fingerprint);
  }
  /**
   * Multi-Source Aggregation: Combines core metadata, artist biography, and all cover variants.
   */
  async getAggregatedAlbum(query) {
    const searchResults = await this.searchAlbums(query);
    if (searchResults.length === 0) return null;
    const primary = searchResults[0];
    const sourcesUsed = [primary.source];
    let detailed = primary;
    if (primary.mbid) {
      const full = await this.musicBrainz.getReleaseDetails(primary.mbid);
      if (full) detailed = { ...primary, ...full };
    }
    let artistBio;
    try {
      const bio = await this.theAudioDb.getArtistBio(detailed.artist);
      if (bio) {
        artistBio = bio;
        sourcesUsed.push("theaudiodb");
      }
    } catch (e) {
    }
    const covers = await this.getCovers(
      { artist: detailed.artist, title: detailed.title },
      detailed.mbid
    );
    if (covers.length > 0) {
      detailed.covers = covers;
      if (!detailed.coverUrl) {
        detailed.coverUrl = covers[0].url;
      }
    }
    return {
      core: detailed,
      artistBio,
      covers,
      sourcesUsed: Array.from(new Set(sourcesUsed))
    };
  }
};
var metadataManager = new MetadataManager();

// server.ts
var app = (0, import_express.default)();
var PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3e3;
app.use(import_express.default.json({ limit: "50mb" }));
app.use(import_express.default.urlencoded({ limit: "50mb", extended: true }));
var geminiClient = null;
function getGemini() {
  if (!geminiClient && process.env.GEMINI_API_KEY) {
    try {
      geminiClient = new import_genai.GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    } catch (e) {
      console.warn("Could not initialize GoogleGenAI:", e);
    }
  }
  return geminiClient;
}
var serverCache = /* @__PURE__ */ new Map();
function cleanArtistVariants(artist) {
  const raw = (artist || "").trim();
  if (!raw) return [];
  const stripped = raw.replace(/\s*\(\d+\)\s*$/g, "").replace(/["']/g, "").trim();
  const variants = /* @__PURE__ */ new Set();
  variants.add(stripped);
  if (/,\s*The$/i.test(stripped)) {
    variants.add(`The ${stripped.replace(/,\s*The$/i, "").trim()}`);
  }
  if (stripped.includes(",") && !stripped.includes("&") && !stripped.includes("and")) {
    const parts = stripped.split(",").map((p) => p.trim());
    if (parts.length === 2 && parts[0] && parts[1]) {
      variants.add(`${parts[1]} ${parts[0]}`);
    }
  }
  if (stripped.includes("&")) {
    variants.add(stripped.replace("&", "and"));
    const firstArtist = stripped.split("&")[0].trim();
    if (firstArtist) variants.add(firstArtist);
  } else if (/\band\b/i.test(stripped)) {
    variants.add(stripped.replace(/\band\b/gi, "&"));
    const firstArtist = stripped.split(/\band\b/i)[0].trim();
    if (firstArtist) variants.add(firstArtist);
  }
  for (const v of Array.from(variants)) {
    const unaccented = v.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
    if (unaccented !== v) {
      variants.add(unaccented);
    }
  }
  return Array.from(variants).filter(Boolean);
}
function cleanTitleVariants(title) {
  const base = (title || "").replace(/^["'\s]+|["'\s]+$/g, "").replace(/^\.{2,}/, "").replace(/★/g, "Blackstar").replace(/^\d+\.\s*/, "").replace(/["']/g, "").trim();
  const variants = /* @__PURE__ */ new Set();
  variants.add(base);
  const baseLower = base.toLowerCase();
  if (baseLower === "untitled") {
    variants.add("Led Zeppelin IV");
    variants.add("IV");
  }
  if (baseLower.includes("\u2605") || baseLower.includes("blackstar")) {
    variants.add("Blackstar");
  }
  if (baseLower.includes("ziggy stardust")) {
    variants.add("Ziggy Stardust");
  }
  if (baseLower.startsWith("...nothing like the sun")) {
    variants.add("Nothing Like The Sun");
  }
  if (baseLower.startsWith("...nada como el sol")) {
    variants.add("Nada Como El Sol");
  }
  if (baseLower.includes("outside (the nathan adler")) {
    variants.add("Outside");
  }
  if (baseLower.includes("satisfaction / con le mie lacrime")) {
    variants.add("Satisfaction");
    variants.add("Con Le Mie Lacrime");
  }
  if (baseLower.includes("(she's) sexy + 17")) {
    variants.add("Sexy + 17");
    variants.add("Sexy and 17");
  }
  const cleanParentheses = base.replace(/\s*\(.*?\)\s*/g, " ").replace(/\s*\[.*?\]\s*/g, " ").replace(/\b(2xLP|LP|CD|Album|EP|MiniAlbum|Comp|Deluxe|Edition|Remastered|Unofficial|RP|Gat|Ltd|Num|Pic|Son|Car|Dig)\b/gi, "").replace(/\s+/g, " ").trim();
  if (cleanParentheses) variants.add(cleanParentheses);
  const cleanSuffixes = cleanParentheses.replace(/\s*-\s*(?:remaster|deluxe|edition|anniversary|version|live|popular favorites|mono|stereo|original).*/gi, "").trim();
  if (cleanSuffixes) variants.add(cleanSuffixes);
  if (cleanParentheses.includes("/")) {
    const firstPart = cleanParentheses.split("/")[0].trim();
    if (firstPart) variants.add(firstPart);
  }
  if (cleanParentheses.includes("\u2022")) {
    const firstPart = cleanParentheses.split("\u2022")[0].trim();
    if (firstPart) variants.add(firstPart);
  }
  for (const v of Array.from(variants)) {
    const unaccented = v.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
    if (unaccented !== v) {
      variants.add(unaccented);
    }
  }
  return Array.from(variants).filter(Boolean);
}
function normalizeMatchText(s) {
  if (!s) return "";
  return s.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/\s*\(\d+\)$/, "").replace(/^the\s+/, "").replace(/[^a-z0-9]/g, "");
}
function isArtistMatch(targetArtist, candidateArtist) {
  if (!candidateArtist) return false;
  const t = normalizeMatchText(targetArtist);
  const c = normalizeMatchText(candidateArtist);
  if (!t || !c) return false;
  if (t === c) return true;
  const candLower = candidateArtist.toLowerCase();
  if (candLower.includes("tribute") || candLower.includes("karaoke") || candLower.includes("in the style of") || candLower.includes("originally performed") || candLower.includes("orchestra") && !targetArtist.toLowerCase().includes("orchestra") || candLower.includes("string quartet") && !targetArtist.toLowerCase().includes("string quartet") || candLower.includes("instrumental version")) {
    return false;
  }
  if (t.includes(c) || c.includes(t)) {
    const minLen = Math.min(t.length, c.length);
    if (minLen >= 4) return true;
  }
  const tWords = targetArtist.toLowerCase().split(/[\s,&/-]+/).filter((w) => w.length > 2 && w !== "the" && w !== "and");
  const cWords = candidateArtist.toLowerCase().split(/[\s,&/-]+/).filter((w) => w.length > 2 && w !== "the" && w !== "and");
  const matchedWords = tWords.filter((w) => cWords.includes(w));
  if (matchedWords.length > 0 && matchedWords.length >= Math.min(tWords.length, cWords.length)) {
    return true;
  }
  return false;
}
function isAlbumMatch(targetAlbum, candidateAlbum) {
  if (!candidateAlbum) return false;
  const t = normalizeMatchText(targetAlbum);
  const c = normalizeMatchText(candidateAlbum);
  if (!t || !c) return false;
  if (t === c || t.includes(c) || c.includes(t)) return true;
  const cleanT = normalizeMatchText(targetAlbum.split(/[:\-\(]/)[0]);
  const cleanC = normalizeMatchText(candidateAlbum.split(/[:\-\(]/)[0]);
  if (cleanT && cleanC && (cleanT === cleanC || cleanT.includes(cleanC) || cleanC.includes(cleanT))) {
    return true;
  }
  if ((targetAlbum.toLowerCase().includes("untitled") || targetAlbum.toLowerCase().includes("iv")) && (candidateAlbum.toLowerCase().includes("untitled") || candidateAlbum.toLowerCase().includes("iv") || candidateAlbum.toLowerCase().includes("four"))) {
    return true;
  }
  return false;
}
var albumTracksCache = /* @__PURE__ */ new Map();
async function queryDeezer(artist, title) {
  const artistList = cleanArtistVariants(artist);
  const titleList = cleanTitleVariants(title);
  const candidates = [];
  for (const a of artistList.slice(0, 2)) {
    for (const v of titleList.slice(0, 2)) {
      try {
        const q = encodeURIComponent(`artist:"${a}" album:"${v}"`);
        const res = await fetch(`https://api.deezer.com/search?q=${q}&limit=4`, { signal: AbortSignal.timeout(3500) });
        if (res.ok) {
          const data = await res.json();
          if (data.data && Array.isArray(data.data)) {
            for (const item of data.data) {
              if (!isArtistMatch(artist, item.artist?.name)) continue;
              const cover = item.album?.cover_xl || item.album?.cover_big;
              if (cover && !candidates.some((c) => c.coverUrl === cover)) {
                candidates.push({
                  coverUrl: cover,
                  source: "Deezer",
                  resolution: "1000x1000 HD",
                  title: item.title,
                  artist: item.artist?.name,
                  albumName: item.album?.title,
                  previewAudioUrl: item.preview || void 0,
                  previewTrackTitle: item.title || void 0
                });
              }
            }
          }
        }
      } catch (e) {
      }
      if (candidates.length < 2) {
        try {
          const q = encodeURIComponent(`${a} ${v}`);
          const res = await fetch(`https://api.deezer.com/search?q=${q}&limit=4`, { signal: AbortSignal.timeout(3500) });
          if (res.ok) {
            const data = await res.json();
            if (data.data && Array.isArray(data.data)) {
              for (const item of data.data) {
                if (!isArtistMatch(artist, item.artist?.name)) continue;
                const cover = item.album?.cover_xl || item.album?.cover_big;
                if (cover && !candidates.some((c) => c.coverUrl === cover)) {
                  candidates.push({
                    coverUrl: cover,
                    source: "Deezer",
                    resolution: "1000x1000 HD",
                    title: item.title,
                    artist: item.artist?.name,
                    albumName: item.album?.title,
                    previewAudioUrl: item.preview || void 0,
                    previewTrackTitle: item.title || void 0
                  });
                }
              }
            }
          }
        } catch (e) {
        }
      }
      if (candidates.length > 0) break;
    }
    if (candidates.length > 0) break;
  }
  return candidates;
}
async function queryiTunes(artist, title) {
  const artistList = cleanArtistVariants(artist);
  const titleList = cleanTitleVariants(title);
  const candidates = [];
  for (const a of artistList.slice(0, 2)) {
    for (const v of titleList.slice(0, 2)) {
      try {
        const itTerm = encodeURIComponent(`${a} ${v}`);
        const res = await fetch(`https://itunes.apple.com/search?term=${itTerm}&media=music&entity=album&limit=5`, { signal: AbortSignal.timeout(3500) });
        if (res.ok) {
          const data = await res.json();
          if (data.results && Array.isArray(data.results)) {
            for (const item of data.results) {
              if (!isArtistMatch(artist, item.artistName)) continue;
              const cover = item.artworkUrl100?.replace("100x100bb", "1000x1000bb");
              if (cover && !candidates.some((c) => c.coverUrl === cover)) {
                candidates.push({
                  coverUrl: cover,
                  source: "Apple Music",
                  resolution: "1000x1000 HD",
                  title: item.collectionName,
                  artist: item.artistName,
                  albumName: item.collectionName,
                  year: item.releaseDate ? item.releaseDate.substring(0, 4) : void 0
                });
              }
            }
          }
        }
      } catch (e) {
      }
      try {
        const itTerm = encodeURIComponent(`${a} ${v}`);
        const res = await fetch(`https://itunes.apple.com/search?term=${itTerm}&media=music&entity=song&limit=5`, { signal: AbortSignal.timeout(3500) });
        if (res.ok) {
          const data = await res.json();
          if (data.results && Array.isArray(data.results)) {
            for (const item of data.results) {
              if (!isArtistMatch(artist, item.artistName)) continue;
              const cover = item.artworkUrl100?.replace("100x100bb", "1000x1000bb");
              if (cover && !candidates.some((c) => c.coverUrl === cover)) {
                candidates.push({
                  coverUrl: cover,
                  source: "Apple Music",
                  resolution: "1000x1000 HD",
                  title: item.trackName,
                  artist: item.artistName,
                  albumName: item.collectionName,
                  previewAudioUrl: item.previewUrl || void 0,
                  previewTrackTitle: item.trackName || void 0,
                  year: item.releaseDate ? item.releaseDate.substring(0, 4) : void 0
                });
              }
            }
          }
        }
      } catch (e) {
      }
      if (candidates.length > 0) break;
    }
    if (candidates.length > 0) break;
  }
  if (candidates.length === 0 && titleList[0]) {
    try {
      const itTerm = encodeURIComponent(titleList[0]);
      const res = await fetch(`https://itunes.apple.com/search?term=${itTerm}&media=music&entity=album&limit=6`, { signal: AbortSignal.timeout(3500) });
      if (res.ok) {
        const data = await res.json();
        if (data.results && Array.isArray(data.results)) {
          for (const item of data.results) {
            if (isArtistMatch(artist, item.artistName)) {
              const cover = item.artworkUrl100?.replace("100x100bb", "1000x1000bb");
              if (cover && !candidates.some((c) => c.coverUrl === cover)) {
                candidates.push({
                  coverUrl: cover,
                  source: "Apple Music",
                  resolution: "1000x1000 HD",
                  title: item.collectionName,
                  artist: item.artistName,
                  albumName: item.collectionName,
                  year: item.releaseDate ? item.releaseDate.substring(0, 4) : void 0
                });
              }
            }
          }
        }
      }
    } catch (e) {
    }
  }
  return candidates;
}
async function queryMusicBrainz(artist, title) {
  const cArtist = cleanArtistVariants(artist)[0] || artist;
  const variants = cleanTitleVariants(title);
  const candidates = [];
  for (const v of variants) {
    try {
      const mbQ = encodeURIComponent(`artist:"${cArtist}" AND release:"${v}"`);
      const res = await fetch(`https://musicbrainz.org/ws/2/release/?query=${mbQ}&limit=2&fmt=json`, {
        headers: { "User-Agent": "Viniloteca/1.0 (info@viniloteca.app)" }
      });
      if (res.ok) {
        const data = await res.json();
        if (data.releases && Array.isArray(data.releases)) {
          for (const rel of data.releases.slice(0, 2)) {
            try {
              const caRes = await fetch(`https://coverartarchive.org/release/${rel.id}`);
              if (caRes.ok) {
                const caData = await caRes.json();
                const img = caData.images?.[0]?.thumbnails?.large || caData.images?.[0]?.image;
                if (img && !candidates.some((c) => c.coverUrl === img)) {
                  candidates.push({
                    coverUrl: img,
                    source: "Cover Art Archive",
                    resolution: "Scansione Vinile Originale",
                    title: rel.title,
                    artist: rel["artist-credit"]?.[0]?.name || cArtist,
                    albumName: rel.title,
                    year: rel.date ? rel.date.substring(0, 4) : void 0
                  });
                }
              }
            } catch (err) {
            }
          }
        }
      }
    } catch (e) {
    }
    if (candidates.length > 0) break;
  }
  return candidates;
}
function getDiscogsHeaders() {
  const headers = {
    "User-Agent": "VinilotecaApp/1.0 (+https://viniloteca.app; mailto:support@viniloteca.app)",
    "Accept": "application/json"
  };
  const token = process.env.DISCOGS_TOKEN;
  if (token && token.trim()) {
    headers["Authorization"] = `Discogs token=${token.trim()}`;
  }
  return headers;
}
async function queryDiscogs(artist, title) {
  const candidates = [];
  const cArtist = cleanArtistVariants(artist)[0] || artist;
  const cTitle = cleanTitleVariants(title)[0] || title;
  const headers = getDiscogsHeaders();
  try {
    const q = encodeURIComponent(`${cArtist} ${cTitle}`);
    const res = await fetch(
      `https://api.discogs.com/database/search?q=${q}&type=release&per_page=6`,
      {
        headers,
        signal: AbortSignal.timeout(4e3)
      }
    );
    if (res.ok) {
      const data = await res.json();
      if (data.results && Array.isArray(data.results)) {
        for (const item of data.results) {
          const rawCover = item.cover_image || item.thumb;
          if (rawCover && !rawCover.includes("spacer.gif")) {
            let itemArtist = cArtist;
            let itemTitle = item.title || cTitle;
            if (item.title && item.title.includes(" - ")) {
              const parts = item.title.split(" - ");
              itemArtist = parts[0].trim();
              itemTitle = parts.slice(1).join(" - ").trim();
            }
            const formatStr = Array.isArray(item.format) ? item.format.join(", ") : item.format || "Vinyl, LP";
            const labelStr = Array.isArray(item.label) ? item.label[0] : item.label;
            candidates.push({
              coverUrl: `/api/music/cover-proxy?url=${encodeURIComponent(rawCover)}`,
              source: "Discogs",
              resolution: `Discogs (${formatStr})`,
              title: itemTitle,
              artist: itemArtist,
              albumName: itemTitle,
              year: item.year ? String(item.year) : void 0,
              discogsId: item.id,
              discogsUrl: item.uri ? `https://www.discogs.com${item.uri}` : `https://www.discogs.com/release/${item.id}`,
              format: formatStr,
              label: labelStr,
              catalogNumber: item.catno
            });
          }
        }
      }
    }
  } catch (e) {
  }
  return candidates;
}
async function queryDiscogsBarcode(barcodeOrCatNo) {
  const headers = getDiscogsHeaders();
  const raw = (barcodeOrCatNo || "").trim();
  if (!raw) return null;
  const isDigitsOnly = /^\d+$/.test(raw.replace(/[\s-]/g, ""));
  const cleanDigits = raw.replace(/[^0-9]/g, "");
  try {
    let results = [];
    if (isDigitsOnly && cleanDigits.length >= 8) {
      const q = encodeURIComponent(cleanDigits);
      const res = await fetch(
        `https://api.discogs.com/database/search?barcode=${q}&type=release&per_page=6`,
        { headers, signal: AbortSignal.timeout(4500) }
      );
      if (res.ok) {
        const data = await res.json();
        if (data && data.results && Array.isArray(data.results)) {
          results = data.results;
        }
      }
    } else {
      const qCat = encodeURIComponent(raw);
      const catRes = await fetch(
        `https://api.discogs.com/database/search?catno=${qCat}&type=release&per_page=6`,
        { headers, signal: AbortSignal.timeout(4500) }
      );
      if (catRes.ok) {
        const catData = await catRes.json();
        if (catData && catData.results && Array.isArray(catData.results)) {
          results = catData.results;
        }
      }
    }
    if (results.length === 0) {
      const qGen = encodeURIComponent(raw);
      try {
        const fallbackRes = await fetch(
          `https://api.discogs.com/database/search?q=${qGen}&type=release&per_page=6`,
          { headers, signal: AbortSignal.timeout(4500) }
        );
        if (fallbackRes.ok) {
          const fallbackData = await fallbackRes.json();
          if (fallbackData && fallbackData.results && Array.isArray(fallbackData.results)) {
            results = fallbackData.results;
          }
        }
      } catch (e) {
      }
    }
    if (results.length === 0 && isDigitsOnly) {
      try {
        const qCatFallback = encodeURIComponent(cleanDigits);
        const catRes = await fetch(
          `https://api.discogs.com/database/search?catno=${qCatFallback}&type=release&per_page=6`,
          { headers, signal: AbortSignal.timeout(4500) }
        );
        if (catRes.ok) {
          const catData = await catRes.json();
          if (catData && catData.results && Array.isArray(catData.results)) {
            results = catData.results;
          }
        }
      } catch (e) {
      }
    }
    if (results.length === 0) return null;
    const vinylRelease = results.find(
      (r) => Array.isArray(r.format) && r.format.some((f) => /vinyl|lp|12"|album/i.test(f))
    );
    const best = vinylRelease || results[0];
    const discogsMatches = results.map((item) => {
      let itemArtist = "";
      let itemTitle = item.title || "";
      if (item.title && item.title.includes(" - ")) {
        const parts = item.title.split(" - ");
        itemArtist = parts[0].trim();
        itemTitle = parts.slice(1).join(" - ").trim();
      }
      const rawCover2 = item.cover_image && !item.cover_image.includes("spacer.gif") ? item.cover_image : item.thumb;
      return {
        id: item.id,
        title: itemTitle,
        artist: itemArtist,
        year: item.year ? String(item.year) : void 0,
        format: Array.isArray(item.format) ? item.format.join(", ") : item.format,
        label: Array.isArray(item.label) ? item.label[0] : item.label,
        catno: item.catno,
        country: item.country,
        coverUrl: rawCover2 ? `/api/music/cover-proxy?url=${encodeURIComponent(rawCover2)}` : void 0,
        discogsUrl: item.uri ? `https://www.discogs.com${item.uri}` : `https://www.discogs.com/release/${item.id}`
      };
    });
    let artist = "";
    let title = best.title || "";
    if (title.includes(" - ")) {
      const parts = title.split(" - ");
      artist = parts[0].trim();
      title = parts.slice(1).join(" - ").trim();
    }
    let format = Array.isArray(best.format) ? best.format.join(", ") : best.format || "Vinyl, LP, Album";
    let label = Array.isArray(best.label) ? best.label[0] : best.label || "";
    let releasedYear = best.year ? String(best.year) : "";
    let catalogNumber = best.catno || "";
    let country = best.country || "";
    let genre = Array.isArray(best.genre) ? best.genre.join(", ") : best.genre || "";
    const rawCover = best.cover_image && !best.cover_image.includes("spacer.gif") ? best.cover_image : best.thumb;
    let coverUrl = rawCover ? `/api/music/cover-proxy?url=${encodeURIComponent(rawCover)}` : void 0;
    const discogsId = best.id;
    const discogsUrl = best.uri ? `https://www.discogs.com${best.uri}` : `https://www.discogs.com/release/${best.id}`;
    let tracks = [];
    try {
      const relRes = await fetch(`https://api.discogs.com/releases/${best.id}`, {
        headers,
        signal: AbortSignal.timeout(4500)
      });
      if (relRes.ok) {
        const relData = await relRes.json();
        if (relData.artists && Array.isArray(relData.artists) && relData.artists.length > 0) {
          const cleanArtist = relData.artists.map((a) => a.name.replace(/\s*\(\d+\)$/, "")).join(", ");
          if (cleanArtist) artist = cleanArtist;
        }
        if (relData.title) title = relData.title;
        if (relData.year && !releasedYear) releasedYear = String(relData.year);
        else if (relData.released && !releasedYear) releasedYear = String(relData.released).substring(0, 4);
        if (relData.country && !country) country = relData.country;
        if (relData.labels && Array.isArray(relData.labels) && relData.labels.length > 0) {
          if (!label && relData.labels[0].name) label = relData.labels[0].name;
          if (!catalogNumber && relData.labels[0].catno) catalogNumber = relData.labels[0].catno;
        }
        if (relData.formats && Array.isArray(relData.formats) && relData.formats.length > 0) {
          const f = relData.formats[0];
          const parts = [f.name, ...f.descriptions || [], f.text].filter(Boolean);
          if (parts.length > 0) format = parts.join(", ");
        }
        if (relData.genres && Array.isArray(relData.genres) && relData.genres.length > 0) {
          genre = relData.genres.join(", ");
        }
        if (!coverUrl && relData.images && Array.isArray(relData.images) && relData.images.length > 0) {
          const primaryImg = relData.images.find((img) => img.type === "primary") || relData.images[0];
          const uri = primaryImg.resource_url || primaryImg.uri;
          if (uri) {
            coverUrl = `/api/music/cover-proxy?url=${encodeURIComponent(uri)}`;
          }
        }
        if (relData.tracklist && Array.isArray(relData.tracklist)) {
          tracks = relData.tracklist.filter((t) => t.type_ !== "heading" && t.title).map((t, idx) => ({
            id: `discogs-${best.id}-${idx + 1}`,
            position: t.position || String(idx + 1),
            title: t.title,
            duration: t.duration || void 0
          }));
        }
      }
    } catch (err) {
    }
    return {
      artist: artist || void 0,
      title: title || void 0,
      label: label || void 0,
      format: format || void 0,
      releasedYear: releasedYear || void 0,
      catalogNumber: catalogNumber || void 0,
      country: country || void 0,
      genre: genre || void 0,
      coverUrl: coverUrl || void 0,
      discogsId,
      discogsUrl,
      discogsMatches: discogsMatches.length > 0 ? discogsMatches : void 0,
      tracks: tracks.length > 0 ? tracks : void 0,
      source: "Discogs Database"
    };
  } catch (e) {
    return null;
  }
}
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", timestamp: (/* @__PURE__ */ new Date()).toISOString() });
});
async function resolveBarcode(rawBarcode) {
  const rawTrimmed = (rawBarcode || "").trim();
  const cleanAlphanumeric = rawTrimmed.replace(/[^0-9A-Za-z]/g, "");
  if (!rawTrimmed && !cleanAlphanumeric) {
    return { found: false, barcode: rawBarcode };
  }
  const cacheKey = `barcode_lookup:${cleanAlphanumeric.toLowerCase()}`;
  if (serverCache.has(cacheKey)) {
    return serverCache.get(cacheKey);
  }
  const variants = /* @__PURE__ */ new Set();
  variants.add(rawTrimmed);
  variants.add(cleanAlphanumeric);
  const isNumeric = /^\d+$/.test(cleanAlphanumeric);
  if (isNumeric) {
    if (cleanAlphanumeric.length === 12) variants.add(`0${cleanAlphanumeric}`);
    if (cleanAlphanumeric.length === 13 && cleanAlphanumeric.startsWith("0")) variants.add(cleanAlphanumeric.substring(1));
    if (cleanAlphanumeric.length === 14 && cleanAlphanumeric.startsWith("00")) variants.add(cleanAlphanumeric.substring(2));
  } else {
    const matchPrefix = cleanAlphanumeric.match(/^([a-zA-Z]+)(\d.*)$/);
    if (matchPrefix) {
      variants.add(`${matchPrefix[1]} ${matchPrefix[2]}`);
      variants.add(`${matchPrefix[1]}-${matchPrefix[2]}`);
    }
    if (rawTrimmed.includes("-")) {
      variants.add(rawTrimmed.replace(/-/g, " "));
    }
  }
  let artist = "";
  let title = "";
  let label = "";
  let format = "LP, Album";
  let releasedYear = "";
  let catalogNumber = "";
  let coverUrl = "";
  let previewAudioUrl = "";
  let previewTrackTitle = "";
  let tracks = [];
  let source = "";
  let country = "";
  let genre = "";
  let discogsId;
  let discogsUrl;
  let discogsMatches;
  for (const b of Array.from(variants)) {
    try {
      const discogsMeta = await queryDiscogsBarcode(b);
      if (discogsMeta && (discogsMeta.title || discogsMeta.artist)) {
        if (!title && discogsMeta.title) title = discogsMeta.title;
        if (!artist && discogsMeta.artist) artist = discogsMeta.artist;
        if (discogsMeta.label) label = discogsMeta.label;
        if (discogsMeta.catalogNumber) catalogNumber = discogsMeta.catalogNumber;
        if (discogsMeta.format) format = discogsMeta.format;
        if (discogsMeta.releasedYear) releasedYear = discogsMeta.releasedYear;
        if (discogsMeta.country) country = discogsMeta.country;
        if (discogsMeta.genre) genre = discogsMeta.genre;
        if (discogsMeta.coverUrl && !coverUrl) coverUrl = discogsMeta.coverUrl;
        if (discogsMeta.discogsId) discogsId = discogsMeta.discogsId;
        if (discogsMeta.discogsUrl) discogsUrl = discogsMeta.discogsUrl;
        if (discogsMeta.discogsMatches) discogsMatches = discogsMeta.discogsMatches;
        if (discogsMeta.tracks && discogsMeta.tracks.length > 0 && tracks.length === 0) {
          tracks = discogsMeta.tracks;
        }
        source = "Discogs Vinyl Database";
        break;
      }
    } catch (e) {
    }
  }
  if (isNumeric) {
    for (const b of Array.from(variants)) {
      try {
        const res = await fetch(`https://api.deezer.com/album/upc:${b}`, {
          signal: AbortSignal.timeout(3500)
        });
        if (res.ok) {
          const data = await res.json();
          if (data && data.id && data.title && !data.error) {
            if (!title) title = data.title;
            if (!artist) artist = data.artist?.name || "";
            if (!coverUrl) coverUrl = data.cover_xl || data.cover_big || "";
            if (!label) label = data.label || "";
            if (!releasedYear && data.release_date) releasedYear = data.release_date.substring(0, 4);
            if (!genre && data.genres?.data?.[0]?.name) genre = data.genres.data[0].name;
            if (data.record_type === "single") format = '7", Single';
            else if (data.record_type === "ep") format = '12", EP';
            else format = "LP, Album";
            if (data.tracks?.data && Array.isArray(data.tracks.data) && data.tracks.data.length > 0 && tracks.length === 0) {
              tracks = data.tracks.data.map((t, idx) => ({
                id: String(t.id || idx + 1),
                position: t.track_position || idx + 1,
                title: t.title || `Traccia ${idx + 1}`,
                duration: t.duration || 30,
                previewAudioUrl: t.preview ? `/api/music/stream-preview?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(t.title)}&url=${encodeURIComponent(t.preview)}` : void 0
              }));
              const firstWithAudio = tracks.find((t) => t.previewAudioUrl);
              if (firstWithAudio) {
                previewAudioUrl = firstWithAudio.previewAudioUrl;
                previewTrackTitle = firstWithAudio.title;
              }
            }
            if (!source) source = "Deezer UPC";
            break;
          }
        }
      } catch (e) {
      }
    }
  }
  if (isNumeric && (!title || !artist || !coverUrl || !previewAudioUrl)) {
    for (const b of Array.from(variants)) {
      try {
        const itRes = await fetch(`https://itunes.apple.com/lookup?upc=${b}&entity=album`, {
          signal: AbortSignal.timeout(3500)
        });
        if (itRes.ok) {
          const itData = await itRes.json();
          if (itData.results && itData.results.length > 0) {
            const albumItem = itData.results[0];
            if (!title) title = albumItem.collectionName;
            if (!artist) artist = albumItem.artistName;
            if (!coverUrl && albumItem.artworkUrl100) {
              coverUrl = albumItem.artworkUrl100.replace("100x100bb", "1000x1000bb");
            }
            if (!releasedYear && albumItem.releaseDate) {
              releasedYear = albumItem.releaseDate.substring(0, 4);
            }
            if (!label && albumItem.copyright) {
              label = albumItem.copyright.replace(/^[℗©\s\d]+/, "").trim();
            }
            if (!genre && albumItem.primaryGenreName) {
              genre = albumItem.primaryGenreName;
            }
            if (!source) source = "Apple Music UPC";
            try {
              const songRes = await fetch(`https://itunes.apple.com/lookup?upc=${b}&entity=song&limit=10`, {
                signal: AbortSignal.timeout(3500)
              });
              if (songRes.ok) {
                const sData = await songRes.json();
                if (sData.results && sData.results.length > 1) {
                  const songItems = sData.results.filter((x) => x.wrapperType === "track");
                  if (songItems.length > 0) {
                    if (!previewAudioUrl && songItems[0].previewUrl) {
                      previewAudioUrl = songItems[0].previewUrl;
                      previewTrackTitle = songItems[0].trackName;
                    }
                    if (tracks.length === 0) {
                      tracks = songItems.map((s, idx) => ({
                        id: String(s.trackId || idx + 1),
                        position: s.trackNumber || idx + 1,
                        title: s.trackName || `Traccia ${idx + 1}`,
                        duration: s.trackTimeMillis ? Math.round(s.trackTimeMillis / 1e3) : 30,
                        previewAudioUrl: s.previewUrl
                      }));
                    }
                  }
                }
              }
            } catch (e) {
            }
            break;
          }
        }
      } catch (e) {
      }
    }
  }
  if (!title || !artist || tracks.length === 0) {
    try {
      const mbParts = [];
      if (isNumeric) {
        for (const v of Array.from(variants)) {
          mbParts.push(`barcode:${v}`);
        }
      } else {
        for (const v of Array.from(variants)) {
          mbParts.push(`catno:"${v}"`);
          mbParts.push(`"${v}"`);
        }
      }
      const mbQuery = encodeURIComponent(mbParts.join(" OR "));
      const mbRes = await fetch(`https://musicbrainz.org/ws/2/release/?query=${mbQuery}&limit=5&fmt=json`, {
        headers: { "User-Agent": "Viniloteca/1.1.0 (contact@viniloteca.app)" },
        signal: AbortSignal.timeout(5e3)
      });
      if (mbRes.ok) {
        const mbData = await mbRes.json();
        if (mbData.releases && mbData.releases.length > 0) {
          const rel = mbData.releases[0];
          if (!title) title = rel.title;
          if (!artist) artist = rel["artist-credit"]?.[0]?.name || "";
          if (!releasedYear && rel.date) releasedYear = rel.date.substring(0, 4);
          if (rel.country && !country) country = rel.country;
          if (!label && rel["label-info-list"]?.[0]?.label?.name) {
            label = rel["label-info-list"][0].label.name;
          }
          if (!catalogNumber && rel["label-info-list"]?.[0]?.["catalog-number"]) {
            catalogNumber = rel["label-info-list"][0]["catalog-number"];
          }
          const mediaFormat = rel.media?.[0]?.format || "";
          if (mediaFormat) {
            if (mediaFormat.toLowerCase().includes("vinyl") || mediaFormat.toLowerCase().includes('12"')) {
              format = (rel.media?.length || 1) > 1 ? `${rel.media.length}xLP, Album` : "LP, Album";
            } else if (mediaFormat.toLowerCase().includes("cd")) {
              format = (rel.media?.length || 1) > 1 ? `${rel.media.length}xCD, Album` : "CD, Album";
            } else if (mediaFormat.toLowerCase().includes('7"')) {
              format = '7", Single';
            } else {
              format = mediaFormat;
            }
          }
          if (!source) source = "MusicBrainz Catalog";
          if (rel.id && tracks.length === 0) {
            try {
              const detailRes = await fetch(
                `https://musicbrainz.org/ws/2/release/${rel.id}?inc=recordings+artists+labels+media&fmt=json`,
                {
                  headers: { "User-Agent": "Viniloteca/1.1.0 (contact@viniloteca.app)" },
                  signal: AbortSignal.timeout(4500)
                }
              );
              if (detailRes.ok) {
                const detail = await detailRes.json();
                if (detail.media && Array.isArray(detail.media)) {
                  const mbTracks = [];
                  detail.media.forEach((med, mIdx) => {
                    const sideLetter = detail.media.length > 1 ? mIdx === 0 ? "A" : mIdx === 1 ? "B" : mIdx === 2 ? "C" : "D" : "";
                    if (med.tracks && Array.isArray(med.tracks)) {
                      med.tracks.forEach((t, tIdx) => {
                        mbTracks.push({
                          id: t.id || `mb-${rel.id}-${mIdx + 1}-${tIdx + 1}`,
                          position: t.number || (sideLetter ? `${sideLetter}${tIdx + 1}` : String(tIdx + 1)),
                          title: t.title || t.recording?.title || `Traccia ${tIdx + 1}`,
                          duration: t.length ? Math.round(t.length / 1e3) : 30
                        });
                      });
                    }
                  });
                  if (mbTracks.length > 0) {
                    tracks = mbTracks;
                  }
                }
              }
            } catch (e) {
            }
          }
          if (!coverUrl && rel.id) {
            try {
              const caRes = await fetch(`https://coverartarchive.org/release/${rel.id}`, {
                signal: AbortSignal.timeout(3500)
              });
              if (caRes.ok) {
                const caData = await caRes.json();
                const caImg = caData.images?.[0]?.thumbnails?.large || caData.images?.[0]?.image;
                if (caImg) coverUrl = caImg;
              }
            } catch (e) {
            }
          }
        }
      }
    } catch (e) {
    }
  }
  if (artist && title) {
    if (!coverUrl || !previewAudioUrl) {
      try {
        const [itRes, dzRes] = await Promise.all([
          queryiTunes(artist, title),
          queryDeezer(artist, title)
        ]);
        const candidates = [...itRes, ...dzRes];
        if (!coverUrl) {
          const withCover = candidates.find((c) => c.coverUrl);
          if (withCover) coverUrl = withCover.coverUrl;
        }
        if (!previewAudioUrl) {
          const withAudio = candidates.find((c) => c.previewAudioUrl);
          if (withAudio) {
            previewAudioUrl = withAudio.previewAudioUrl;
            previewTrackTitle = withAudio.previewTrackTitle || withAudio.title || title;
          }
        }
      } catch (e) {
      }
    }
  }
  if ((!title || !artist) && getGemini()) {
    try {
      const ai = getGemini();
      const prompt = `Identify the commercial vinyl or CD music album release associated with barcode, catalog number, or release code "${rawTrimmed}".
Return ONLY a valid JSON object with:
{
  "found": true,
  "artist": "Artist name",
  "title": "Album title",
  "label": "Record label",
  "releasedYear": "YYYY",
  "catalogNumber": "Catalog number",
  "format": "LP, Album",
  "genre": "Genre"
}
If unknown or not found, return {"found": false}. Output only pure JSON without markdown.`;
      const aiRes = await ai.models.generateContent({
        model: "gemini-2.5-flash",
        contents: prompt
      });
      const text = aiRes.text ? aiRes.text.trim().replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "") : "";
      if (text) {
        const parsed = JSON.parse(text);
        if (parsed.found && parsed.artist && parsed.title) {
          artist = parsed.artist;
          title = parsed.title;
          if (parsed.label) label = parsed.label;
          if (parsed.releasedYear) releasedYear = String(parsed.releasedYear);
          if (parsed.catalogNumber) catalogNumber = parsed.catalogNumber;
          if (parsed.format) format = parsed.format;
          if (parsed.genre) genre = parsed.genre;
          source = "Gemini AI + Disc Catalog";
          const [itRes, dzRes] = await Promise.all([
            queryiTunes(artist, title),
            queryDeezer(artist, title)
          ]);
          const candidates = [...itRes, ...dzRes];
          const bestWithCover = candidates.find((c) => c.coverUrl);
          if (bestWithCover) coverUrl = bestWithCover.coverUrl;
          const bestWithAudio = candidates.find((c) => c.previewAudioUrl);
          if (bestWithAudio) {
            previewAudioUrl = bestWithAudio.previewAudioUrl;
            previewTrackTitle = bestWithAudio.previewTrackTitle || bestWithAudio.title || title;
          }
        }
      }
    } catch (e) {
      console.warn("Gemini barcode resolution failed:", e);
    }
  }
  const result = {
    found: Boolean(artist && title),
    barcode: cleanAlphanumeric || rawTrimmed,
    artist: artist || void 0,
    title: title || void 0,
    label: label || "Etichetta Indipendente",
    format: format || "LP, Album",
    releasedYear: releasedYear || "",
    catalogNumber: catalogNumber || rawTrimmed || cleanAlphanumeric,
    coverUrl: coverUrl || void 0,
    previewAudioUrl: previewAudioUrl || void 0,
    previewTrackTitle: previewTrackTitle || void 0,
    tracks: tracks.length > 0 ? tracks : void 0,
    source: source || "Database Vinili",
    country: country || void 0,
    genre: genre || void 0,
    discogsId,
    discogsUrl,
    discogsMatches: discogsMatches && discogsMatches.length > 0 ? discogsMatches : void 0
  };
  if (result.found) {
    serverCache.set(cacheKey, result);
  }
  return result;
}
app.get("/api/music/barcode", async (req, res) => {
  const barcode = String(req.query.barcode || req.query.catno || req.query.code || "").trim();
  if (!barcode) {
    return res.status(400).json({ error: "Codice a barre o numero di catalogo richiesto" });
  }
  try {
    const result = await resolveBarcode(barcode);
    return res.json(result);
  } catch (err) {
    console.error("Error resolving barcode/catalog:", err);
    return res.status(500).json({ error: "Errore durante la risoluzione del codice a barre o catalogo" });
  }
});
app.post("/api/music/recognize-cover", async (req, res) => {
  try {
    const { imageBase64 } = req.body;
    if (!imageBase64) {
      return res.status(400).json({ error: "Immagine di copertina richiesta in formato Base64" });
    }
    const cleanBase64 = imageBase64.includes(",") ? imageBase64.split(",")[1] : imageBase64;
    const ai = getGemini();
    if (ai) {
      const prompt = `You are a vinyl record recognition expert. Look at this vinyl cover or album artwork.
Identify the musical artist name, album title, approximate release year, record label, and catalog number (if visible).
Return ONLY a valid JSON object with the following schema:
{
  "found": true,
  "artist": "Artist name",
  "title": "Album title",
  "releasedYear": "YYYY",
  "label": "Record label",
  "catalogNumber": "CatNo or empty",
  "genre": "Genre"
}
If unidentifiable, return {"found": false}. Output ONLY pure JSON without markdown code fences.`;
      const aiRes = await ai.models.generateContent({
        model: "gemini-2.5-flash",
        contents: [
          {
            role: "user",
            parts: [
              { text: prompt },
              {
                inlineData: {
                  mimeType: "image/jpeg",
                  data: cleanBase64
                }
              }
            ]
          }
        ]
      });
      const text = aiRes.text ? aiRes.text.trim().replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "") : "";
      if (text) {
        try {
          const parsed = JSON.parse(text);
          if (parsed.found && (parsed.artist || parsed.title)) {
            const queryTerm = `${parsed.artist || ""} ${parsed.title || ""}`.trim() || parsed.catalogNumber;
            const lookup = queryTerm ? await resolveBarcode(queryTerm) : null;
            if (lookup && lookup.found) {
              return res.json({
                ...lookup,
                artist: parsed.artist || lookup.artist,
                title: parsed.title || lookup.title,
                label: parsed.label || lookup.label,
                releasedYear: parsed.releasedYear || lookup.releasedYear,
                source: "Gemini Vision AI + Music DB"
              });
            }
            return res.json({
              found: true,
              artist: parsed.artist || "Artista Sconosciuto",
              title: parsed.title || "Senza Titolo",
              label: parsed.label || "Etichetta Discografica",
              format: "LP, Album",
              releasedYear: parsed.releasedYear,
              catalogNumber: parsed.catalogNumber,
              genre: parsed.genre,
              source: "Gemini Vision AI"
            });
          }
        } catch (parseErr) {
          console.warn("Cover vision JSON parse error:", parseErr);
        }
      }
    }
    return res.json({
      found: false,
      message: ai ? "Impossibile estrarre con certezza artista o titolo da questa foto. Assicurati che l'immagine sia a fuoco e ben illuminata." : "Per il riconoscimento visivo con AI inserisci una chiave GEMINI_API_KEY nel file .env, oppure usa la scansione del codice a barre/catalogo."
    });
  } catch (err) {
    console.error("Error in /api/music/recognize-cover:", err);
    return res.status(500).json({ error: err.message || "Errore durante l'analisi visiva della copertina" });
  }
});
app.get("/api/discogs/barcode/:barcode", async (req, res) => {
  const rawBarcode = String(req.params.barcode || "").trim();
  if (!rawBarcode) {
    return res.status(400).json({ error: "Codice a barre o catalogo Discogs mancante" });
  }
  try {
    const discogsMeta = await queryDiscogsBarcode(rawBarcode);
    if (discogsMeta && (discogsMeta.title || discogsMeta.artist)) {
      return res.json({
        found: true,
        barcode: rawBarcode,
        ...discogsMeta
      });
    }
    const fallbackMeta = await resolveBarcode(rawBarcode);
    if (fallbackMeta && fallbackMeta.found) {
      return res.json(fallbackMeta);
    }
    return res.json({ found: false, barcode: rawBarcode });
  } catch (err) {
    console.error("Error in /api/discogs/barcode:", err);
    return res.status(500).json({ error: "Errore nella ricerca Discogs del codice a barre" });
  }
});
app.get("/api/music/cover-proxy", async (req, res) => {
  const targetUrl = String(req.query.url || "").trim();
  if (!targetUrl || !targetUrl.startsWith("http")) {
    return res.status(400).send("Invalid image URL");
  }
  const cacheKey = `img_proxy:${targetUrl}`;
  if (serverCache.has(cacheKey)) {
    const cached = serverCache.get(cacheKey);
    res.setHeader("Content-Type", cached.contentType);
    res.setHeader("Cache-Control", "public, max-age=604800, immutable");
    return res.send(cached.buffer);
  }
  try {
    const upstreamRes = await fetch(targetUrl, {
      signal: AbortSignal.timeout(6e3),
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
      }
    });
    if (!upstreamRes.ok) {
      return res.status(upstreamRes.status).send("Upstream cover fetch failed");
    }
    const contentType = upstreamRes.headers.get("content-type") || "image/jpeg";
    const arrayBuf = await upstreamRes.arrayBuffer();
    const buffer = Buffer.from(arrayBuf);
    if (buffer.byteLength < 5 * 1024 * 1024) {
      serverCache.set(cacheKey, { contentType, buffer });
    }
    res.setHeader("Content-Type", contentType);
    res.setHeader("Cache-Control", "public, max-age=604800, immutable");
    return res.send(buffer);
  } catch (err) {
    return res.status(502).send("Error proxying album cover");
  }
});
app.get("/api/music/search", async (req, res) => {
  const artist = String(req.query.artist || "").trim();
  const title = String(req.query.title || "").trim();
  if (!artist && !title) {
    return res.status(400).json({ error: "Artist or title parameter required" });
  }
  const cacheKey = `${artist.toLowerCase()}:::${title.toLowerCase()}`;
  if (serverCache.has(cacheKey)) {
    return res.json(serverCache.get(cacheKey));
  }
  try {
    const [itunesRes, deezerRes, mbRes, discogsRes] = await Promise.all([
      queryiTunes(artist, title),
      queryDeezer(artist, title),
      queryMusicBrainz(artist, title),
      queryDiscogs(artist, title)
    ]);
    const allCandidates = [...itunesRes, ...deezerRes, ...discogsRes, ...mbRes];
    if (allCandidates.length === 0) {
      const emptyResult = { found: false, candidates: [] };
      serverCache.set(cacheKey, emptyResult);
      return res.json(emptyResult);
    }
    const bestWithAudio = allCandidates.find((c) => c.previewAudioUrl && c.coverUrl) || allCandidates[0];
    const result = {
      found: true,
      coverUrl: bestWithAudio.coverUrl,
      previewAudioUrl: bestWithAudio.previewAudioUrl,
      previewTrackTitle: bestWithAudio.previewTrackTitle || bestWithAudio.title,
      artistName: bestWithAudio.artist,
      albumName: bestWithAudio.albumName || bestWithAudio.title,
      year: bestWithAudio.year,
      source: bestWithAudio.source,
      candidates: allCandidates
    };
    serverCache.set(cacheKey, result);
    return res.json(result);
  } catch (err) {
    console.error("API /api/music/search error:", err);
    return res.status(500).json({ error: "Search failed" });
  }
});
app.get("/api/music/candidates", async (req, res) => {
  const query = String(req.query.q || "").trim();
  const artist = String(req.query.artist || "").trim();
  const title = String(req.query.title || "").trim();
  const searchArtist = artist || query;
  const searchTitle = title || query;
  try {
    const [itunesRes, deezerRes, mbRes, discogsRes] = await Promise.all([
      queryiTunes(searchArtist, searchTitle),
      queryDeezer(searchArtist, searchTitle),
      queryMusicBrainz(searchArtist, searchTitle),
      queryDiscogs(searchArtist, searchTitle)
    ]);
    const candidates = [...itunesRes, ...deezerRes, ...discogsRes, ...mbRes];
    return res.json({ candidates });
  } catch (err) {
    console.error("API /api/music/candidates error:", err);
    return res.status(500).json({ error: "Candidates fetch failed", candidates: [] });
  }
});
var audioStreamCache = /* @__PURE__ */ new Map();
async function resolveAudioStreamUrl(artist, title, hintUrl) {
  const cacheKey = `${artist.toLowerCase().trim()}:::${title.toLowerCase().trim()}`;
  const now = Date.now();
  if (hintUrl && hintUrl.includes("itunes.apple.com")) {
    return { url: hintUrl };
  }
  const cached = audioStreamCache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    return { url: cached.url, trackTitle: cached.trackTitle };
  }
  try {
    const itunes = await queryiTunes(artist, title);
    const itMatch = itunes.find((c) => c.previewAudioUrl && isArtistMatch(artist, c.artist));
    if (itMatch && itMatch.previewAudioUrl) {
      const entry = {
        url: itMatch.previewAudioUrl,
        trackTitle: itMatch.previewTrackTitle || itMatch.title,
        expiresAt: now + 14 * 24 * 3600 * 1e3
        // Apple preview URLs are permanent
      };
      audioStreamCache.set(cacheKey, entry);
      return entry;
    }
  } catch (e) {
  }
  try {
    const deezer = await queryDeezer(artist, title);
    const dzMatch = deezer.find((c) => c.previewAudioUrl && isArtistMatch(artist, c.artist));
    if (dzMatch && dzMatch.previewAudioUrl) {
      const entry = {
        url: dzMatch.previewAudioUrl,
        trackTitle: dzMatch.previewTrackTitle || dzMatch.title,
        expiresAt: now + 50 * 60 * 1e3
        // Deezer Akamai tokens valid ~1 hour
      };
      audioStreamCache.set(cacheKey, entry);
      return entry;
    }
  } catch (e) {
  }
  return null;
}
app.get("/api/music/resolve-preview", async (req, res) => {
  const artist = String(req.query.artist || "").trim();
  const title = String(req.query.title || "").trim();
  const hintUrl = String(req.query.url || "").trim();
  if (!artist && !title && !hintUrl) {
    return res.status(400).json({ error: "artist or title is required" });
  }
  try {
    const resolved = await resolveAudioStreamUrl(artist, title, hintUrl);
    if (!resolved) {
      return res.json({ found: false });
    }
    return res.json({
      found: true,
      previewAudioUrl: resolved.url,
      previewTrackTitle: resolved.trackTitle,
      streamUrl: `/api/music/stream-preview?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(title)}`
    });
  } catch (err) {
    console.error("Error in /api/music/resolve-preview:", err);
    return res.status(500).json({ error: "Preview resolution failed" });
  }
});
function parseDurationSec(dur) {
  if (typeof dur === "number") return dur;
  if (!dur) return 30;
  const parts = String(dur).split(":");
  if (parts.length === 2) {
    return (parseInt(parts[0], 10) || 0) * 60 + (parseInt(parts[1], 10) || 0);
  }
  return parseInt(String(dur), 10) || 30;
}
async function fetchAlbumTracks(artist, title) {
  const cacheKey = `${artist.toLowerCase().trim()}:::${title.toLowerCase().trim()}`;
  if (albumTracksCache.has(cacheKey)) {
    return albumTracksCache.get(cacheKey).tracks;
  }
  const artistList = cleanArtistVariants(artist);
  const titleList = cleanTitleVariants(title);
  for (const a of artistList.slice(0, 2)) {
    for (const v of titleList.slice(0, 3)) {
      try {
        const q = encodeURIComponent(`${a} ${v}`);
        const itRes = await fetch(`https://itunes.apple.com/search?term=${q}&media=music&entity=album&limit=6`, {
          signal: AbortSignal.timeout(3500)
        });
        if (itRes.ok) {
          const itData = await itRes.json();
          if (itData.results && Array.isArray(itData.results)) {
            const albumMatch = itData.results.find(
              (r) => isArtistMatch(artist, r.artistName) && isAlbumMatch(v, r.collectionName)
            );
            if (albumMatch && albumMatch.collectionId) {
              const songRes = await fetch(`https://itunes.apple.com/lookup?id=${albumMatch.collectionId}&entity=song`, {
                signal: AbortSignal.timeout(4e3)
              });
              if (songRes.ok) {
                const sData = await songRes.json();
                const songItems = (sData.results || []).filter((r) => r.wrapperType === "track");
                if (songItems.length > 0) {
                  const tracks = songItems.map((s, idx) => ({
                    id: String(s.trackId || idx + 1),
                    position: s.trackNumber || idx + 1,
                    title: s.trackName || `Traccia ${idx + 1}`,
                    duration: s.trackTimeMillis ? Math.round(s.trackTimeMillis / 1e3) : 30,
                    previewAudioUrl: s.previewUrl || void 0
                  }));
                  albumTracksCache.set(cacheKey, { tracks, albumTitle: albumMatch.collectionName });
                  return tracks;
                }
              }
            }
          }
        }
      } catch (e) {
      }
    }
  }
  for (const a of artistList.slice(0, 2)) {
    for (const v of titleList.slice(0, 3)) {
      try {
        const q = encodeURIComponent(`artist:"${a}" album:"${v}"`);
        let res = await fetch(`https://api.deezer.com/search/album?q=${q}&limit=5`, {
          signal: AbortSignal.timeout(3500)
        });
        let data = await res.json();
        let album = (data.data || []).find(
          (alb) => isArtistMatch(artist, alb.artist?.name) && isAlbumMatch(v, alb.title)
        );
        if (!album?.id) {
          const qBroad = encodeURIComponent(`${a} ${v}`);
          res = await fetch(`https://api.deezer.com/search/album?q=${qBroad}&limit=5`, {
            signal: AbortSignal.timeout(3500)
          });
          data = await res.json();
          album = (data.data || []).find(
            (alb) => isArtistMatch(artist, alb.artist?.name) && isAlbumMatch(v, alb.title)
          );
        }
        if (album?.id) {
          const tRes = await fetch(`https://api.deezer.com/album/${album.id}/tracks?limit=50`, {
            signal: AbortSignal.timeout(4e3)
          });
          const tData = await tRes.json();
          if (tData.data && Array.isArray(tData.data) && tData.data.length > 0) {
            const tracks = tData.data.map((t, idx) => ({
              id: String(t.id || idx + 1),
              position: t.track_position || idx + 1,
              title: t.title || `Traccia ${idx + 1}`,
              duration: t.duration || 30,
              previewAudioUrl: t.preview ? `/api/music/stream-preview?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(t.title)}&url=${encodeURIComponent(t.preview)}` : void 0
            }));
            albumTracksCache.set(cacheKey, { tracks, albumTitle: album.title });
            return tracks;
          }
        }
      } catch (e) {
      }
    }
  }
  try {
    const discogsCandidates = await queryDiscogs(artist, title);
    const releaseWithId = discogsCandidates.find((c) => c.discogsId);
    if (releaseWithId && releaseWithId.discogsId) {
      const headers = getDiscogsHeaders();
      const dRes = await fetch(`https://api.discogs.com/releases/${releaseWithId.discogsId}`, {
        headers,
        signal: AbortSignal.timeout(4e3)
      });
      if (dRes.ok) {
        const dData = await dRes.json();
        if (dData.tracklist && Array.isArray(dData.tracklist)) {
          const validTracks = dData.tracklist.filter((t) => t.type_ !== "heading" && t.title);
          if (validTracks.length > 0) {
            const tracks = validTracks.map((t, idx) => ({
              id: `discogs-${releaseWithId.discogsId}-${idx + 1}`,
              position: t.position || String(idx + 1),
              title: t.title,
              duration: parseDurationSec(t.duration),
              previewAudioUrl: `/api/music/stream-preview?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(t.title)}`
            }));
            albumTracksCache.set(cacheKey, { tracks, albumTitle: dData.title });
            return tracks;
          }
        }
      }
    }
  } catch (e) {
  }
  return [];
}
app.get("/api/music/album-tracks", async (req, res) => {
  const artist = String(req.query.artist || "").trim();
  const title = String(req.query.title || "").trim();
  if (!artist && !title) {
    return res.status(400).json({ error: "artist or title is required" });
  }
  try {
    const tracks = await fetchAlbumTracks(artist, title);
    return res.json({ tracks, count: tracks.length });
  } catch (err) {
    console.error("Error in /api/music/album-tracks:", err);
    return res.status(500).json({ error: "Failed to fetch album tracks" });
  }
});
app.get("/api/music/stream-preview", async (req, res) => {
  const artist = String(req.query.artist || "").trim();
  const title = String(req.query.title || "").trim();
  let audioUrl = String(req.query.url || "").trim();
  try {
    if (!audioUrl || audioUrl.includes("dzcdn.net")) {
      const resolved = await resolveAudioStreamUrl(artist, title, audioUrl);
      if (resolved && resolved.url) {
        audioUrl = resolved.url;
      }
    }
    if (!audioUrl) {
      return res.status(404).send("Audio preview not found");
    }
    const clientRange = req.headers.range;
    const fetchHeaders = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    };
    if (clientRange) {
      fetchHeaders["Range"] = clientRange;
    }
    let upstream = await fetch(audioUrl, { headers: fetchHeaders });
    if (upstream.status === 403 && (artist || title)) {
      const cacheKey = `${artist.toLowerCase().trim()}:::${title.toLowerCase().trim()}`;
      audioStreamCache.delete(cacheKey);
      const fresh = await resolveAudioStreamUrl(artist, title);
      if (fresh && fresh.url && fresh.url !== audioUrl) {
        audioUrl = fresh.url;
        upstream = await fetch(audioUrl, { headers: fetchHeaders });
      }
    }
    if (!upstream.ok) {
      return res.status(upstream.status).send(`Upstream audio error: ${upstream.status}`);
    }
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Range, Content-Type, Accept");
    res.setHeader("Access-Control-Expose-Headers", "Content-Range, Content-Length, Accept-Ranges, Content-Type");
    res.setHeader("Accept-Ranges", "bytes");
    res.setHeader("Cache-Control", "public, max-age=86400");
    let contentType = upstream.headers.get("content-type") || "";
    if (!contentType || contentType.includes("octet-stream") || contentType.includes("text/")) {
      contentType = audioUrl.includes(".m4a") || audioUrl.includes("aac") ? "audio/mp4" : "audio/mpeg";
    }
    if (contentType === "audio/x-m4a") {
      contentType = "audio/mp4";
    }
    res.setHeader("Content-Type", contentType);
    const contentLength = upstream.headers.get("content-length");
    if (contentLength) {
      res.setHeader("Content-Length", contentLength);
    }
    const contentRange = upstream.headers.get("content-range");
    if (contentRange) {
      res.setHeader("Content-Range", contentRange);
    }
    res.status(upstream.status);
    if (upstream.body) {
      const nodeStream = import_stream.Readable.fromWeb(upstream.body);
      nodeStream.on("error", () => {
        if (!res.headersSent) res.status(500).end();
      });
      nodeStream.pipe(res);
    } else {
      const buf = await upstream.arrayBuffer();
      res.end(Buffer.from(buf));
    }
  } catch (err) {
    console.error("Audio stream proxy error:", err);
    if (!res.headersSent) {
      res.status(500).send("Audio stream error");
    }
  }
});
app.post("/api/music/batch", async (req, res) => {
  const { items } = req.body || {};
  if (!Array.isArray(items)) {
    return res.status(400).json({ error: "Expected items array" });
  }
  const results = {};
  const CHUNK_SIZE = 4;
  for (let i = 0; i < items.length; i += CHUNK_SIZE) {
    const chunk = items.slice(i, i + CHUNK_SIZE);
    await Promise.all(
      chunk.map(async (item) => {
        if (!item.artist && !item.title) return;
        const cacheKey = `${item.artist.toLowerCase()}:::${item.title.toLowerCase()}`;
        if (serverCache.has(cacheKey)) {
          results[item.id] = serverCache.get(cacheKey);
          return;
        }
        try {
          const [itunes, deezer] = await Promise.all([
            queryiTunes(item.artist, item.title),
            queryDeezer(item.artist, item.title)
          ]);
          const candidates = [...itunes, ...deezer];
          if (candidates.length > 0) {
            const best = candidates.find((c) => c.previewAudioUrl) || candidates[0];
            const data = {
              found: true,
              coverUrl: best.coverUrl,
              previewAudioUrl: best.previewAudioUrl,
              previewTrackTitle: best.previewTrackTitle || best.title
            };
            serverCache.set(cacheKey, data);
            results[item.id] = data;
          }
        } catch (e) {
        }
      })
    );
  }
  return res.json({ results });
});
app.get("/api/discogs/status", (req, res) => {
  const hasToken = Boolean(process.env.DISCOGS_TOKEN && process.env.DISCOGS_TOKEN.trim());
  return res.json({
    connected: true,
    hasToken,
    message: hasToken ? "Autenticazione Discogs attiva con Personal Access Token" : "Modalit\xE0 Discogs pubblica attiva (limite 25 rich./min)"
  });
});
app.get("/api/discogs/search", async (req, res) => {
  const rawQ = String(req.query.q || "").trim();
  const rawArtist = String(req.query.artist || "").trim();
  const rawTitle = String(req.query.title || req.query.release_title || "").trim();
  const rawBarcode = String(req.query.barcode || "").trim();
  const rawCatno = String(req.query.catno || "").trim();
  const formatFilter = String(req.query.format || "").trim().toLowerCase();
  const page = Math.max(1, parseInt(String(req.query.page || "1"), 10) || 1);
  const perPage = Math.min(50, Math.max(1, parseInt(String(req.query.per_page || "20"), 10) || 20));
  if (!rawQ && !rawArtist && !rawTitle && !rawBarcode && !rawCatno) {
    return res.status(400).json({ error: "Specificare almeno un termine di ricerca" });
  }
  const cArtist = cleanArtistVariants(rawArtist)[0] || rawArtist;
  const cTitle = cleanTitleVariants(rawTitle)[0] || rawTitle;
  const cacheKey = `discogs_search_v2:${rawQ}:${cArtist}:${cTitle}:${rawBarcode}:${rawCatno}:${formatFilter}:${page}:${perPage}`;
  if (serverCache.has(cacheKey)) {
    return res.json(serverCache.get(cacheKey));
  }
  try {
    const headers = getDiscogsHeaders();
    const performDiscogsQuery = async (params) => {
      params.set("type", "release");
      params.set("page", String(page));
      params.set("per_page", String(perPage));
      if (formatFilter && formatFilter !== "all") {
        if (formatFilter === "vinyl" || formatFilter === "lp") params.set("format", "Vinyl");
        else if (formatFilter === "cd") params.set("format", "CD");
        else if (formatFilter === '7"') params.set("format", '7"');
        else params.set("format", formatFilter);
      }
      const resp = await fetch(`https://api.discogs.com/database/search?${params.toString()}`, {
        headers,
        signal: AbortSignal.timeout(6500)
      });
      if (!resp.ok) {
        if (resp.status === 429) {
          throw new Error("RATE_LIMIT");
        }
        return null;
      }
      return await resp.json();
    };
    let searchParams = new URLSearchParams();
    if (rawBarcode) {
      searchParams.set("barcode", rawBarcode);
    } else if (rawCatno) {
      searchParams.set("catno", rawCatno);
    } else if (cArtist && cTitle) {
      searchParams.set("artist", cArtist);
      searchParams.set("release_title", cTitle);
    } else if (cArtist) {
      searchParams.set("artist", cArtist);
    } else if (cTitle) {
      searchParams.set("release_title", cTitle);
    } else if (rawQ) {
      searchParams.set("q", rawQ);
    }
    let data = null;
    try {
      data = await performDiscogsQuery(searchParams);
    } catch (e) {
      if (e.message === "RATE_LIMIT") {
        return res.status(429).json({ error: "Limite richieste Discogs raggiunto. Riprova tra poco o inserisci un DISCOGS_TOKEN." });
      }
    }
    let rawResults = data && data.results && Array.isArray(data.results) ? data.results : [];
    if (rawResults.length === 0 && (cArtist || cTitle || rawQ)) {
      const broadTerm = [cArtist, cTitle].filter(Boolean).join(" ").trim() || rawQ;
      if (broadTerm) {
        const fallbackParams = new URLSearchParams();
        fallbackParams.set("q", broadTerm);
        try {
          const fallbackData = await performDiscogsQuery(fallbackParams);
          if (fallbackData && fallbackData.results && Array.isArray(fallbackData.results)) {
            rawResults = fallbackData.results;
          }
        } catch (e) {
        }
      }
    }
    rawResults.sort((a, b) => {
      const aIsVinyl = Array.isArray(a.format) && a.format.some((f) => /vinyl|lp|12"|album/i.test(f));
      const bIsVinyl = Array.isArray(b.format) && b.format.some((f) => /vinyl|lp|12"|album/i.test(f));
      if (aIsVinyl && !bIsVinyl) return -1;
      if (!aIsVinyl && bIsVinyl) return 1;
      return 0;
    });
    const results = rawResults.map((item) => {
      let itemArtist = cArtist || "";
      let itemTitle = item.title || "";
      if (item.title && item.title.includes(" - ")) {
        const parts = item.title.split(" - ");
        itemArtist = parts[0].replace(/\s*\(\d+\)$/, "").trim();
        itemTitle = parts.slice(1).join(" - ").trim();
      }
      const rawCover = item.cover_image && !item.cover_image.includes("spacer.gif") ? item.cover_image : item.thumb;
      const coverUrl = rawCover ? `/api/music/cover-proxy?url=${encodeURIComponent(rawCover)}` : void 0;
      const formatArr = Array.isArray(item.format) ? item.format : item.format ? [item.format] : [];
      const formatStr = formatArr.join(", ") || "Vinyl, LP, Album";
      return {
        id: item.id,
        title: itemTitle,
        artist: itemArtist,
        rawTitle: item.title,
        year: item.year ? String(item.year) : void 0,
        format: formatStr,
        label: Array.isArray(item.label) ? item.label[0] : item.label,
        catalogNumber: item.catno,
        catno: item.catno,
        country: item.country,
        genre: Array.isArray(item.genre) ? item.genre.join(", ") : item.genre,
        style: Array.isArray(item.style) ? item.style.join(", ") : item.style,
        coverUrl,
        thumbUrl: item.thumb && !item.thumb.includes("spacer.gif") ? `/api/music/cover-proxy?url=${encodeURIComponent(item.thumb)}` : void 0,
        uri: item.uri ? `https://www.discogs.com${item.uri}` : `https://www.discogs.com/release/${item.id}`,
        barcode: Array.isArray(item.barcode) ? item.barcode : []
      };
    });
    const output = {
      pagination: data && data.pagination ? data.pagination : { page, pages: 1, items: results.length },
      results
    };
    serverCache.set(cacheKey, output);
    return res.json(output);
  } catch (err) {
    console.error("Discogs search error:", err);
    return res.status(500).json({ error: "Errore durante la ricerca su Discogs" });
  }
});
app.get("/api/discogs/release/:id", async (req, res) => {
  const releaseId = String(req.params.id || "").trim();
  if (!releaseId) {
    return res.status(400).json({ error: "ID rilascio Discogs mancante" });
  }
  const cacheKey = `discogs_rel_v2:${releaseId}`;
  if (serverCache.has(cacheKey)) {
    return res.json(serverCache.get(cacheKey));
  }
  try {
    const headers = getDiscogsHeaders();
    const upstreamRes = await fetch(`https://api.discogs.com/releases/${releaseId}`, {
      headers,
      signal: AbortSignal.timeout(6500)
    });
    if (!upstreamRes.ok) {
      return res.status(upstreamRes.status).json({ error: `Rilascio Discogs non trovato (${upstreamRes.status})` });
    }
    const data = await upstreamRes.json();
    const artistName = (data.artists || []).map((a) => a.name.replace(/\s*\(\d+\)$/, "")).join(", ") || "Artista Sconosciuto";
    const primaryImage = (data.images || []).find((img) => img.type === "primary") || data.images?.[0];
    const rawCover = primaryImage?.resource_url || primaryImage?.uri || data.thumb;
    const coverUrl = rawCover ? `/api/music/cover-proxy?url=${encodeURIComponent(rawCover)}` : void 0;
    const labelName = data.labels?.[0]?.name || "";
    const catno = data.labels?.[0]?.catno || "";
    const formatStr = (data.formats || []).map((f) => [f.name, ...f.descriptions || []].filter(Boolean).join(", ")).join("; ") || "Vinyl, LP";
    const tracks = (data.tracklist || []).filter((t) => t.type_ !== "heading" && t.title).map((t, idx) => ({
      id: `discogs-${data.id}-${idx + 1}`,
      position: t.position || String(idx + 1),
      title: t.title,
      duration: parseDurationSec(t.duration)
    }));
    let previewAudioUrl;
    let previewTrackTitle;
    try {
      const itunesCandidates = await queryiTunes(artistName, data.title);
      const withAudio = itunesCandidates.find((c) => c.previewAudioUrl);
      if (withAudio && withAudio.previewAudioUrl) {
        previewAudioUrl = withAudio.previewAudioUrl;
        previewTrackTitle = withAudio.previewTrackTitle || withAudio.title;
      }
    } catch (e) {
    }
    const result = {
      id: data.id,
      title: data.title,
      artist: artistName,
      label: labelName,
      catalogNumber: catno,
      releasedYear: data.year ? String(data.year) : data.released ? String(data.released).substring(0, 4) : "",
      releasedDate: data.released || void 0,
      country: data.country || void 0,
      genre: (data.genres || []).join(", "),
      styles: (data.styles || []).join(", "),
      format: formatStr,
      coverUrl,
      previewAudioUrl,
      previewTrackTitle,
      images: (data.images || []).slice(0, 6).map((img) => ({
        uri: `/api/music/cover-proxy?url=${encodeURIComponent(img.resource_url || img.uri)}`,
        type: img.type
      })),
      tracks,
      uri: data.uri || `https://www.discogs.com/release/${data.id}`,
      notes: data.notes || void 0,
      lowestPrice: data.lowest_price ? `${data.lowest_price} \u20AC` : void 0,
      numForSale: data.num_for_sale || void 0
    };
    serverCache.set(cacheKey, result);
    return res.json(result);
  } catch (err) {
    console.error(`Discogs release fetch error for ${releaseId}:`, err);
    return res.status(500).json({ error: "Errore durante il recupero del rilascio da Discogs" });
  }
});
app.get("/api/metadata/status", (req, res) => {
  try {
    const statuses = metadataManager.getProvidersStatus();
    return res.json({ providers: statuses });
  } catch (err) {
    return res.status(500).json({ error: "Impossibile recuperare lo stato dei provider" });
  }
});
app.get("/api/metadata/search", async (req, res) => {
  try {
    const { artist, title, query, barcode, catno, format } = req.query;
    const results = await metadataManager.searchAlbums({
      artist: typeof artist === "string" ? artist : void 0,
      title: typeof title === "string" ? title : void 0,
      query: typeof query === "string" ? query : void 0,
      barcode: typeof barcode === "string" ? barcode : void 0,
      catalogNumber: typeof catno === "string" ? catno : void 0,
      format: typeof format === "string" ? format : void 0
    });
    return res.json({ results });
  } catch (err) {
    console.error("Metadata search failed:", err);
    return res.status(500).json({ error: "Errore durante la ricerca metadati multi-provider" });
  }
});
app.get("/api/metadata/album", async (req, res) => {
  try {
    const { id, mbid, source } = req.query;
    if (!id && !mbid) {
      return res.status(400).json({ error: "Parametro id o mbid obbligatorio" });
    }
    const album = await metadataManager.getAlbumDetails(
      typeof id === "string" ? id : "",
      typeof source === "string" ? source : void 0,
      typeof mbid === "string" ? mbid : void 0
    );
    if (!album) {
      return res.status(404).json({ error: "Dettagli album non trovati" });
    }
    return res.json({ album });
  } catch (err) {
    console.error("Metadata album detail failed:", err);
    return res.status(500).json({ error: "Errore durante il recupero dei dettagli album" });
  }
});
app.get("/api/metadata/covers", async (req, res) => {
  try {
    const { artist, title, mbid } = req.query;
    const covers = await metadataManager.getCovers(
      {
        artist: typeof artist === "string" ? artist : void 0,
        title: typeof title === "string" ? title : void 0
      },
      typeof mbid === "string" ? mbid : void 0
    );
    return res.json({ covers });
  } catch (err) {
    console.error("Metadata covers fetch failed:", err);
    return res.status(500).json({ error: "Errore durante il recupero delle copertine multi-sorgente" });
  }
});
app.get("/api/metadata/artist-bio", async (req, res) => {
  try {
    const { artist } = req.query;
    if (!artist || typeof artist !== "string") {
      return res.status(400).json({ error: "Parametro artist obbligatorio" });
    }
    const bio = await metadataManager.getArtistBio(artist);
    if (!bio) {
      return res.status(404).json({ error: "Biografia non trovata" });
    }
    return res.json({ bio });
  } catch (err) {
    console.error("Metadata artist bio fetch failed:", err);
    return res.status(500).json({ error: "Errore durante il recupero della biografia artista" });
  }
});
app.get("/api/metadata/lyrics", async (req, res) => {
  try {
    const { artist, title, album } = req.query;
    if (!artist || !title || typeof artist !== "string" || typeof title !== "string") {
      return res.status(400).json({ error: "Parametri artist e title obbligatori" });
    }
    const lyrics = await metadataManager.getLyrics(
      artist,
      title,
      typeof album === "string" ? album : void 0
    );
    if (!lyrics) {
      return res.status(404).json({ error: "Testo non trovato per questo brano" });
    }
    return res.json({ lyrics });
  } catch (err) {
    console.error("Metadata lyrics fetch failed:", err);
    return res.status(500).json({ error: "Errore durante il recupero dei testi" });
  }
});
function findFpcalcExecutable() {
  const resPath = process.env.RESOURCES_PATH || process.resourcesPath;
  const possiblePaths = [];
  if (resPath) {
    possiblePaths.push(
      import_path.default.join(resPath, "bin", "fpcalc.exe"),
      import_path.default.join(resPath, "app.asar.unpacked", "bin", "fpcalc.exe"),
      import_path.default.join(resPath, "fpcalc.exe"),
      import_path.default.join(resPath, "app.asar.unpacked", "fpcalc.exe")
    );
  }
  possiblePaths.push(
    import_path.default.join(process.cwd(), "bin", "fpcalc.exe"),
    import_path.default.join(process.cwd(), "fpcalc.exe"),
    import_path.default.join(__dirname, "..", "bin", "fpcalc.exe"),
    import_path.default.join(__dirname, "bin", "fpcalc.exe")
  );
  for (const p of possiblePaths) {
    if (import_fs.default.existsSync(p) && !p.includes("app.asar\\") && !p.includes("app.asar/")) {
      return p;
    }
  }
  for (const p of possiblePaths) {
    if (import_fs.default.existsSync(p)) {
      const unpacked = p.replace("app.asar", "app.asar.unpacked");
      if (import_fs.default.existsSync(unpacked)) {
        return unpacked;
      }
      try {
        const tempDir = import_path.default.join(import_os.default.tmpdir(), "viniloteca_bin");
        if (!import_fs.default.existsSync(tempDir)) import_fs.default.mkdirSync(tempDir, { recursive: true });
        const tempExe = import_path.default.join(tempDir, "fpcalc.exe");
        if (!import_fs.default.existsSync(tempExe) || import_fs.default.statSync(tempExe).size === 0) {
          import_fs.default.writeFileSync(tempExe, import_fs.default.readFileSync(p));
        }
        return tempExe;
      } catch {
        return p;
      }
    }
  }
  try {
    const cmd = process.platform === "win32" ? "where fpcalc" : "which fpcalc";
    const out = (0, import_child_process.execSync)(cmd, { stdio: ["pipe", "pipe", "ignore"] }).toString().trim();
    if (out) {
      const first = out.split(/\r?\n/)[0].trim();
      if (first && import_fs.default.existsSync(first)) return first;
    }
  } catch {
  }
  return null;
}
app.get("/api/metadata/fpcalc-status", (req, res) => {
  const exePath = findFpcalcExecutable();
  return res.json({
    available: Boolean(exePath && import_fs.default.existsSync(exePath)),
    path: exePath,
    expectedPath: "bin/fpcalc.exe"
  });
});
app.post("/api/metadata/fpcalc", async (req, res) => {
  try {
    const { fileName, fileBase64 } = req.body;
    if (!fileBase64) {
      return res.status(400).json({ error: "fileBase64 richiesto per il calcolo dell'impronta audio" });
    }
    const fpcalcPath = findFpcalcExecutable();
    if (!fpcalcPath || !import_fs.default.existsSync(fpcalcPath)) {
      return res.status(500).json({
        error: "Eseguibile fpcalc.exe non trovato nel sistema. Assicurati che sia presente nella cartella bin/"
      });
    }
    const tempDir = import_path.default.join(import_os.default.tmpdir(), "viniloteca_audio");
    if (!import_fs.default.existsSync(tempDir)) import_fs.default.mkdirSync(tempDir, { recursive: true });
    const ext = import_path.default.extname(fileName || "audio.mp3") || ".mp3";
    const tempAudioFile = import_path.default.join(
      tempDir,
      `audio_${Date.now()}_${Math.random().toString(36).substring(2, 6)}${ext}`
    );
    const buffer = Buffer.from(fileBase64, "base64");
    import_fs.default.writeFileSync(tempAudioFile, buffer);
    (0, import_child_process.execFile)(fpcalcPath, ["-json", tempAudioFile], { timeout: 15e3 }, (error, stdout, stderr) => {
      try {
        if (import_fs.default.existsSync(tempAudioFile)) import_fs.default.unlinkSync(tempAudioFile);
      } catch {
      }
      if (error) {
        console.error("fpcalc execution error:", error, stderr);
        return res.status(500).json({ error: `Errore calcolo impronta fpcalc: ${error.message}` });
      }
      try {
        const data = JSON.parse(stdout);
        return res.json({
          duration: data.duration,
          fingerprint: data.fingerprint
        });
      } catch (parseErr) {
        return res.status(500).json({ error: "Risposta fpcalc non valida" });
      }
    });
  } catch (err) {
    console.error("Error in /api/metadata/fpcalc:", err);
    return res.status(500).json({ error: err.message || "Errore elaborazione file audio" });
  }
});
app.post("/api/metadata/fingerprint", async (req, res) => {
  try {
    const { duration, fingerprint } = req.body;
    if (!duration || !fingerprint) {
      return res.status(400).json({ error: "Parametri duration e fingerprint obbligatori nel body JSON" });
    }
    const matches = await metadataManager.identifyAudio(Number(duration), String(fingerprint));
    return res.json({ matches });
  } catch (err) {
    console.error("Metadata fingerprint recognition failed:", err);
    return res.status(500).json({
      error: err?.message || "Errore durante il riconoscimento audio AcoustID"
    });
  }
});
app.get("/api/metadata/aggregate", async (req, res) => {
  try {
    const { artist, title } = req.query;
    if (!artist || !title || typeof artist !== "string" || typeof title !== "string") {
      return res.status(400).json({ error: "Parametri artist e title obbligatori" });
    }
    const aggregated = await metadataManager.getAggregatedAlbum({ artist, title });
    if (!aggregated) {
      return res.status(404).json({ error: "Nessun dato aggregato trovato" });
    }
    return res.json(aggregated);
  } catch (err) {
    console.error("Metadata aggregation failed:", err);
    return res.status(500).json({ error: "Errore durante l'aggregazione metadati" });
  }
});
function getStorageFilePath() {
  const baseDir = process.env.APPDATA ? import_path.default.join(process.env.APPDATA, "Viniloteca") : import_path.default.join(process.cwd(), "data");
  if (!import_fs.default.existsSync(baseDir)) {
    try {
      import_fs.default.mkdirSync(baseDir, { recursive: true });
    } catch (e) {
      console.warn("[Storage] Impossibile creare la cartella dati:", e);
    }
  }
  return import_path.default.join(baseDir, "collection.json");
}
app.get("/api/records", (req, res) => {
  try {
    const filePath = getStorageFilePath();
    if (import_fs.default.existsSync(filePath)) {
      const raw = import_fs.default.readFileSync(filePath, "utf-8");
      if (raw.trim()) {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          return res.json({ records: parsed, count: parsed.length, path: filePath });
        }
      }
    }
    return res.json({ records: null, count: 0 });
  } catch (err) {
    console.error("[Storage] Errore lettura collezione da disco:", err);
    return res.status(500).json({ error: "Errore durante la lettura dei dischi da disco" });
  }
});
app.post("/api/records", (req, res) => {
  try {
    const { records } = req.body;
    if (!Array.isArray(records)) {
      return res.status(400).json({ error: 'Formato dati non valido: array "records" richiesto' });
    }
    const filePath = getStorageFilePath();
    const tempPath = `${filePath}.tmp`;
    import_fs.default.writeFileSync(tempPath, JSON.stringify(records, null, 2), "utf-8");
    if (import_fs.default.existsSync(tempPath)) {
      import_fs.default.renameSync(tempPath, filePath);
    }
    return res.json({ success: true, count: records.length, path: filePath });
  } catch (err) {
    console.error("[Storage] Errore salvataggio collezione su disco:", err);
    return res.status(500).json({ error: "Errore durante il salvataggio dei dischi su disco" });
  }
});
var activeServer = null;
var activePort = PORT;
var isMiddlewaresConfigured = false;
async function setupServerMiddlewares() {
  if (isMiddlewaresConfigured) return;
  isMiddlewaresConfigured = true;
  if (process.env.NODE_ENV !== "production") {
    const { createServer: createViteServer } = await import("vite");
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa"
    });
    app.use(vite.middlewares);
  } else {
    const distPath = process.env.STATIC_DIST_PATH || (import_fs.default.existsSync(import_path.default.join(__dirname, "index.html")) ? __dirname : import_path.default.join(process.cwd(), "dist"));
    app.use(import_express.default.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(import_path.default.join(distPath, "index.html"));
    });
  }
}
async function startServer(preferredPort = PORT) {
  if (activeServer) {
    return { server: activeServer, port: activePort };
  }
  await setupServerMiddlewares();
  const maxAttempts = 10;
  let portToTry = preferredPort;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const server = await new Promise((resolve, reject) => {
        const s = import_http.default.createServer(app);
        s.once("error", (err) => {
          reject(err);
        });
        s.listen(portToTry, "0.0.0.0", () => {
          resolve(s);
        });
      });
      activeServer = server;
      activePort = portToTry;
      console.log(`Server running on http://localhost:${activePort}`);
      return { server: activeServer, port: activePort };
    } catch (err) {
      if (err.code === "EADDRINUSE" && attempt < maxAttempts - 1) {
        console.warn(`[Server] Porta ${portToTry} gi\xE0 occupata, tentativo con porta ${portToTry + 1}...`);
        portToTry++;
      } else {
        throw err;
      }
    }
  }
  throw new Error(`Impossibile avviare il server: tutte le porte da ${preferredPort} a ${portToTry} sono occupate.`);
}
if (process.env.AUTO_START_SERVER !== "false") {
  startServer(PORT).catch((err) => {
    console.error("[Server] Errore durante l'avvio automatico:", err);
  });
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  app,
  server,
  startServer
});
//# sourceMappingURL=server.cjs.map
