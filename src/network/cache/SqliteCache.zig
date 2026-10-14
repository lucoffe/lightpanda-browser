// Copyright (C) 2023-2026  Lightpanda (Selecy SAS)
//
// Francis Bouvier <francis@lightpanda.io>
// Pierre Tachoire <pierre@lightpanda.io>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

const std = @import("std");
const lp = @import("lightpanda");

const Cache = @import("Cache.zig");

const log = lp.log;
const CacheGetRequest = Cache.CacheGetRequest;
const RenewResponse = Cache.RenewResponse;
const CachePutRequest = Cache.CachePutRequest;
const CacheGetResult = Cache.CacheGetResult;
const CachedResponse = Cache.CachedResponse;

const Http = @import("../http.zig");
const Blob = @import("../../storage/sqlite/Sqlite.zig").Blob;
const Pool = @import("../../storage/sqlite/Pool.zig");
const Migration = @import("../../storage/sqlite/Sqlite.zig").Migration;
const Migrations = @import("../../storage/sqlite/Sqlite.zig").Migrations;

pub const SqliteCache = @This();

allocator: std.mem.Allocator,
pool: Pool,
options: Options,

pub const Options = struct {
    /// Entries kept, the oldest stored evicted first (--http-cache-entry-limit); 0 = no limit.
    entry_limit: u32 = 0,
    /// Total bytes of the entries, bodies and headers (--http-cache-size-limit); 0 = no limit.
    size_limit: u64 = 0,
    /// Bytes of one entry, body and headers (--http-cache-entry-size-limit); 0 = no limit.
    entry_size_limit: u64 = 0,
    /// SQLite connections shared by the browser sessions (--http-cache-connections).
    connections: u8 = Pool.default_connections,
    /// SQLite's busy timeout, and how long a lookup or a store waits for a
    /// free pooled connection, ms. Past it the lookup is a miss and the store
    /// is skipped (error.PoolBusy / error.Busy).
    busy_timeout_ms: u32 = Pool.default_busy_timeout_ms,
};

/// What a failed lookup, store or renewal counts as in the
/// http_cache_failure metric.
pub const Failure = enum { busy, @"error" };

pub fn failureOf(err: anyerror) Failure {
    return switch (err) {
        error.PoolBusy,
        error.Busy,
        error.BusyRecovery,
        error.BusySnapshot,
        error.BusyTimeout,
        error.Locked,
        error.LockedSharedCache,
        error.LockedVTab,
        => .busy,
        else => .@"error",
    };
}

// A busy cache is expected under contention: a debug line, not an error.
fn logFailure(comptime msg: []const u8, err: anyerror, url: ?[]const u8) void {
    switch (failureOf(err)) {
        .busy => log.debug(.cache, msg, .{ .url = url, .err = err }),
        .@"error" => log.err(.cache, msg, .{ .url = url, .err = err }),
    }
}

const cache_migrations: []const Migration = &.{
    .{ .sql =
    \\ create table cache (
    \\      url               text not null primary key,
    \\      status            integer not null,
    \\      stored_at         integer not null,
    \\      age_at_store      integer not null,
    \\      max_age           integer not null,
    \\      must_revalidate   integer not null,
    \\      content_type      text not null,
    \\      etag              text,
    \\      last_modified     text,
    \\      body              blob not null
    \\ ) strict
    },
    .{ .sql =
    \\ create table header (
    \\      url               text not null,
    \\      name              text not null,
    \\      value             blob not null,
    \\      vary              integer not null,
    \\      foreign key (url) references cache(url) on delete cascade
    \\ ) strict
    },
    .{ .sql = "create index header_url on header(url)" },
    // Replaced by cache_evict, which leads with stored_at.
    .{ .sql = "create index cache_stored_at on cache(stored_at)" },
    .{ .sql =
    \\ create index cache_expiry on cache(stored_at + max_age - age_at_store)
    \\     where etag is null and last_modified is null
    },
    // Body and header bytes of an entry, for --http-cache-size-limit. The
    // column lands after the body, so without cache_evict, which covers
    // every column evict_over_size_sql reads, size eviction would read every
    // entry's body. Built in the same step so an upgraded cache is scanned
    // once; entries stored before it count as 0 bytes until they are
    // replaced or renewed. cache_evict leads with stored_at, so it also
    // serves the count eviction, and cache_stored_at goes.
    .{ .sql =
    \\ alter table cache add column size integer not null default 0;
    \\ create index cache_evict on cache(stored_at, size, max_age, age_at_store);
    \\ drop index cache_stored_at;
    },
};

// Don't change this without also updating the cache_expiry index.
const purge_expired_sql =
    \\ delete from cache
    \\ where etag is null and last_modified is null
    \\     and stored_at + max_age - age_at_store <= $1
;

// Evicts the oldest stored entries past the newest $1.
const evict_over_count_sql =
    \\ delete from cache
    \\ where rowid in (
    \\     select rowid from cache
    \\     order by stored_at desc
    \\     limit -1 offset $1
    \\ )
;

// Don't change the columns this reads without also updating the cache_evict
// index. Evicts, expired entries first and then the oldest stored, until the
// total size is within $2. The running sum includes the row itself, so a row
// goes while the bytes freed before it are short of the excess. "Expired"
// here is past max-age only: a must-revalidate entry, which every lookup
// revalidates, is not ranked first until its max-age has passed too.
const evict_over_size_sql =
    \\ delete from cache where rowid in (
    \\     select rowid from (
    \\         select rowid, size, sum(size) over (
    \\             order by (stored_at + max_age - age_at_store <= $1) desc, stored_at, rowid
    \\         ) as running
    \\         from cache
    \\     ) where running - size < (select total(size) from cache) - $2
    \\ )
;

// Index-only: under the limit, the size eviction's window query is skipped.
const total_size_sql = "select total(size) from cache";

const update_size_sql =
    \\ update cache set size = length(body) +
    \\     (select coalesce(sum(length(name) + length(value)), 0) from header where header.url = cache.url)
    \\ where url = $1
    \\ returning size
;

// The bytes put stores for an entry: body, and every header name and value,
// Vary rows included. update_size_sql counts the same.
fn entrySize(req: CachePutRequest, body: []const u8) u64 {
    var size: u64 = body.len;
    for (req.headers) |h| size += h.name.len + h.value.len;
    for (req.vary_headers) |h| size += h.name.len + h.value.len;
    return size;
}

const SqliteCachePath = union(enum) {
    path: []const u8,
    memory,

    pub fn format(
        self: SqliteCachePath,
        writer: *std.Io.Writer,
    ) std.Io.Writer.Error!void {
        const name = switch (self) {
            .memory => "memory",
            .path => |p| p,
        };
        try writer.writeAll(name);
    }
};

/// Fails with error.CacheUnavailable, logged here, when the cache's directory
/// cannot be made or its database cannot be opened (unwritable or missing
/// directory, a path that is a file, ...): Cache.init then runs without a cache.
pub fn init(allocator: std.mem.Allocator, path: SqliteCachePath, options: Options) !SqliteCache {
    const pool_options: Pool.Options = .{
        .connections = options.connections,
        .busy_timeout_ms = options.busy_timeout_ms,
        .acquire_timeout_ms = options.busy_timeout_ms,
    };
    var pool = switch (path) {
        .memory => try Pool.init(allocator, ":memory:", pool_options),
        .path => |cache_dir| blk: {
            std.Io.Dir.cwd().createDirPath(lp.io, cache_dir) catch |e| {
                log.warn(
                    .cache,
                    "failed to make path",
                    .{ .kind = "SqliteCache", .path = cache_dir, .err = e },
                );
                return error.CacheUnavailable;
            };

            const full_path = try allocator.printSentinel(
                "{s}/cache.db",
                .{std.mem.trimEnd(u8, cache_dir, &.{'/'})},
                0,
            );
            defer allocator.free(full_path);
            break :blk Pool.init(allocator, full_path, pool_options) catch |e| switch (e) {
                error.OutOfMemory => return e,
                else => {
                    log.warn(.cache, "failed to open", .{ .kind = "SqliteCache", .path = full_path, .err = e });
                    return error.CacheUnavailable;
                },
            };
        },
    };
    errdefer pool.deinit(allocator);

    var version: usize = 0;

    {
        const conn = try pool.acquire();
        defer pool.release(conn);

        try conn.exec("pragma journal_mode=wal", .{});
        version = try Migrations.run(conn, cache_migrations);
    }

    for (pool.conns) |conn| {
        try conn.exec("pragma foreign_keys=on", .{});
    }

    var cache: SqliteCache = .{ .allocator = allocator, .pool = pool, .options = options };
    cache.maintenance(lp.datetime.timestamp(.real));

    log.info(.cache, "sqlite cache initialized", .{
        .path = path,
        .entry_limit = options.entry_limit,
        .size_limit = options.size_limit,
        .entry_size_limit = options.entry_size_limit,
        .connections = pool.conns.len,
        .version = version,
    });
    return cache;
}

pub fn deinit(self: *SqliteCache) void {
    self.pool.deinit(self.allocator);
}

/// Removes expired entries and enforces the entry and size limits.
pub fn maintenance(self: *SqliteCache, now: u64) void {
    const conn = self.pool.acquire() catch |err| {
        logFailure("maintenance", err, null);
        return;
    };
    defer self.pool.release(conn);

    conn.exec(purge_expired_sql, .{@as(i64, @intCast(now))}) catch |err| {
        logFailure("purge expired", err, null);
    };
    const purged = conn.changes();

    var evicted: i64 = 0;
    if (self.options.entry_limit > 0) {
        conn.exec(evict_over_count_sql, .{@as(i64, @intCast(self.options.entry_limit))}) catch |err| {
            logFailure("evict overflow", err, null);
        };
        evicted += conn.changes();
    }
    if (self.options.size_limit > 0) size: {
        const limit: i64 = @intCast(@min(self.options.size_limit, std.math.maxInt(i64)));
        const total = conn.scalar(f64, total_size_sql, .{}) catch |err| {
            logFailure("evict over size", err, null);
            break :size;
        } orelse 0;
        if (total <= @as(f64, @floatFromInt(limit))) break :size;
        conn.exec(evict_over_size_sql, .{ @as(i64, @intCast(now)), limit }) catch |err| {
            logFailure("evict over size", err, null);
            break :size;
        };
        evicted += conn.changes();
    }

    log.debug(.cache, "maintenance", .{ .purged = purged, .evicted = evicted });
}

pub fn get(self: *SqliteCache, arena: std.mem.Allocator, req: CacheGetRequest) !CacheGetResult {
    const conn = try self.pool.acquire();
    defer self.pool.release(conn);

    try conn.begin(.deferred);
    defer conn.rollback() catch {};

    var entry = try conn.row(
        \\ select status, stored_at, age_at_store,
        \\     max_age, must_revalidate, content_type, etag, last_modified, body
        \\ from cache where url = $1
    , .{req.url}) orelse {
        log.debug(.cache, "miss", .{ .url = req.url, .reason = "missing" });
        return .miss;
    };
    defer entry.deinit();

    const status: u16 = @intCast(entry.get(i64, 0));
    const stored_at: u64 = @intCast(entry.get(i64, 1));
    const age_at_store: u64 = @intCast(entry.get(i64, 2));
    const max_age: u64 = @intCast(entry.get(i64, 3));
    const must_revalidate = entry.get(bool, 4);
    const raw_content_type = entry.get([]const u8, 5);
    const raw_etag = entry.get(?[]const u8, 6);
    const raw_last_modified = entry.get(?[]const u8, 7);
    const raw_body = entry.get(Blob, 8);

    const expired = must_revalidate or blk: {
        const age = (req.timestamp - stored_at) + age_at_store;
        break :blk age >= @as(i64, @intCast(max_age));
    };
    const has_validators = raw_etag != null or raw_last_modified != null;

    // If it is expired without validators,
    // we are going to have to make a network request for this resource.
    if (expired and !has_validators) {
        log.debug(.cache, "miss", .{ .url = req.url, .reason = "expired with no validators" });
        return .stale;
    }

    var vary_rows = try conn.rows(
        "select name, value from header where url = $1 and vary = true",
        .{req.url},
    );
    defer vary_rows.deinit();

    while (try vary_rows.next()) |row| {
        const name = row.get([]const u8, 0);
        const value = row.get(Blob, 1).data;

        const incoming = for (req.request_headers) |rh| {
            if (std.ascii.eqlIgnoreCase(rh.name, name)) break rh.value;
        } else "";

        if (!std.ascii.eqlIgnoreCase(value, incoming)) {
            log.debug(.cache, "miss", .{
                .url = req.url,
                .reason = "vary mismatch",
                .header = name,
                .expected = value,
                .got = incoming,
            });
            return .miss;
        }
    }

    var header_rows = try conn.rows(
        "select name, value from header where url = $1 and vary = false",
        .{req.url},
    );
    defer header_rows.deinit();

    var headers: std.ArrayList(Http.Header) = .empty;

    while (try header_rows.next()) |row| {
        const name = try arena.dupe(u8, row.get([]const u8, 0));
        const value = try arena.dupe(u8, row.get(Blob, 1).data);
        try headers.append(arena, .{ .name = name, .value = value });
    }

    log.debug(.cache, "hit", .{ .url = req.url, .expired = expired });

    const resp: CachedResponse = .{
        .status = status,
        .content_type = try arena.dupe(u8, raw_content_type),
        .headers = headers.items,
        .etag = if (raw_etag) |v| try arena.dupe(u8, v) else null,
        .last_modified = if (raw_last_modified) |v| try arena.dupe(u8, v) else null,
        .data = .{ .buffer = try arena.dupe(u8, raw_body.data) },
    };
    return if (expired) .{ .revalidate = resp } else .{ .hit = resp };
}

pub fn put(self: *SqliteCache, req: CachePutRequest, body: []const u8) !void {
    const size = entrySize(req, body);
    if (self.options.entry_size_limit > 0 and size > self.options.entry_size_limit) {
        log.debug(.cache, "no store", .{ .url = req.url, .size = size, .reason = "too large" });
        return error.EntryTooLarge;
    }

    const conn = try self.pool.acquire();
    defer self.pool.release(conn);

    try conn.begin(.immediate);
    defer conn.rollback() catch {};

    try conn.exec("delete from cache where url = $1", .{req.url});

    try conn.exec(
        \\ insert into cache
        \\     (url, status, stored_at, age_at_store, max_age, must_revalidate,
        \\     content_type, etag, last_modified, body, size)
        \\ values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    , .{
        req.url,
        @as(i64, @intCast(req.status)),
        req.stored_at,
        @as(i64, @intCast(req.age_at_store)),
        @as(i64, @intCast(req.cache_control.max_age)),
        req.cache_control.must_revalidate,
        req.content_type,
        req.etag,
        req.last_modified,
        Blob{ .data = body },
        @as(i64, @intCast(size)),
    });

    var lower_name: [256]u8 = undefined;
    for (req.headers) |h| {
        if (h.name.len > lower_name.len) return error.HeaderNameTooLong;
        const name = std.ascii.lowerString(lower_name[0..h.name.len], h.name);
        try conn.exec(
            "insert into header (url, name, value, vary) values ($1, $2, $3, false)",
            .{ req.url, name, Blob{ .data = h.value } },
        );
    }
    for (req.vary_headers) |h| {
        if (h.name.len > lower_name.len) return error.HeaderNameTooLong;
        const name = std.ascii.lowerString(lower_name[0..h.name.len], h.name);
        try conn.exec(
            "insert into header (url, name, value, vary) values ($1, $2, $3, true)",
            .{ req.url, name, Blob{ .data = h.value } },
        );
    }

    try conn.commit();

    log.debug(.cache, "put", .{ .url = req.url, .body_len = body.len });
}

pub fn clear(self: *SqliteCache) !void {
    const conn = try self.pool.acquire();
    defer self.pool.release(conn);

    try conn.exec("delete from cache", .{});
    log.debug(.cache, "clear", .{});
}

pub fn evict(self: *SqliteCache, url: []const u8) void {
    const conn = self.pool.acquire() catch |err| {
        logFailure("evict", err, url);
        return;
    };
    defer self.pool.release(conn);

    conn.exec("delete from cache where url = $1", .{url}) catch |err| {
        logFailure("evict", err, url);
        return;
    };

    log.debug(.cache, "evict", .{ .url = url });
}

pub fn renew(self: *SqliteCache, _: std.mem.Allocator, req: RenewResponse) !void {
    const conn = try self.pool.acquire();
    defer self.pool.release(conn);

    try conn.begin(.immediate);
    defer conn.rollback() catch {};

    const directive = req.directive();

    try conn.exec(
        \\ update cache
        \\ set stored_at = $1,
        \\     age_at_store = $2,
        \\     max_age = coalesce($3, max_age),
        \\     must_revalidate = coalesce($4, must_revalidate),
        \\     content_type = coalesce($5, content_type),
        \\     etag = coalesce($6, etag),
        \\     last_modified = coalesce($7, last_modified)
        \\ where url = $8
    , .{
        directive.timestamp,
        directive.age_at_store,
        directive.max_age,
        directive.must_revalidate,
        directive.content_type,
        directive.etag,
        directive.last_modified,
        req.url,
    });

    const affected = conn.changes();
    if (affected == 0) {
        log.debug(.cache, "miss", .{ .url = req.url, .reason = "missing" });
        return error.CacheEntryNotFound;
    }

    // Clear old non-Vary headers.
    try conn.exec("delete from header where url = $1 and vary = false", .{req.url});

    var lower_name: [256]u8 = undefined;
    for (req.headers) |h| {
        if (h.name.len > lower_name.len) return error.HeaderNameTooLong;
        const name = std.ascii.lowerString(lower_name[0..h.name.len], h.name);
        try conn.exec(
            "insert into header (url, name, value, vary) values ($1, $2, $3, false)",
            .{ req.url, name, Blob{ .data = h.value } },
        );
    }

    const size = (try conn.scalar(i64, update_size_sql, .{req.url})) orelse 0;

    // Longer headers can take the entry past the limit: it goes, as a put
    // of that size would not have been stored.
    if (self.options.entry_size_limit > 0) {
        if (@as(u64, @intCast(size)) > self.options.entry_size_limit) {
            try conn.exec("delete from cache where url = $1", .{req.url});
            try conn.commit();
            log.debug(.cache, "evict", .{ .url = req.url, .size = size, .reason = "too large" });
            return;
        }
    }

    try conn.commit();

    log.debug(.cache, "renewed", .{
        .url = req.url,
        .timestamp = req.timestamp,
    });
}

const testing = std.testing;

fn setupCache(allocator: std.mem.Allocator) !Cache {
    return Cache{ .kind = .{ .sqlite = try .init(allocator, .memory, .{}) } };
}

test "SqliteCache: failureOf" {
    for ([_]anyerror{ error.PoolBusy, error.Busy, error.BusyTimeout, error.BusySnapshot, error.Locked }) |e| {
        try testing.expectEqual(Failure.busy, failureOf(e));
    }
    for ([_]anyerror{ error.Full, error.IoErr, error.ReadOnly, error.Corrupt, error.OutOfMemory }) |e| {
        try testing.expectEqual(Failure.@"error", failureOf(e));
    }
}

test "SqliteCache: Migrations" {
    const allocator = testing.allocator;
    var pool = try Pool.init(allocator, ":memory:", .{});
    defer pool.deinit(allocator);

    const conn = try pool.acquire();
    defer pool.release(conn);

    _ = try Migrations.run(conn, cache_migrations);
}

test "SqliteCache: basic put and get" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: u64 = @intCast(std.Io.Timestamp.now(testing.io, .boot).toSeconds());
    const req = CachePutRequest{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600, .must_revalidate = false },
        .headers = &.{.{ .name = "Content-Type", .value = "text/html" }},
        .vary_headers = &.{},
    };

    try cache.put(req, "hello world");

    const result = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now,
            .request_headers = &.{},
        },
    );

    try testing.expect(result == .hit);
    try testing.expectEqualStrings("hello world", result.hit.data.buffer);
    try testing.expectEqual(@as(u16, 200), result.hit.status);
    try testing.expectEqualStrings("text/html", result.hit.content_type);
}

test "SqliteCache: get expiration" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now = 5000;
    const max_age = 1000;

    const req = CachePutRequest{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 900,
        .cache_control = .{ .max_age = max_age },
        .etag = "ABC",
        .headers = &.{},
        .vary_headers = &.{},
    };

    try cache.put(req, "hello world");

    // age = 50 + 900 = 950 < 1000: fresh
    const fresh = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now + 50,
            .request_headers = &.{},
        },
    );
    try testing.expect(fresh == .hit);

    const stale = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now + 200,
            .request_headers = &.{},
        },
    );
    try testing.expect(stale == .revalidate);
    try testing.expectEqualStrings("hello world", stale.revalidate.data.buffer);
}

test "SqliteCache: get expiration (without validators)" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now = 5000;
    const max_age = 1000;

    const req = CachePutRequest{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 900,
        .cache_control = .{ .max_age = max_age },
        .headers = &.{},
        .vary_headers = &.{},
    };

    try cache.put(req, "hello world");

    // age = 50 + 900 = 950 < 1000: fresh
    const fresh = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now + 50,
            .request_headers = &.{},
        },
    );
    try testing.expect(fresh == .hit);

    const stale = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now + 200,
            .request_headers = &.{},
        },
    );
    try testing.expect(stale == .stale);
}

test "SqliteCache: put override" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    {
        const req = CachePutRequest{
            .url = "https://example.com",
            .content_type = "text/html",
            .status = 200,
            .stored_at = 5000,
            .age_at_store = 0,
            .cache_control = .{ .max_age = 1000 },
            .headers = &.{},
            .vary_headers = &.{},
        };
        try cache.put(req, "hello world");

        const result = try cache.get(
            arena.allocator(),
            .{
                .url = "https://example.com",
                .timestamp = 5000,
                .request_headers = &.{},
            },
        );
        try testing.expect(result == .hit);
        try testing.expectEqualStrings("hello world", result.hit.data.buffer);
    }

    {
        const req = CachePutRequest{
            .url = "https://example.com",
            .content_type = "text/html",
            .status = 200,
            .stored_at = 10000,
            .age_at_store = 0,
            .cache_control = .{ .max_age = 2000 },
            .headers = &.{},
            .vary_headers = &.{},
        };
        try cache.put(req, "goodbye world");

        const result = try cache.get(
            arena.allocator(),
            .{
                .url = "https://example.com",
                .timestamp = 10000,
                .request_headers = &.{},
            },
        );
        try testing.expect(result == .hit);
        try testing.expectEqualStrings("goodbye world", result.hit.data.buffer);
    }
}

test "SqliteCache: vary hit and miss" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: u64 = @intCast(std.Io.Timestamp.now(testing.io, .boot).toSeconds());
    const req = CachePutRequest{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{},
        .vary_headers = &.{
            .{ .name = "Accept-Encoding", .value = "gzip" },
        },
    };

    try cache.put(req, "hello world");

    const hit = try cache.get(arena.allocator(), .{
        .url = "https://example.com",
        .timestamp = now,
        .request_headers = &.{.{ .name = "Accept-Encoding", .value = "gzip" }},
    });
    try testing.expect(hit == .hit);
    try testing.expectEqualStrings("hello world", hit.hit.data.buffer);

    const mismatch = try cache.get(arena.allocator(), .{
        .url = "https://example.com",
        .timestamp = now,
        .request_headers = &.{.{ .name = "Accept-Encoding", .value = "br" }},
    });
    try testing.expect(mismatch == .miss);

    const missing_header = try cache.get(arena.allocator(), .{
        .url = "https://example.com",
        .timestamp = now,
        .request_headers = &.{},
    });
    try testing.expect(missing_header == .miss);
}

test "SqliteCache: vary multiple headers" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: u64 = @intCast(std.Io.Timestamp.now(testing.io, .boot).toSeconds());
    const req = CachePutRequest{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{},
        .vary_headers = &.{
            .{ .name = "Accept-Encoding", .value = "gzip" },
            .{ .name = "Accept-Language", .value = "en" },
        },
    };

    try cache.put(req, "hello world");

    const hit = try cache.get(arena.allocator(), .{
        .url = "https://example.com",
        .timestamp = now,
        .request_headers = &.{
            .{ .name = "Accept-Encoding", .value = "gzip" },
            .{ .name = "Accept-Language", .value = "en" },
        },
    });
    try testing.expect(hit == .hit);
    try testing.expectEqualStrings("hello world", hit.hit.data.buffer);

    const mismatch = try cache.get(arena.allocator(), .{
        .url = "https://example.com",
        .timestamp = now,
        .request_headers = &.{
            .{ .name = "Accept-Encoding", .value = "gzip" },
            .{ .name = "Accept-Language", .value = "fr" },
        },
    });
    try testing.expect(mismatch == .miss);
}

test "SqliteCache: clear removes all entries" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: u64 = @intCast(std.Io.Timestamp.now(testing.io, .boot).toSeconds());
    try cache.put(.{
        .url = "https://example.com/a",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{},
        .vary_headers = &.{},
    }, "body a");

    try cache.put(.{
        .url = "https://example.com/b",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{},
        .vary_headers = &.{},
    }, "body b");

    try testing.expect((try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com/a",
            .timestamp = now,
            .request_headers = &.{},
        },
    )) == .hit);
    try testing.expect((try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com/b",
            .timestamp = now,
            .request_headers = &.{},
        },
    )) == .hit);

    try cache.clear();

    try testing.expect((try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com/a",
            .timestamp = now,
            .request_headers = &.{},
        },
    )) == .miss);
    try testing.expect((try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com/b",
            .timestamp = now,
            .request_headers = &.{},
        },
    )) == .miss);
}

test "SqliteCache: put after clear works" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: u64 = @intCast(std.Io.Timestamp.now(testing.io, .boot).toSeconds());
    const req = CachePutRequest{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{},
        .vary_headers = &.{},
    };

    try cache.put(req, "before clear");
    try cache.clear();

    try testing.expect((try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now,
            .request_headers = &.{},
        },
    )) == .miss);

    try cache.put(req, "after clear");
    const result = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now,
            .request_headers = &.{},
        },
    );
    try testing.expect(result == .hit);
    try testing.expectEqualStrings("after clear", result.hit.data.buffer);
}

test "SqliteCache: evict removes entry" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: u64 = @intCast(std.Io.Timestamp.now(testing.io, .boot).toSeconds());
    const req = CachePutRequest{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{},
        .vary_headers = &.{},
    };

    try cache.put(req, "hello world");

    const before = try cache.get(
        arena.allocator(),
        .{ .url = "https://example.com", .timestamp = now, .request_headers = &.{} },
    );
    try testing.expect(before == .hit);

    cache.evict("https://example.com");

    try testing.expect((try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now,
            .request_headers = &.{},
        },
    )) == .miss);
}

test "SqliteCache: renew refreshes expiry" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: i64 = 5000;
    try cache.put(.{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 1000 },
        .etag = "ABC",
        .headers = &.{},
        .vary_headers = &.{},
    }, "hello world");

    try cache.renew(
        arena.allocator(),
        .{ .url = "https://example.com", .timestamp = now + 500, .headers = &.{} },
    );

    // Clock reset to now+500, so still fresh at now+1200
    const fresh = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now + 1200,
            .request_headers = &.{},
        },
    );
    try testing.expect(fresh == .hit);

    const stale = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now + 1500,
            .request_headers = &.{},
        },
    );
    try testing.expect(stale == .revalidate);
}

test "SqliteCache: renew preserves body" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: u64 = @intCast(std.Io.Timestamp.now(testing.io, .boot).toSeconds());
    try cache.put(.{
        .url = "https://example.com",
        .content_type = "text/html",
        .status = 200,
        .stored_at = now,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{},
        .vary_headers = &.{},
    }, "original body");

    try cache.renew(
        arena.allocator(),
        .{ .url = "https://example.com", .timestamp = now + 100, .headers = &.{} },
    );

    const result = try cache.get(
        arena.allocator(),
        .{
            .url = "https://example.com",
            .timestamp = now + 100,
            .request_headers = &.{},
        },
    );
    try testing.expect(result == .hit);
    try testing.expectEqualStrings("original body", result.hit.data.buffer);
}

test "SqliteCache: maintenance purges expired entries without validators" {
    var cache = try setupCache(testing.allocator);
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // Expires at 6000, no validators: purged.
    try cache.put(.{
        .url = "https://example.com/expired",
        .content_type = "text/html",
        .status = 200,
        .stored_at = 5000,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 1000 },
        .headers = &.{},
        .vary_headers = &.{},
    }, "expired");

    // Expires at 6000 but has an etag: kept for revalidation.
    try cache.put(.{
        .url = "https://example.com/revalidate",
        .content_type = "text/html",
        .status = 200,
        .stored_at = 5000,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 1000 },
        .etag = "ABC",
        .headers = &.{},
        .vary_headers = &.{},
    }, "revalidate");

    // Expires at 7900: kept.
    try cache.put(.{
        .url = "https://example.com/fresh",
        .content_type = "text/html",
        .status = 200,
        .stored_at = 6900,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 1000 },
        .headers = &.{},
        .vary_headers = &.{},
    }, "fresh");

    cache.maintenance(7000);

    try testing.expect((try cache.get(arena.allocator(), .{
        .url = "https://example.com/expired",
        .timestamp = 7000,
        .request_headers = &.{},
    })) == .miss);
    try testing.expect((try cache.get(arena.allocator(), .{
        .url = "https://example.com/revalidate",
        .timestamp = 7000,
        .request_headers = &.{},
    })) == .revalidate);
    try testing.expect((try cache.get(arena.allocator(), .{
        .url = "https://example.com/fresh",
        .timestamp = 7000,
        .request_headers = &.{},
    })) == .hit);
}

test "SqliteCache: expired purge uses the cache_expiry index" {
    var cache = try SqliteCache.init(testing.allocator, .memory, .{});
    defer cache.deinit();

    const conn = try cache.pool.acquire();
    defer cache.pool.release(conn);

    var plan = (try conn.row(
        "explain query plan " ++ purge_expired_sql,
        .{@as(i64, 0)},
    )) orelse return error.NoQueryPlan;
    defer plan.deinit();

    const detail = plan.get([]const u8, 3);
    if (std.mem.find(u8, detail, "cache_expiry") == null) {
        std.debug.print("query plan: {s}\n", .{detail});
        return error.FullTableScan;
    }
}

test "SqliteCache: evicts oldest entries over the limit" {
    var cache = Cache{ .kind = .{ .sqlite = try .init(testing.allocator, .memory, .{ .entry_limit = 3 }) } };
    defer cache.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const now: u64 = @intCast(std.Io.Timestamp.now(testing.io, .boot).toSeconds());

    const urls = [_][:0]const u8{
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
        "https://example.com/d",
    };

    for (urls, 0..) |url, i| {
        try cache.put(.{
            .url = url,
            .content_type = "text/html",
            .status = 200,
            .stored_at = now + i,
            .age_at_store = 0,
            .cache_control = .{ .max_age = 600 },
            .headers = &.{},
            .vary_headers = &.{},
        }, url);
    }

    cache.maintenance(now + urls.len);

    const evicted = try cache.get(arena.allocator(), .{
        .url = "https://example.com/a",
        .timestamp = now,
        .request_headers = &.{},
    });
    try testing.expect(evicted == .miss);

    for (urls[1..]) |url| {
        const hit = try cache.get(arena.allocator(), .{
            .url = url,
            .timestamp = now + urls.len,
            .request_headers = &.{},
        });
        try testing.expect(hit == .hit);
        try testing.expectEqualStrings(url, hit.hit.data.buffer);
    }
}

fn testPut(cache: *Cache, url: [:0]const u8, stored_at: u64, max_age: u64, etag: ?[]const u8, body: []const u8) !void {
    try cache.put(.{
        .url = url,
        .content_type = "text/plain",
        .status = 200,
        .stored_at = stored_at,
        .age_at_store = 0,
        .cache_control = .{ .max_age = max_age },
        .etag = etag,
        .headers = &.{},
        .vary_headers = &.{},
    }, body);
}

fn testGet(cache: *Cache, arena: std.mem.Allocator, url: []const u8, at: u64) !std.meta.Tag(CacheGetResult) {
    return std.meta.activeTag(try cache.get(arena, .{ .url = url, .timestamp = at, .request_headers = &.{} }));
}

test "SqliteCache: size limit evicts expired entries first, then the oldest stored" {
    // 4 entries of 10 bytes, a 20-byte cap: 20 bytes go.
    var cache = Cache{ .kind = .{ .sqlite = try .init(testing.allocator, .memory, .{ .size_limit = 20 }) } };
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // the newest, but expired at 4600 (it has an etag, so the expiry purge keeps it)
    try testPut(&cache, "https://example.com/expired", 4500, 100, "\"e\"", "0123456789");
    try testPut(&cache, "https://example.com/old", 2000, 100_000, null, "0123456789");
    try testPut(&cache, "https://example.com/mid", 3000, 100_000, null, "0123456789");
    try testPut(&cache, "https://example.com/new", 4000, 100_000, null, "0123456789");

    cache.maintenance(5000);

    try testing.expectEqual(.miss, try testGet(&cache, arena.allocator(), "https://example.com/expired", 5000));
    try testing.expectEqual(.miss, try testGet(&cache, arena.allocator(), "https://example.com/old", 5000));
    try testing.expectEqual(.hit, try testGet(&cache, arena.allocator(), "https://example.com/mid", 5000));
    try testing.expectEqual(.hit, try testGet(&cache, arena.allocator(), "https://example.com/new", 5000));
}

test "SqliteCache: size counts the body and the headers, and an entry over the entry size limit is not stored" {
    var cache = Cache{ .kind = .{ .sqlite = try .init(testing.allocator, .memory, .{ .entry_size_limit = 10 }) } };
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const req = CachePutRequest{
        .url = "https://example.com/e",
        .content_type = "text/plain",
        .status = 200,
        .stored_at = 5000,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{.{ .name = "a", .value = "bc" }}, // 3 bytes
        // a Vary header the request lacked, as tryCache records it: 2 bytes
        .vary_headers = &.{.{ .name = "vv", .value = "" }},
    };
    try cache.put(req, "12345"); // 5 + 3 + 2 = 10: fits
    try testing.expectEqual(.hit, try testGet(&cache, arena.allocator(), "https://example.com/e", 5000));
    {
        const conn = try cache.kind.sqlite.pool.acquire();
        defer cache.kind.sqlite.pool.release(conn);
        try testing.expectEqual(10, (try conn.scalar(i64, "select size from cache where url = $1", .{"https://example.com/e"})).?);
        // what a renewal recounts agrees with what put stored
        try conn.exec(update_size_sql, .{"https://example.com/e"});
        try testing.expectEqual(10, (try conn.scalar(i64, "select size from cache where url = $1", .{"https://example.com/e"})).?);
    }

    var big = req;
    big.url = "https://example.com/big";
    try testing.expectError(error.EntryTooLarge, cache.put(big, "123456")); // 11 bytes
    try testing.expectEqual(.miss, try testGet(&cache, arena.allocator(), "https://example.com/big", 5000));
}

test "SqliteCache: a renewal recounts the size, and one that outgrows the entry size limit evicts the entry" {
    var cache = Cache{ .kind = .{ .sqlite = try .init(testing.allocator, .memory, .{ .entry_size_limit = 11 }) } };
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    try cache.put(.{
        .url = "https://example.com/e",
        .content_type = "text/plain",
        .status = 200,
        .stored_at = 5000,
        .age_at_store = 0,
        .cache_control = .{ .max_age = 600 },
        .headers = &.{.{ .name = "a", .value = "bc" }}, // 3 bytes
        .vary_headers = &.{},
    }, "12345"); // 8 bytes

    // longer headers recount the size: 5 + 6 = 11, still within the limit
    try cache.renew(arena.allocator(), .{ .url = "https://example.com/e", .timestamp = 5100, .headers = &.{.{ .name = "xy", .value = "zzzz" }} });
    {
        const conn = try cache.kind.sqlite.pool.acquire();
        defer cache.kind.sqlite.pool.release(conn);
        try testing.expectEqual(11, (try conn.scalar(i64, "select size from cache where url = $1", .{"https://example.com/e"})).?);
    }
    try testing.expectEqual(.hit, try testGet(&cache, arena.allocator(), "https://example.com/e", 5100));

    // 5 + 7 = 12 is over the limit: the entry goes, with its headers
    try cache.renew(arena.allocator(), .{ .url = "https://example.com/e", .timestamp = 5200, .headers = &.{.{ .name = "xy", .value = "zzzzz" }} });
    try testing.expectEqual(.miss, try testGet(&cache, arena.allocator(), "https://example.com/e", 5200));
    const conn = try cache.kind.sqlite.pool.acquire();
    defer cache.kind.sqlite.pool.release(conn);
    try testing.expectEqual(0, (try conn.scalar(i64, "select count(*) from header", .{})).?);
}

test "SqliteCache: a size limit below one entry empties the cache and puts keep working" {
    var cache = Cache{ .kind = .{ .sqlite = try .init(testing.allocator, .memory, .{ .size_limit = 4 }) } };
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    try testPut(&cache, "https://example.com/a", 5000, 600, null, "0123456789");
    cache.maintenance(5000);
    try testing.expectEqual(.miss, try testGet(&cache, arena.allocator(), "https://example.com/a", 5000));

    try testPut(&cache, "https://example.com/a", 5001, 600, null, "0123456789");
    try testing.expectEqual(.hit, try testGet(&cache, arena.allocator(), "https://example.com/a", 5001));
}

// Fails when `sql`, an explained query, scans the cache table itself rather
// than an index that covers the query: such a scan reads every row, body
// included. With `want`, also fails unless some step contains it.
fn expectNoTableScan(conn: anytype, sql: []const u8, values: anytype, want: ?[]const u8) !void {
    var explained = try conn.rows(sql, values);
    defer explained.deinit();

    var steps: usize = 0;
    var found = want == null;
    while (try explained.next()) |step| {
        steps += 1;
        const detail = step.get([]const u8, 3);
        if (want) |w| {
            if (std.mem.find(u8, detail, w) != null) found = true;
        }
        if (std.mem.startsWith(u8, detail, "SCAN cache") and std.mem.find(u8, detail, "COVERING INDEX") == null) {
            std.debug.print("query step: {s}\n", .{detail});
            return error.FullTableScan;
        }
    }
    if (steps == 0) return error.NothingExplained;
    if (!found) {
        std.debug.print("no query step contains: {s}\n", .{want.?});
        return error.IndexNotUsed;
    }
}

test "SqliteCache: maintenance's queries do not scan the cache table" {
    var cache = try SqliteCache.init(testing.allocator, .memory, .{});
    defer cache.deinit();

    const conn = try cache.pool.acquire();
    defer cache.pool.release(conn);

    try expectNoTableScan(conn, "explain query plan " ++ purge_expired_sql, .{@as(i64, 0)}, null);
    try expectNoTableScan(conn, "explain query plan " ++ evict_over_count_sql, .{@as(i64, 3)}, "COVERING INDEX cache_evict");
    try expectNoTableScan(conn, "explain query plan " ++ total_size_sql, .{}, "COVERING INDEX cache_evict");
    try expectNoTableScan(conn, "explain query plan " ++ evict_over_size_sql, .{ @as(i64, 0), @as(i64, 1000) }, "COVERING INDEX cache_evict");
}

test "SqliteCache: maintenance does not read the bodies" {
    const sqlite3 = @import("../../storage/sqlite/Sqlite.zig").c;
    const body_pages = 16;
    const entries = 8;

    var cache = Cache{ .kind = .{ .sqlite = try .init(testing.allocator, .memory, .{ .entry_limit = 100, .size_limit = 1 << 30 }) } };
    defer cache.deinit();
    const conn = cache.kind.sqlite.pool.conns[0];

    // Each body spans body_pages pages of whatever size the database uses.
    const page_size: usize = @intCast((try conn.scalar(i64, "pragma page_size", .{})).?);
    const body = try testing.allocator.alloc(u8, body_pages * page_size);
    defer testing.allocator.free(body);
    @memset(body, 'x');

    var url_buf: [64]u8 = undefined;
    for (0..entries) |i| {
        const url = try std.fmt.bufPrintSentinel(&url_buf, "https://example.com/{d}", .{i}, 0);
        try testPut(&cache, url, 5000 + i, 600, null, body);
    }

    var cur: c_int = 0;
    var high: c_int = 0;
    _ = sqlite3.sqlite3_db_status(conn.conn, sqlite3.SQLITE_DBSTATUS_CACHE_HIT, &cur, &high, 1);
    _ = sqlite3.sqlite3_db_status(conn.conn, sqlite3.SQLITE_DBSTATUS_CACHE_MISS, &cur, &high, 1);

    // Nothing is expired, over the count or over the size: nothing goes.
    // Under the size limit maintenance skips the size eviction, so it runs
    // here too, with the same limit.
    cache.kind.sqlite.maintenance(5000 + entries);
    try conn.exec(evict_over_size_sql, .{ @as(i64, 5000 + entries), @as(i64, 1 << 30) });

    var hits: c_int = 0;
    var misses: c_int = 0;
    _ = sqlite3.sqlite3_db_status(conn.conn, sqlite3.SQLITE_DBSTATUS_CACHE_HIT, &hits, &high, 0);
    _ = sqlite3.sqlite3_db_status(conn.conn, sqlite3.SQLITE_DBSTATUS_CACHE_MISS, &misses, &high, 0);
    const pages: usize = @intCast(hits + misses);
    if (pages >= body_pages) {
        std.debug.print("maintenance read {d} pages, one body is {d}\n", .{ pages, body_pages });
        return error.ReadTheBodies;
    }
    try testing.expectEqual(entries, (try conn.scalar(i64, "select count(*) from cache", .{})).?);
}

test "SqliteCache: a cache at the size limit is left alone, one byte over it is evicted" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    for ([_]struct { limit: u64, kept: bool }{
        .{ .limit = 20, .kept = true }, // exactly at the limit: the eviction is skipped
        .{ .limit = 19, .kept = false },
    }) |case| {
        var cache = Cache{ .kind = .{ .sqlite = try .init(testing.allocator, .memory, .{ .size_limit = case.limit }) } };
        defer cache.deinit();
        try testPut(&cache, "https://example.com/old", 4000, 100_000, null, "0123456789");
        try testPut(&cache, "https://example.com/new", 4500, 100_000, null, "0123456789");
        cache.maintenance(5000);
        const want: std.meta.Tag(CacheGetResult) = if (case.kept) .hit else .miss;
        try testing.expectEqual(want, try testGet(&cache, arena.allocator(), "https://example.com/old", 5000));
        try testing.expectEqual(.hit, try testGet(&cache, arena.allocator(), "https://example.com/new", 5000));
    }
}

test "SqliteCache: a cache from before the size column is upgraded on open, and puts, gets and size eviction work on it" {
    const lp_testing = @import("../../testing.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try lp_testing.tmpPath(&tmp, "cache");
    try std.Io.Dir.cwd().createDirPath(lp.io, dir);
    const db_path = try testing.allocator.printSentinel("{s}/cache.db", .{dir}, 0);
    defer testing.allocator.free(db_path);

    const now = lp.datetime.timestamp(.real);
    var old_len: usize = 0;
    {
        // A cache as the previous version left it: every migration but the
        // last, and an entry whose body spans several pages.
        var pool = try Pool.init(testing.allocator, db_path, .{});
        defer pool.deinit(testing.allocator);
        const conn = try pool.acquire();
        defer pool.release(conn);
        _ = try Migrations.run(conn, cache_migrations[0 .. cache_migrations.len - 1]);
        const page_size: usize = @intCast((try conn.scalar(i64, "pragma page_size", .{})).?);
        const old_body = try testing.allocator.alloc(u8, 3 * page_size);
        defer testing.allocator.free(old_body);
        old_len = old_body.len;
        @memset(old_body, 'o');
        try conn.exec(
            \\ insert into cache (url, status, stored_at, age_at_store, max_age, must_revalidate, content_type, body)
            \\ values ('https://example.com/old', 200, $1, 0, 600, 0, 'text/plain', $2)
        , .{ @as(i64, @intCast(now - 10)), Blob{ .data = old_body } });
        try conn.exec("insert into header (url, name, value, vary) values ('https://example.com/old', 'x-old', x'31', 0)", .{});
    }

    // 150 bytes: the two new 100-byte entries do not both fit.
    var cache = Cache{ .kind = .{ .sqlite = try .init(testing.allocator, .{ .path = dir }, .{ .size_limit = 150 }) } };
    defer cache.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    {
        const conn = try cache.kind.sqlite.pool.acquire();
        defer cache.kind.sqlite.pool.release(conn);
        try testing.expectEqual(1, (try conn.scalar(i64, "select count(*) from pragma_index_list('cache') where name = 'cache_evict'", .{})).?);
        try testing.expectEqual(0, (try conn.scalar(i64, "select count(*) from pragma_index_list('cache') where name = 'cache_stored_at'", .{})).?);
        // the old entry counts as 0 bytes until it is replaced or renewed
        try testing.expectEqual(0, (try conn.scalar(i64, "select size from cache where url = 'https://example.com/old'", .{})).?);
    }
    {
        const got = try cache.get(arena.allocator(), .{ .url = "https://example.com/old", .timestamp = now, .request_headers = &.{} });
        try testing.expect(got == .hit);
        try testing.expectEqual(old_len, got.hit.data.buffer.len);
    }

    const body: [100]u8 = @splat('n');
    try testPut(&cache, "https://example.com/a", now - 5, 600, null, &body);
    try testPut(&cache, "https://example.com/b", now, 600, null, &body);

    // 200 bytes over 150: the oldest stored go until the rest fits.
    cache.maintenance(now);
    try testing.expectEqual(.miss, try testGet(&cache, arena.allocator(), "https://example.com/old", now));
    try testing.expectEqual(.miss, try testGet(&cache, arena.allocator(), "https://example.com/a", now));
    try testing.expectEqual(.hit, try testGet(&cache, arena.allocator(), "https://example.com/b", now));
}
