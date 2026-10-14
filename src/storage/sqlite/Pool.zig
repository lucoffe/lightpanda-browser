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
const Sqlite = @import("Sqlite.zig");

const Thread = std.Thread;
const Allocator = std.mem.Allocator;

const Pool = @This();

available: usize,
mutex: std.Io.Mutex,
cond: std.Io.Condition,
conns: []Sqlite.Conn,
// How long acquire waits for a free connection, ms; null = until one is released.
acquire_timeout_ms: ?u32,

pub const default_connections: u8 = 5;
pub const default_busy_timeout_ms: u32 = 1000;

pub const Options = struct {
    /// Connections in the pool, at least 1 (a pool on an in-memory database
    /// always has 1).
    connections: u8 = default_connections,
    /// SQLite's busy timeout on each connection, ms.
    busy_timeout_ms: u32 = default_busy_timeout_ms,
    /// How long acquire waits for a free connection before error.PoolBusy,
    /// ms; null waits until one is released.
    acquire_timeout_ms: ?u32 = null,
};

pub fn init(allocator: Allocator, path: [:0]const u8, opts: Options) !Pool {
    std.debug.assert(opts.connections > 0);
    // can't have a pool of connections to in-memory database, so, to keep the
    // API simple, we create a pool of 1.
    const count: usize = if (std.mem.eql(u8, path, ":memory:")) 1 else opts.connections;

    var conns = try allocator.alloc(Sqlite.Conn, count);
    errdefer allocator.free(conns);

    var initialized: usize = 0;
    errdefer {
        for (0..initialized) |i| {
            conns[i].close();
        }
    }

    for (0..count) |i| {
        conns[i] = try Sqlite.Conn.open(path);
        initialized += 1;
        try conns[i].busyTimeout(@intCast(opts.busy_timeout_ms));
    }

    return .{
        .cond = .init,
        .mutex = .init,
        .conns = conns,
        .available = count,
        .acquire_timeout_ms = opts.acquire_timeout_ms,
    };
}

pub fn deinit(self: *Pool, allocator: Allocator) void {
    for (self.conns) |conn| {
        conn.close();
    }
    allocator.free(self.conns);
}

/// A free connection, waiting for one to be released. With an acquire
/// timeout, gives up with error.PoolBusy once it has passed. Not a
/// cancelation point.
pub fn acquire(self: *Pool) error{PoolBusy}!Sqlite.Conn {
    const timeout: std.Io.Timeout = if (self.acquire_timeout_ms) |ms|
        (std.Io.Timeout{ .duration = .{ .raw = .fromMilliseconds(ms), .clock = .awake } }).toDeadline(lp.io)
    else
        .none;

    self.mutex.lockUncancelable(lp.io);
    defer self.mutex.unlock(lp.io);
    // The timed wait is cancelable; acquire, like the lock above, is not.
    const cancel_protection = lp.io.swapCancelProtection(.blocked);
    defer _ = lp.io.swapCancelProtection(cancel_protection);
    while (self.available == 0) {
        self.cond.waitTimeout(lp.io, &self.mutex, timeout) catch |err| switch (err) {
            // A connection released as the wait gave up is still taken.
            error.Timeout => if (self.available == 0) return error.PoolBusy,
            error.Canceled => unreachable,
        };
    }
    const index = self.available - 1;
    self.available = index;
    return self.conns[index];
}

pub fn release(self: *Pool, conn: Sqlite.Conn) void {
    var conns = self.conns;

    self.mutex.lockUncancelable(lp.io);
    const available = self.available;
    conns[available] = conn;
    self.available = available + 1;
    self.mutex.unlock(lp.io);
    self.cond.signal(lp.io);
}

const testing = @import("../../testing.zig");
test "Sqlite: Pool" {
    try testThreaded(.{}, 6);
}

test "Sqlite: Pool with an acquire timeout, more threads than connections" {
    // SqliteCache always sets one, so this is the acquire that runs in production.
    try testThreaded(.{ .acquire_timeout_ms = 5000 }, 12);
}

const max_test_threads = 12;

fn testThreaded(opts: Options, thread_count: usize) !void {
    std.debug.assert(thread_count <= max_test_threads);

    // :memory: _has_ to run with a single connection in the pool, which isn't
    // that useful for testing. So we create a temp file.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testing.tmpPath(&tmp, "pool.sqlite");

    var pool = try Pool.init(testing.allocator, path, opts);
    defer pool.deinit(testing.allocator);

    {
        const conn = try pool.acquire();
        defer pool.release(conn);

        try conn.exec("create table pool_test (cnt int not null)", .{});
        try conn.exec("insert into pool_test (cnt) values (0)", .{});
    }

    for (pool.conns) |conn| {
        // This is not safe and can result in corruption. This is only set
        // because the tests might be run on really slow hardware and we
        // want to avoid having a busy timeout.
        try conn.exec("pragma synchronous=off", .{});

        // Also not safe, but we're trying to avoid busy timeouts without using
        // WAL mode, which can trigger false positives in thread-sanitizer
        try conn.exec("pragma journal_mode=memory", .{});
    }

    var failures: std.atomic.Value(usize) = .init(0);
    var threads: [max_test_threads]Thread = undefined;
    for (threads[0..thread_count]) |*t| {
        t.* = try Thread.spawn(.{}, testPool, .{ &pool, &failures });
    }
    for (threads[0..thread_count]) |t| t.join();

    // every acquire got a connection, and every connection came back
    try testing.expectEqual(0, failures.load(.monotonic));
    try testing.expectEqual(pool.conns.len, pool.available);

    const c1 = try pool.acquire();
    defer pool.release(c1);

    const row = (try c1.row("select cnt from pool_test", .{})).?;
    try testing.expectEqual(@as(i64, @intCast(thread_count * 100)), row.get(i64, 0));
    row.deinit();

    try c1.exec("drop table pool_test", .{});
}

fn testPool(p: *Pool, failures: *std.atomic.Value(usize)) void {
    for (0..100) |_| {
        const conn = p.acquire() catch {
            _ = failures.fetchAdd(1, .monotonic);
            continue;
        };
        conn.exec("begin immediate", .{}) catch unreachable;
        conn.exec("update pool_test set cnt = cnt + 1", .{}) catch |err| {
            std.debug.print("update err: {any}\n", .{err});
            unreachable;
        };
        conn.exec("commit", .{}) catch unreachable;
        p.release(conn);
        lp.io.sleep(.fromMicroseconds(100), .awake) catch {};
    }
}

test "Sqlite: Pool acquire gives up past its timeout" {
    var pool = try Pool.init(testing.allocator, ":memory:", .{ .acquire_timeout_ms = 20 });
    defer pool.deinit(testing.allocator);

    const held = try pool.acquire();
    const before = lp.datetime.milliTimestamp(.awake);
    try testing.expectError(error.PoolBusy, pool.acquire());
    try testing.expect(lp.datetime.milliTimestamp(.awake) - before >= 20);
    pool.release(held);

    const again = try pool.acquire();
    pool.release(again);
}

test "Sqlite: Pool acquire is woken by a release before its timeout" {
    var pool = try Pool.init(testing.allocator, ":memory:", .{ .acquire_timeout_ms = 10_000 });
    defer pool.deinit(testing.allocator);

    const held = try pool.acquire();
    const releaser = try Thread.spawn(.{}, struct {
        fn run(p: *Pool, conn: Sqlite.Conn) void {
            lp.io.sleep(.fromMilliseconds(20), .awake) catch {};
            p.release(conn);
        }
    }.run, .{ &pool, held });

    const before = lp.datetime.milliTimestamp(.awake);
    const conn = try pool.acquire();
    const waited = lp.datetime.milliTimestamp(.awake) - before;
    releaser.join();
    pool.release(conn);
    // woken by the release, not by the 10 s timeout
    try testing.expect(waited < 5_000);
}
