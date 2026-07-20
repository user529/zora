/// state_crypto.zig — encryption primitives for optional state-at-rest encryption.
///
/// Owns three concerns:
///
/// 1. AEAD envelope (XChaCha20-Poly1305): nonce(24) ‖ ciphertext ‖ tag(16).
///    The nonce is drawn from the CSPRNG per call so two encryptions of the same
///    plaintext differ. No associated data is bound.
///
/// 2. Argon2id key derivation: derives a 32-byte key from a passphrase and a
///    random 16-byte salt using OWASP-minimum cost parameters. The key is stored
///    in an mlocked, page-aligned heap buffer (best-effort) via the Cipher type.
///
/// 3. Passphrase verifier: encrypts a known constant into the database meta table
///    at first open so subsequent opens can confirm the passphrase is correct
///    before touching user data.
///
/// The state store calls these to encrypt payloads before writing them to SQLite
/// and to decrypt them on read.

const std = @import("std");

const Aead = std.crypto.aead.chacha_poly.XChaCha20Poly1305;

pub const key_len: usize = Aead.key_length;     // 32
pub const nonce_len: usize = Aead.nonce_length; // 24
pub const tag_len: usize = Aead.tag_length;     // 16
pub const overhead: usize = nonce_len + tag_len;

// sealInto / openAlloc are the two AEAD envelope primitives. They are standalone
// functions taking the raw key (not Cipher methods) for two reasons: the Cipher
// methods of the same intent delegate to them without a name collision — a bare
// `encryptInto(...)` inside Cipher.encryptInto would recurse into the method —
// and the envelope can be exercised independently of key derivation.

/// Seal `plaintext` into `out` as nonce ‖ ciphertext ‖ tag. `out` must hold at
/// least `plaintext.len + overhead` bytes; returns the filled prefix. The nonce
/// is drawn from the CSPRNG via `io` per call.
fn sealInto(io: std.Io, key: [key_len]u8, out: []u8, plaintext: []const u8) []u8 {
    std.debug.assert(out.len >= plaintext.len + overhead);
    const nonce = out[0..nonce_len];
    io.random(nonce);
    const cipher = out[nonce_len..][0..plaintext.len];
    const tag = out[nonce_len + plaintext.len ..][0..tag_len];
    Aead.encrypt(cipher, tag, plaintext, "", nonce.*, key);
    return out[0 .. plaintext.len + overhead];
}

/// Open an envelope produced by sealInto, allocating the recovered plaintext.
/// Returns error.DecryptFailed on a wrong key, truncation, or tampering.
fn openAlloc(
    allocator: std.mem.Allocator,
    key: [key_len]u8,
    blob: []const u8,
) error{ DecryptFailed, OutOfMemory }![]u8 {
    if (blob.len < overhead) return error.DecryptFailed;
    const msg_len = blob.len - overhead;
    const nonce = blob[0..nonce_len];
    const cipher = blob[nonce_len..][0..msg_len];
    const tag = blob[nonce_len + msg_len ..][0..tag_len];

    const out = try allocator.alloc(u8, msg_len);
    errdefer allocator.free(out);
    Aead.decrypt(out, cipher, tag.*, "", nonce.*, key) catch return error.DecryptFailed;
    return out;
}

// ---------------------------------------------------------------------------
// Argon2id key derivation, Cipher, params, and verifier
// ---------------------------------------------------------------------------

const argon2 = std.crypto.pwhash.argon2;
const log = std.log.scoped(.state_crypto);

pub const salt_len: usize = 16;

/// The known plaintext encrypted by makeVerifierHex and checked by
/// checkVerifierHex to confirm a passphrase at database open time.
pub const verifier_plaintext = "zora-state-v1";

/// Argon2id tuning parameters stored alongside the salt in the database meta
/// table so the values remain stable even when this default changes.
pub const ArgonParams = struct { t: u32, m: u32, p: u24 };

/// Argon2id cost defaults: OWASP minimum (t=2, m=19 MiB, p=1). The values are
/// persisted at database creation time and do not change for the lifetime of
/// that database.
pub const default_argon = ArgonParams{ .t = 2, .m = 19456, .p = 1 };

/// Return a freshly drawn random salt of salt_len bytes.
pub fn randomSalt(io: std.Io) [salt_len]u8 {
    var s: [salt_len]u8 = undefined;
    io.random(&s);
    return s;
}

/// Render params as the canonical string "argon2id$t=<t>$m=<m>$p=<p>".
pub fn encodeParams(out: []u8, p: ArgonParams) ![]const u8 {
    return std.fmt.bufPrint(out, "argon2id$t={d}$m={d}$p={d}", .{ p.t, p.m, p.p });
}

/// Parse a string produced by encodeParams. Returns error.BadParams on any
/// format violation.
pub fn parseParams(s: []const u8) error{BadParams}!ArgonParams {
    var t: ?u32 = null;
    var m: ?u32 = null;
    var p: ?u24 = null;
    var saw_algo = false;
    var it = std.mem.splitScalar(u8, s, '$');
    while (it.next()) |field| {
        if (std.mem.eql(u8, field, "argon2id")) {
            saw_algo = true;
            continue;
        }
        const eq = std.mem.indexOfScalar(u8, field, '=') orelse return error.BadParams;
        const name = field[0..eq];
        const val = field[eq + 1 ..];
        if (std.mem.eql(u8, name, "t")) {
            t = std.fmt.parseInt(u32, val, 10) catch return error.BadParams;
        } else if (std.mem.eql(u8, name, "m")) {
            m = std.fmt.parseInt(u32, val, 10) catch return error.BadParams;
        } else if (std.mem.eql(u8, name, "p")) {
            p = std.fmt.parseInt(u24, val, 10) catch return error.BadParams;
        }
    }
    if (!saw_algo) return error.BadParams;
    return .{
        .t = t orelse return error.BadParams,
        .m = m orelse return error.BadParams,
        .p = p orelse return error.BadParams,
    };
}

// Alignment required by std.c.mlock/munlock: pointer must be page-granular.
const key_page_align = std.mem.Alignment.fromByteUnits(std.heap.page_size_min);

/// Owns a 32-byte derived key in a page-aligned heap buffer.
///
/// The buffer is mlocked before key derivation (best-effort: failure is logged
/// at warn, never error) so the key does not reach swap even transiently. The
/// key is stored on the heap rather than inline so a Cipher value can be moved
/// without invalidating the locked page. Call deinit when done.
pub const Cipher = struct {
    allocator: std.mem.Allocator,
    /// Page-aligned slice of exactly key_len bytes.
    key: []align(std.heap.page_size_min) u8,
    locked: bool,
    io: std.Io,

    /// Encrypt plaintext into out as nonce ‖ ciphertext ‖ tag. out must hold at
    /// least plaintext.len + overhead bytes; returns the filled prefix.
    pub fn encryptInto(self: Cipher, out: []u8, plaintext: []const u8) []u8 {
        return sealInto(self.io, self.key[0..key_len].*, out, plaintext);
    }

    /// Decrypt an envelope produced by encryptInto. Returns owned plaintext.
    pub fn decryptAlloc(
        self: Cipher,
        allocator: std.mem.Allocator,
        blob: []const u8,
    ) error{ DecryptFailed, OutOfMemory }![]u8 {
        return openAlloc(allocator, self.key[0..key_len].*, blob);
    }

    /// Securely erase the key, munlock if locked, and free the buffer. Must be
    /// called before the Cipher goes out of scope.
    pub fn deinit(self: *Cipher) void {
        std.crypto.secureZero(u8, self.key);
        if (self.locked) _ = std.c.munlock(@ptrCast(self.key.ptr), self.key.len);
        self.allocator.free(self.key);
    }
};

/// Derive a Cipher from passphrase + salt using Argon2id at cost p. The key
/// buffer is page-aligned and mlocked before derivation. The caller still owns
/// passphrase and must zero it when no further derivations are needed.
pub fn deriveCipher(
    allocator: std.mem.Allocator,
    io: std.Io,
    passphrase: []const u8,
    salt: [salt_len]u8,
    p: ArgonParams,
) error{ OutOfMemory, KdfFailed }!Cipher {
    const key = try allocator.alignedAlloc(u8, key_page_align, key_len);
    errdefer allocator.free(key);

    // Lock before derivation so the key never lands in swap, even transiently.
    const locked = std.c.mlock(@ptrCast(key.ptr), key.len) == 0;
    if (!locked) log.warn("mlock of key buffer failed — key may reach swap", .{});

    argon2.kdf(
        allocator,
        key,
        passphrase,
        &salt,
        .{ .t = p.t, .m = p.m, .p = p.p },
        .argon2id,
        io,
    ) catch {
        if (locked) _ = std.c.munlock(@ptrCast(key.ptr), key.len);
        return error.KdfFailed;
    };
    return .{ .allocator = allocator, .key = key, .locked = locked, .io = io };
}

/// Hex-encode an AEAD encryption of verifier_plaintext for storage in the
/// database meta table. The caller owns the returned slice.
pub fn makeVerifierHex(cipher: Cipher, allocator: std.mem.Allocator) error{OutOfMemory}![]u8 {
    var buf: [verifier_plaintext.len + overhead]u8 = undefined;
    const blob = cipher.encryptInto(&buf, verifier_plaintext);
    const hex = try allocator.alloc(u8, blob.len * 2);
    errdefer allocator.free(hex);
    _ = std.fmt.bufPrint(hex, "{x}", .{blob}) catch unreachable;
    return hex;
}

/// Decode and decrypt a stored verifier hex string. Returns
/// error.WrongEncryptionKey on auth failure, bad hex, or content mismatch so
/// callers get a single diagnostic without leaking which stage failed.
pub fn checkVerifierHex(
    cipher: Cipher,
    allocator: std.mem.Allocator,
    hex: []const u8,
) error{ WrongEncryptionKey, OutOfMemory }!void {
    if (hex.len % 2 != 0 or hex.len / 2 < overhead) return error.WrongEncryptionKey;
    const blob = try allocator.alloc(u8, hex.len / 2);
    defer allocator.free(blob);
    _ = std.fmt.hexToBytes(blob, hex) catch return error.WrongEncryptionKey;
    const out = cipher.decryptAlloc(allocator, blob) catch |e| switch (e) {
        error.DecryptFailed => return error.WrongEncryptionKey,
        error.OutOfMemory => return error.OutOfMemory,
    };
    defer allocator.free(out);
    if (!std.mem.eql(u8, out, verifier_plaintext)) return error.WrongEncryptionKey;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "encrypt/decrypt round-trips" {
    const key: [key_len]u8 = @splat(7);
    const msg = "{\"count\":42}";
    var buf: [128]u8 = undefined;
    const blob = sealInto(testing.io, key, &buf, msg);
    try testing.expectEqual(msg.len + overhead, blob.len);

    const out = try openAlloc(testing.allocator, key, blob);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings(msg, out);
}

test "a wrong key fails to decrypt" {
    const key: [key_len]u8 = @splat(1);
    var wrong = key;
    wrong[0] = 2;
    var buf: [64]u8 = undefined;
    const blob = sealInto(testing.io, key, &buf, "secret");
    try testing.expectError(error.DecryptFailed, openAlloc(testing.allocator, wrong, blob));
}

test "a tampered envelope fails to decrypt" {
    const key: [key_len]u8 = @splat(3);
    var buf: [64]u8 = undefined;
    const blob = sealInto(testing.io, key, &buf, "secret");
    blob[blob.len - 1] ^= 0xFF; // flip a tag byte
    try testing.expectError(error.DecryptFailed, openAlloc(testing.allocator, key, blob));
}

test "the same plaintext yields distinct ciphertext (random nonce)" {
    const key: [key_len]u8 = @splat(9);
    var a: [64]u8 = undefined;
    var b: [64]u8 = undefined;
    const ba = sealInto(testing.io, key, &a, "same");
    const bb = sealInto(testing.io, key, &b, "same");
    try testing.expect(!std.mem.eql(u8, ba, bb));
}

test "a too-short blob fails cleanly" {
    const key: [key_len]u8 = @splat(0);
    try testing.expectError(error.DecryptFailed, openAlloc(testing.allocator, key, "short"));
}

test "deriveCipher is deterministic for the same passphrase and salt" {
    const salt: [salt_len]u8 = @splat(5);
    var c1 = try deriveCipher(testing.allocator, testing.io, "hunter2", salt, default_argon);
    defer c1.deinit();
    var c2 = try deriveCipher(testing.allocator, testing.io, "hunter2", salt, default_argon);
    defer c2.deinit();
    try testing.expectEqualSlices(u8, c1.key, c2.key);
}

test "a different passphrase derives a different key" {
    const salt: [salt_len]u8 = @splat(5);
    var c1 = try deriveCipher(testing.allocator, testing.io, "right", salt, default_argon);
    defer c1.deinit();
    var c2 = try deriveCipher(testing.allocator, testing.io, "wrong", salt, default_argon);
    defer c2.deinit();
    try testing.expect(!std.mem.eql(u8, c1.key, c2.key));
}

test "Cipher encrypt/decrypt round-trips" {
    const salt = randomSalt(testing.io);
    var cph = try deriveCipher(testing.allocator, testing.io, "pw", salt, default_argon);
    defer cph.deinit();
    var buf: [64]u8 = undefined;
    const blob = cph.encryptInto(&buf, "hello");
    const out = try cph.decryptAlloc(testing.allocator, blob);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("hello", out);
}

test "params encode then parse round-trips" {
    var buf: [64]u8 = undefined;
    const s = try encodeParams(&buf, default_argon);
    const p = try parseParams(s);
    try testing.expectEqual(default_argon.t, p.t);
    try testing.expectEqual(default_argon.m, p.m);
    try testing.expectEqual(default_argon.p, p.p);
}

test "parseParams rejects input missing the argon2id prefix" {
    try testing.expectError(error.BadParams, parseParams("t=2$m=19456$p=1"));
}

test "verifier accepts the right passphrase and rejects a wrong one" {
    const salt = randomSalt(testing.io);
    var good = try deriveCipher(testing.allocator, testing.io, "correct", salt, default_argon);
    defer good.deinit();
    const hex = try makeVerifierHex(good, testing.allocator);
    defer testing.allocator.free(hex);
    try checkVerifierHex(good, testing.allocator, hex); // no error

    var bad = try deriveCipher(testing.allocator, testing.io, "incorrect", salt, default_argon);
    defer bad.deinit();
    try testing.expectError(error.WrongEncryptionKey, checkVerifierHex(bad, testing.allocator, hex));
}
