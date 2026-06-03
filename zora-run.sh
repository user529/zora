#!/bin/sh
# zora-run — entrypoint wrapper: select a malloc implementation, then exec zora.

set -eu

log() { printf 'zora-run: %s\n' "$*" >&2; }

# get_arch -> the architecture tag for the running machine
# (e.g. "x86-64", "AArch64").
# Empty when the machine is unrecognised; discovery then accepts any match.
get_arch() {
    case "$(uname -m 2>/dev/null || echo unknown)" in
        x86_64|amd64)        echo 'x86-64' ;;
        aarch64|arm64)       echo 'AArch64' ;;
        *)                   echo '' ;;
    esac
}

# find_jemalloc -> prints an absolute, readable libjemalloc path, or nothing.
# It asks ldconfig for the native library first, then falls back to a fixed set
# of directories. The multiarch entries are globs (aarch64-linux-gnu,
# x86_64-linux-gnu, ...), so the one list serves x86-64, ARM64,
# The sysroot prefix is part of each glob, so a fake tree (ZORA_SYSROOT) is searched too.
find_jemalloc() {
    if command -v ldconfig >/dev/null 2>&1; then
        tag=$(get_arch)
        p=$(ldconfig -p 2>/dev/null \
            | awk -v tag="$tag" \
                'index($0, "libjemalloc.so") && (tag == "" || index($0, tag)) \
                    { print $NF; exit }')
        if [ -n "${p:-}" ] && [ -r "$p" ]; then printf '%s\n' "$p"; return 0; fi
    fi
    root=${ZORA_SYSROOT:-}
    for d in "$root"/lib64 "$root"/usr/lib64 "$root"/usr/lib \
             "$root"/usr/lib/*-linux-gnu "$root"/usr/lib/*-linux-musl* \
             "$root"/usr/local/lib; do
        for so in libjemalloc.so.2 libjemalloc.so; do
            if [ -r "$d/$so" ]; then printf '%s\n' "$d/$so"; return 0; fi
        done
    done
    return 1
}

# is_musl -> success if the musl dynamic loader is present.
is_musl() {
    for f in "${ZORA_SYSROOT:-}"/lib/ld-musl-*.so*; do
        [ -e "$f" ] && return 0
    done
    return 1
}

# Resolve the binary relative to this script.
dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
bin=''
for c in "$dir/zora" "$dir/zig-out/bin/zora"; do
    [ -x "$c" ] && { bin=$c; break; }
done
[ -n "$bin" ] || { log "zora binary not found near $dir"; exit 1; }

# 1. Operator overrided LD_PRELOAD. Do not search.
if [ -n "${LD_PRELOAD:-}" ]; then
    log "LD_PRELOAD preset (${LD_PRELOAD}); not searching"
    exec "$bin" "$@"
fi

# 2. FreeBSD: jemalloc is already the system libc malloc — nothing to preload.
if [ "$(uname -s 2>/dev/null || echo unknown)" = FreeBSD ]; then
    log "FreeBSD; using system allocator (jemalloc)"
    exec "$bin" "$@"
fi

# 3. Discover jemalloc and preload it.
jph=$(find_jemalloc) || jph=''
if [ -n "$jph" ]; then
    LD_PRELOAD=$jph
    export LD_PRELOAD
    log "preloading jemalloc: $jph"
    exec "$bin" "$@"
fi

# 4. Fallback: cap glibc arenas; leave musl/other on the standard allocator.
#    An operator-set MALLOC_ARENA_MAX is respected (default applied only if unset).
if is_musl; then
    log "jemalloc not found; musl; standard allocator"
else
    : "${MALLOC_ARENA_MAX:=2}"
    export MALLOC_ARENA_MAX
    log "jemalloc not found; glibc; MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX"
fi

exec "$bin" "$@"
