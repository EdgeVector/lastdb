// CI-only linker wrappers. Record sizes and code addresses, never field data.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <execinfo.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CLASS_CAP 2048
#define SITE_CAP 8192
#define DEPTH 12

struct shape { size_t usable; uint64_t count, requested; };
struct site { size_t usable; uintptr_t frames[DEPTH]; uint64_t count, requested; };
static struct shape classes[CLASS_CAP];
static struct site sites[SITE_CAP];
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static _Atomic int enabled;
static _Thread_local int busy;
static uintptr_t image_base;
static const char *output_path;
static uint64_t lost_classes, lost_sites;

extern void *__real_mi_malloc_aligned(size_t, size_t);
extern void *__real_mi_zalloc_aligned(size_t, size_t);
extern void *__real_mi_realloc_aligned(void *, size_t, size_t);
extern size_t mi_usable_size(const void *);

static void record_allocation(void *pointer, size_t requested) {
    if (!pointer || !atomic_load_explicit(&enabled, memory_order_relaxed) || busy) return;
    busy = 1;
    size_t usable = mi_usable_size(pointer);
    uintptr_t frames[DEPTH] = {0};
    if (requested >= 4096) {
        void *raw[DEPTH];
        int count = backtrace(raw, DEPTH);
        for (int i = 0; i < count; i++) {
            Dl_info info;
            if (dladdr(raw[i], &info) && (uintptr_t)info.dli_fbase == image_base)
                frames[i] = (uintptr_t)raw[i] - image_base;
        }
    }
    // Resolve unwind/module metadata before the lock to avoid loader lock inversion.
    pthread_mutex_lock(&lock);
    size_t slot = (usable * 17) % CLASS_CAP;
    size_t probe;
    for (probe = 0; probe < CLASS_CAP; probe++, slot = (slot + 1) % CLASS_CAP) {
        if (!classes[slot].count || classes[slot].usable == usable) {
            classes[slot].usable = usable;
            classes[slot].count++;
            classes[slot].requested += requested;
            break;
        }
    }
    if (probe == CLASS_CAP) lost_classes++;
    if (requested >= 4096) {
        size_t hash = usable;
        for (int i = 0; i < DEPTH; i++) hash = hash * 33 + frames[i];
        slot = hash % SITE_CAP;
        for (probe = 0; probe < SITE_CAP; probe++, slot = (slot + 1) % SITE_CAP) {
            struct site *entry = &sites[slot];
            if (!entry->count || (entry->usable == usable &&
                                 memcmp(entry->frames, frames, sizeof(frames)) == 0)) {
                entry->usable = usable;
                memcpy(entry->frames, frames, sizeof(frames));
                entry->count++;
                entry->requested += requested;
                break;
            }
        }
        if (probe == SITE_CAP) lost_sites++;
    }
    pthread_mutex_unlock(&lock);
    busy = 0;
}

void *__wrap_mi_malloc_aligned(size_t size, size_t alignment) {
    void *pointer = __real_mi_malloc_aligned(size, alignment);
    record_allocation(pointer, size);
    return pointer;
}

void *__wrap_mi_zalloc_aligned(size_t size, size_t alignment) {
    void *pointer = __real_mi_zalloc_aligned(size, alignment);
    record_allocation(pointer, size);
    return pointer;
}

void *__wrap_mi_realloc_aligned(void *old, size_t size, size_t alignment) {
    void *pointer = __real_mi_realloc_aligned(old, size, alignment);
    record_allocation(pointer, size);
    return pointer;
}

__attribute__((constructor)) static void begin_profile(void) {
    const char *actions = getenv("GITHUB_ACTIONS");
    const char *runner = getenv("RUNNER_ENVIRONMENT");
    output_path = getenv("LASTDB_ALLOCATION_PROFILE");
    if (!output_path || !actions || strcmp(actions, "true") != 0 ||
        !runner || strcmp(runner, "github-hosted") != 0) return;
    Dl_info info;
    if (!dladdr((void *)&begin_profile, &info)) return;
    image_base = (uintptr_t)info.dli_fbase;
    atomic_store_explicit(&enabled, 1, memory_order_relaxed);
}

__attribute__((destructor)) static void end_profile(void) {
    if (!atomic_exchange_explicit(&enabled, 0, memory_order_relaxed)) return;
    FILE *output = fopen(output_path, "w");
    if (!output) return;
    pthread_mutex_lock(&lock);
    fprintf(output, "{\"lost_classes\":%llu,\"lost_sites\":%llu,\"classes\":[",
            (unsigned long long)lost_classes, (unsigned long long)lost_sites);
    int separator = 0;
    for (size_t i = 0; i < CLASS_CAP; i++) {
        struct shape *entry = &classes[i];
        if (!entry->count) continue;
        fprintf(output, "%s{\"usable\":%zu,\"count\":%llu,\"requested\":%llu}",
                separator++ ? "," : "", entry->usable,
                (unsigned long long)entry->count, (unsigned long long)entry->requested);
    }
    fputs("],\"sites\":[", output);
    separator = 0;
    for (size_t i = 0; i < SITE_CAP; i++) {
        struct site *entry = &sites[i];
        if (!entry->count) continue;
        fprintf(output, "%s{\"usable\":%zu,\"count\":%llu,\"requested\":%llu,\"frames\":[",
                separator++ ? "," : "", entry->usable,
                (unsigned long long)entry->count, (unsigned long long)entry->requested);
        for (int frame = 0; frame < DEPTH; frame++)
            fprintf(output, "%s%llu", frame ? "," : "", (unsigned long long)entry->frames[frame]);
        fputs("]}", output);
    }
    fputs("]}\n", output);
    pthread_mutex_unlock(&lock);
    fclose(output);
}
