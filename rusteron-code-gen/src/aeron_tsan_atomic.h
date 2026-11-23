/*
 * Compiler atomics for ThreadSanitizer builds of Aeron.
 *
 * Aeron's normal headers use volatile accesses and fences, plus inline assembly
 * on x86. TSan cannot observe their synchronization. This header replaces only
 * those operations, with the same sizes and memory orders. It adds no shared
 * lock or artificial happens-before edge. Production builds do not include it.
 *
 * build_common.rs puts this file in a private quote-include directory under
 * both native platform header names. aeron_atomic.h supplies the assertions.
 */
#ifndef RUSTERON_TSAN_ATOMIC_H
#define RUSTERON_TSAN_ATOMIC_H

#ifndef __has_feature
#define __has_feature(feature) 0
#endif
#if !defined(__SANITIZE_THREAD__) && !__has_feature(thread_sanitizer)
#error "aeron_tsan_atomic.h requires -fsanitize=thread"
#endif

#include <stdbool.h>
#include <stdint.h>

#define AERON_ATOMIC64_GCC_X86_64_H
#define AERON_ATOMIC64_C11_H

#define AERON_GET_ACQUIRE(dst, src) \
    do \
    { \
        AERON_ATOMIC_ASSERT_VOLATILE_LVALUE(src, "AERON_GET_ACQUIRE: src must be a volatile lvalue"); \
        (dst) = __atomic_load_n(&(src), __ATOMIC_ACQUIRE); \
    } while (0)

#define AERON_SET_RELEASE(dst, src) \
    do \
    { \
        AERON_ATOMIC_ASSERT_VOLATILE_LVALUE(dst, "AERON_SET_RELEASE: dst must be a volatile lvalue"); \
        __atomic_store_n(&(dst), (src), __ATOMIC_RELEASE); \
    } while (0)

#define AERON_GET_AND_ADD_INT64(original, dst, value) \
    do \
    { \
        (original) = __atomic_fetch_add(&(dst), (value), __ATOMIC_SEQ_CST); \
    } while (0)

#define AERON_GET_AND_ADD_INT32(original, dst, value) \
    AERON_GET_AND_ADD_INT64(original, dst, value)

inline bool aeron_cas_int64(volatile int64_t *dst, int64_t expected, int64_t desired)
{
    return __atomic_compare_exchange_n(dst, &expected, desired, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

inline bool aeron_cas_uint64(volatile uint64_t *dst, uint64_t expected, uint64_t desired)
{
    return __atomic_compare_exchange_n(dst, &expected, desired, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

inline bool aeron_cas_int32(volatile int32_t *dst, int32_t expected, int32_t desired)
{
    return __atomic_compare_exchange_n(dst, &expected, desired, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

/* Keep standalone fences. A global TSan annotation here would hide real races. */
inline void aeron_acquire(void)
{
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
}

inline void aeron_release(void)
{
    __atomic_thread_fence(__ATOMIC_RELEASE);
}

#define AERON_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))

#endif
