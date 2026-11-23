#if defined(__linux__)
#define _GNU_SOURCE
#endif

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "aeron_driver_context.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_transport_poller.h"

/* A small C transport library with a static table and one owned runtime.
 * It delegates UDP traffic to Aeron. Only setup and close update these counters. */
typedef struct links_test_runtime_stct
{
    _Atomic uint64_t transports;
    _Atomic uint64_t pollers;
    _Atomic uint64_t transport_closes;
    _Atomic uint64_t poller_closes;
    _Atomic uint64_t sends;
    _Atomic uint64_t receives;
}
links_test_runtime_t;

typedef struct links_test_transport_state_stct
{
    links_test_runtime_t *runtime;
    uint64_t sends;
    uint64_t receives;
}
links_test_transport_state_t;

typedef struct links_test_poller_state_stct
{
    links_test_runtime_t *runtime;
    void *udp_state;
}
links_test_poller_state_t;

static const char runtime_key[] = "links-test.transport-runtime";
static _Atomic uint64_t runtime_closes;

void *links_test_runtime_open(void)
{
    links_test_runtime_t *runtime = calloc(1, sizeof(links_test_runtime_t));
    if (NULL != runtime)
    {
        atomic_init(&runtime->transports, 0);
        atomic_init(&runtime->pollers, 0);
        atomic_init(&runtime->transport_closes, 0);
        atomic_init(&runtime->poller_closes, 0);
        atomic_init(&runtime->sends, 0);
        atomic_init(&runtime->receives, 0);
    }
    return runtime;
}

int links_test_runtime_install(aeron_driver_context_t *context, void *runtime)
{
    aeron_driver_context_bindings_clientd_entry_t *entry =
        aeron_driver_context_bindings_clientd_get_or_find_first_free_entry(context, runtime_key);
    if (NULL == entry)
    {
        return -1;
    }
    entry->clientd = runtime;
    return 0;
}

static links_test_runtime_t *runtime_from_context(aeron_driver_context_t *context)
{
    int index = aeron_driver_context_bindings_clientd_find(context, runtime_key);
    return index < 0 ? NULL : context->bindings_clientd_entries[index].clientd;
}

static int runtime_transport_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    struct sockaddr_storage *connect_addr,
    aeron_udp_channel_transport_params_t *params,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    links_test_runtime_t *runtime = runtime_from_context(context);
    if (NULL == runtime)
    {
        return -1;
    }
    /* A C transport can require Rust sender/receiver strategies to busy-spin. */
    if (0 != strcmp(context->sender_idle_strategy_name, "rusteron-spin") ||
        0 != strcmp(context->receiver_idle_strategy_name, "rusteron-spin"))
    {
        return -1;
    }
    links_test_transport_state_t *state = calloc(1, sizeof(links_test_transport_state_t));
    if (NULL == state)
    {
        return -1;
    }
    int result = aeron_udp_channel_transport_init(
        transport, bind_addr, multicast_if_addr, connect_addr, params, context, affinity);
    if (result >= 0)
    {
        state->runtime = runtime;
        transport->bindings_clientd = state;
        atomic_fetch_add(&runtime->transports, 1);
    }
    else
    {
        free(state);
    }
    return result;
}

static int runtime_transport_close(aeron_udp_channel_transport_t *transport)
{
    links_test_transport_state_t *state = transport->bindings_clientd;
    int result = aeron_udp_channel_transport_close(transport);
    if (NULL != state)
    {
        links_test_runtime_t *runtime = state->runtime;
        transport->bindings_clientd = NULL;
        atomic_fetch_sub(&runtime->transports, 1);
        atomic_fetch_add(&runtime->transport_closes, 1);
        atomic_fetch_add(&runtime->sends, state->sends);
        atomic_fetch_add(&runtime->receives, state->receives);
        free(state);
    }
    return result;
}

static int runtime_send(
    aeron_udp_channel_data_paths_t *paths,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *vectors,
    size_t length,
    int64_t *bytes_sent)
{
    links_test_transport_state_t *state = transport->bindings_clientd;
    state->sends++;
    return aeron_udp_channel_transport_send(paths, transport, address, vectors, length, bytes_sent);
}

static int runtime_receive(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *vectors,
    size_t length,
    int64_t *bytes_received,
    aeron_udp_transport_recv_func_t receive,
    void *clientd)
{
    links_test_transport_state_t *state = transport->bindings_clientd;
    int result = aeron_udp_channel_transport_recvmmsg(transport, vectors, length, bytes_received, receive, clientd);
    if (result > 0)
    {
        state->receives++;
    }
    return result;
}

static int runtime_poller_init(
    aeron_udp_transport_poller_t *poller,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    links_test_runtime_t *runtime = runtime_from_context(context);
    if (NULL == runtime)
    {
        return -1;
    }
    links_test_poller_state_t *state = calloc(1, sizeof(links_test_poller_state_t));
    if (NULL == state)
    {
        return -1;
    }
    int result = aeron_udp_transport_poller_init(poller, context, affinity);
    if (result >= 0)
    {
        state->runtime = runtime;
        state->udp_state = poller->bindings_clientd;
        poller->bindings_clientd = state;
        atomic_fetch_add(&runtime->pollers, 1);
    }
    else
    {
        free(state);
    }
    return result;
}

static int runtime_poller_close(aeron_udp_transport_poller_t *poller)
{
    links_test_poller_state_t *state = poller->bindings_clientd;
    if (NULL != state)
    {
        poller->bindings_clientd = state->udp_state;
    }
    int result = aeron_udp_transport_poller_close(poller);
    if (NULL != state)
    {
        links_test_runtime_t *runtime = state->runtime;
        poller->bindings_clientd = NULL;
        atomic_fetch_sub(&runtime->pollers, 1);
        atomic_fetch_add(&runtime->poller_closes, 1);
        free(state);
    }
    return result;
}

/* Native UDP owns poller->bindings_clientd for its epoll/poll storage. Restore
 * that pointer around every delegated operation, then retain its updated value.
 * A poller belongs to one agent, so these substitutions never overlap. */
static int runtime_poller_add(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    links_test_poller_state_t *state = poller->bindings_clientd;
    poller->bindings_clientd = state->udp_state;
    int result = aeron_udp_transport_poller_add(poller, transport);
    state->udp_state = poller->bindings_clientd;
    poller->bindings_clientd = state;
    return result;
}

static int runtime_poller_remove(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    links_test_poller_state_t *state = poller->bindings_clientd;
    poller->bindings_clientd = state->udp_state;
    int result = aeron_udp_transport_poller_remove(poller, transport);
    state->udp_state = poller->bindings_clientd;
    poller->bindings_clientd = state;
    return result;
}

static int runtime_poller_poll(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *vectors,
    size_t length,
    int64_t *bytes_received,
    aeron_udp_transport_recv_func_t receive,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg,
    void *clientd)
{
    links_test_poller_state_t *state = poller->bindings_clientd;
    poller->bindings_clientd = state->udp_state;
    int result = aeron_udp_transport_poller_poll(poller, vectors, length, bytes_received, receive, recvmmsg, clientd);
    state->udp_state = poller->bindings_clientd;
    poller->bindings_clientd = state;
    return result;
}

static const aeron_udp_channel_transport_bindings_t runtime_bindings =
{
    runtime_transport_init,
    aeron_udp_channel_transport_reconnect,
    runtime_transport_close,
    runtime_receive,
    runtime_send,
    aeron_udp_channel_transport_get_so_rcvbuf,
    aeron_udp_channel_transport_bind_addr_and_port,
    runtime_poller_init,
    runtime_poller_close,
    runtime_poller_add,
    runtime_poller_remove,
    runtime_poller_poll,
    { "owned-test-udp", "media", NULL }
};

const aeron_udp_channel_transport_bindings_t *links_test_runtime_bindings(void)
{
    return &runtime_bindings;
}

void links_test_runtime_snapshot(void *handle, uint64_t *values)
{
    links_test_runtime_t *runtime = handle;
    values[0] = atomic_load(&runtime->transports);
    values[1] = atomic_load(&runtime->pollers);
    values[2] = atomic_load(&runtime->transport_closes);
    values[3] = atomic_load(&runtime->poller_closes);
    values[4] = atomic_load(&runtime->sends);
    values[5] = atomic_load(&runtime->receives);
}

void links_test_runtime_close(void *runtime)
{
    free(runtime);
    atomic_fetch_add(&runtime_closes, 1);
}

uint64_t links_test_runtime_close_count(void)
{
    return atomic_load(&runtime_closes);
}
