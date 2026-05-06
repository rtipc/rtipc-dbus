#include "service.h"


#include <stdlib.h>
#include <unistd.h>

#include <rtipc.h>


#include "server.h"

#define SERVICE_NAME "org.rtipc.server"


typedef struct service {
  sd_event *event;
  sd_bus *bus;
  sd_bus_slot *slot;
  server_t *server;
} service_t;


static int read_channels(sd_bus_message *m, ri_channel_t channels[]) {
  int cnt = 0;

  int r = sd_bus_message_enter_container(m, SD_BUS_TYPE_ARRAY, "(uubay)");
  if (r < 0) {
    LOG_ERR("failed to enter array: %s", strerror(-r));
    return r;
  }

  for (;;) {
    r = sd_bus_message_enter_container(m, SD_BUS_TYPE_STRUCT, "uubay");

    if (r == 0) {
      break;
    } else if (r < 0) {
      LOG_ERR("failed to enter channel[%d]: %s", cnt, strerror(-r));
      return r;
    }

    if (channels) {
      ri_channel_t *channel = &channels[cnt];
      r = sd_bus_message_read(m, "uub", &channel->add_msgs, &channel->msg_size, &channel->eventfd);
      if (r < 0) {
        LOG_ERR("failed to read channel[%d] %s", cnt, strerror(-r));
        return r;
      }

      r = sd_bus_message_read_array(m, 'y', &channel->info.data, &channel->info.size);
      if (r < 0) {
        LOG_ERR("failed to read channel[%d] info %s", cnt, strerror(-r));
        return r;
      }
    } else {
      r = sd_bus_message_skip(m, "uubay");
      if (r < 0) {
        LOG_ERR("failed to skip channel[%d] %s", cnt, strerror(-r));
        return r;
      }
    }

    r = sd_bus_message_exit_container(m);
    if (r < 0) {
      LOG_ERR("failed to exit channel[%d]: %s", cnt, strerror(-r));
      return r;
    }
    cnt++;
  }

  r = sd_bus_message_exit_container(m);
  if (r < 0) {
    LOG_ERR("failed to exit array: %s", strerror(-r));
    return r;
  }

  return cnt;
}


static ri_resource_t* create_resource(sd_bus_message *m)
{
  unsigned n_consumers = 0;
  unsigned n_producers = 0;
  ri_info_t info;

  int r = sd_bus_message_skip(m, "h");
  if (r < 0) {
    LOG_ERR("failed to skip memfd %s", strerror(-r));
    return NULL;
  }

  r = read_channels(m, NULL);
  if (r < 0) {
    LOG_ERR("failed to count consumer channels %s", strerror(-r));
    return NULL;
  }

  n_consumers = r;

  r = sd_bus_message_skip(m, "ah");
  if (r < 0) {
    LOG_ERR("failed to skip consumer eventfds %s", strerror(-r));
    return NULL;
  }

  r = read_channels(m, NULL);
  if (r < 0) {
    LOG_ERR("failed to count producer channels %s", strerror(-r));
    return NULL;
  }

  n_producers = r;

  r = sd_bus_message_skip(m, "ah");
  if (r < 0) {
    LOG_ERR("failed to skip producer eventfds %s", strerror(-r));
    return NULL;
  }

  r = sd_bus_message_read_array(m, 'y', &info.data, &info.size);
  if (r < 0) {
    LOG_ERR("failed to read vector info %s", strerror(-r));
    return NULL;
  }

  r = sd_bus_message_rewind(m, true);
  if (r < 0) {
    LOG_ERR("failed to rewind message %s", strerror(-r));
    return NULL;
  }

  return ri_resource_new(n_consumers, n_producers, &info);
}


static int read_eventfds(sd_bus_message *m, ri_channel_t channels[]) {
  int r = sd_bus_message_enter_container(m, SD_BUS_TYPE_ARRAY, "h");
  if (r < 0) {
    LOG_ERR("enter array failed failed: %s", strerror(-r));
    return r;
  }
  if (channels) {
    for (ri_channel_t *channel = channels; channel->msg_size != 0; channel++) {
      if (channel->eventfd <= 0)
        continue;
      int fd = -1;
      r = sd_bus_message_read(m, "h", &fd);
      if (r < 0) {
        LOG_ERR("read eventfd failed %s", strerror(-r));
        return r;
      }
      channel->eventfd = dup(fd);
      if (channel->eventfd < 0) {
        r = -errno;
        LOG_ERR("read eventfd failed %s", strerror(-r));
        return r;
      }
    }
  }

  r = sd_bus_message_exit_container(m);
  if (r < 0) {
    LOG_ERR("exit array failed failed %s", strerror(-r));
  }
  return r;
}

static int method_connect(sd_bus_message *m, void *userdata, sd_bus_error *ret_error)
{
  int r = -1;
  const int *fds;
  size_t n_fds;
  service_t *service = userdata;

  if (service->server) {
    LOG_ERR("already connected");
    goto fail_rsc;
  }

  ri_resource_t* rsc = create_resource(m);

  if (!rsc) {
    LOG_ERR("alloc_resource failed");
    goto fail_rsc;
  }

  int fd = -1;
  r = sd_bus_message_read(m, "h", &fd);
  if (r < 0) {
    LOG_ERR("Failed to read shmfd: %s", strerror(-r));
    goto fail_parse;
  }

  rsc->shmfd = dup(fd);
  if (rsc->shmfd < 0) {
    r = -errno;
    LOG_ERR("read eventfd failed %s", strerror(-r));
    goto fail_parse;
  }

  r = read_channels(m, rsc->consumers);

  if (r < 0) {
    LOG_ERR("Failed to read consumers channels");
    goto fail_parse;
  }

  r = read_eventfds(m, rsc->consumers);
  if (r < 0) {
    LOG_ERR("Failed to read consumer eventfds");
    goto fail_parse;
  }

  r = read_channels(m, rsc->producers);
  if (r < 0) {
    LOG_ERR("Failed to read producer channels");
    goto fail_parse;
  }

  r = read_eventfds(m, rsc->producers);
  if (r < 0) {
    LOG_ERR("Failed to read producers eventfds");
    goto fail_parse;
  }

  ri_vector_t *vec = ri_vector_new(rsc, true);
  if (!vec) {
    LOG_ERR("ri_vector_new failed");
    goto fail_parse;
  }

  ri_vector_init_shm(vec);

  /* server delete itself when eventloop exits */
  server_t *server = server_new(service->event, vec);
  if (!server) {
    LOG_ERR("server_new failed");
    goto fail_server;
  }

  r = sd_bus_reply_method_return(m, "");
  if (r < 0) {
    LOG_ERR("sd_bus_reply_method_return failed");
    goto fail_server;
  }

  ri_resource_delete(rsc);
  ri_vector_delete(vec);

  return r;
fail_server:
  ri_vector_delete(vec);
fail_parse:
  ri_resource_delete(rsc);
fail_rsc:
  return r;
}


static const sd_bus_vtable rtipc_vtable[] = {
    SD_BUS_VTABLE_START(0),
    SD_BUS_METHOD_WITH_ARGS("Connect",
                            SD_BUS_ARGS("h", shmfd, "a(uubay)", consumers, "ah", consumer_eventfds, "a(uubay)", producers, "ah", producer_eventfds, "ay", info),
                            SD_BUS_NO_RESULT,
                            method_connect,
                            SD_BUS_VTABLE_UNPRIVILEGED),
    SD_BUS_VTABLE_END
};


service_t *service_new(sd_event* event)
{
  service_t *service = calloc(1, sizeof(service_t));
  if (!service)
    goto fail_alloc;

  service->event = event;

  int r = sd_bus_open_user(&service->bus);
  if (r < 0) {
    LOG_ERR("Failed to connect to user bus: %s", strerror(-r));
    goto fail_open_bus;
  }

  r = sd_bus_request_name(service->bus, SERVICE_NAME, 0);
  if (r < 0) {
    LOG_ERR("Failed to acquire service name: %s", strerror(-r));
    goto fail_req_name;
  }

  r = sd_bus_add_object_vtable(service->bus, &service->slot, "/org/rtipc/server", "org.rtipc.server", rtipc_vtable, service);
  if (r < 0) {
    LOG_ERR("Failed to acquire service name: %s", strerror(-r));
    goto fail_add_object;
  }


  r = sd_bus_attach_event(service->bus, event, SD_EVENT_PRIORITY_NORMAL);
  if (r < 0) {
    LOG_ERR("Failed to attach dbus to event: %s", strerror(-r));
    goto fail_add_object;
  }

  return service;

fail_add_object:
  sd_bus_release_name(service->bus, SERVICE_NAME);
fail_req_name:
  sd_bus_close(service->bus);
fail_open_bus:
  free(service);
fail_alloc:
  return NULL;
}


void service_delete(service_t * service)
{
  sd_bus_detach_event(service->bus);
  sd_bus_release_name(service->bus, SERVICE_NAME);
  sd_bus_close(service->bus);
  free(service);
}
