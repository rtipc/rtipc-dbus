#include <stdlib.h>

#include <systemd/sd-bus.h>

#include <rtipc.h>

#include "client.h"
#include "messages.h"

#ifndef _cleanup_
#define _cleanup_(f) __attribute__((cleanup(f)))
#endif



ri_vector_t* rtipc_connect(sd_bus *bus, const ri_config_t *config) {
  int r = -1;
  _cleanup_(sd_bus_message_unrefp) sd_bus_message *msg = NULL;
  _cleanup_(sd_bus_message_unrefp) sd_bus_message *reply = NULL;
  _cleanup_(sd_bus_error_free) sd_bus_error error = SD_BUS_ERROR_NULL;

  ri_vector_t *vec = ri_vector_new(config);
  if (!vec) {
    LOG_ERR("ri_transfer_new failed");
    goto fail_vec;
  }

  size_t req_size = ri_vector_serialize_size(vec);

  uint8_t *req = malloc(req_size);
  if (!req) {
    r = -ENOMEM;
    goto fail_req;
  }

  int fds[100];
  unsigned n_fds = 100;
  r = ri_vector_serialize(vec, req, req_size, fds, &n_fds);
  if (r < 0) {
    LOG_ERR("ri_resource_serialize failed");
    goto fail_serialize;
  }

  r = sd_bus_message_new_method_call(bus, &msg, "org.rtipc.server", "/org/rtipc/server", "org.rtipc.server", "Connect");
  if (r < 0) {
    LOG_ERR("sd_bus_message_new_method_call failed");
    goto fail_call;
  }

  r = sd_bus_message_append_array(msg, SD_BUS_TYPE_BYTE, req, req_size);
  if (r < 0) {
    LOG_ERR("sd_bus_message_append_array req failed");
    goto fail_call;
  }


  r = sd_bus_message_open_container(msg, SD_BUS_TYPE_ARRAY, "h");
  if (r < 0) {
    LOG_ERR("sd_bus_message_open_container failed");
    goto fail_call;
  }

  for (unsigned i = 0; i < n_fds; i++) {
    r = sd_bus_message_append_basic(msg, SD_BUS_TYPE_UNIX_FD, &fds[i]);
    if (r < 0) {
      LOG_ERR("sd_bus_message_append_basic fd failed");
      goto fail_call;
    }
  }

  r = sd_bus_message_close_container(msg);
  if (r < 0) {
    LOG_ERR("sd_bus_message_close_container fd failed");
    goto fail_call;
  }

  r = sd_bus_call(bus, msg, 0, &error, &reply);
  if (r < 0) {
    LOG_ERR("sd_bus_call failed");
    goto fail_call;
  }


  free(req);

  return vec;

fail_call:
fail_serialize:
  free(req);
fail_req:
  ri_vector_delete(vec);
fail_vec:
  return NULL;
}

const ri_channel_t client2server_channels[] = {
    (ri_channel_t) {.add_msgs = 0,
                    .msg_size = sizeof(msg_command_t),
                    .eventfd = 1,
                    .info = {.data = COMMAND_INFO, .size = sizeof(COMMAND_INFO)}},
    {0},
};

const ri_channel_t server2client_channels[] = {
    (ri_channel_t) {.add_msgs = 0,
                    .msg_size = sizeof(msg_response_t),
                    .eventfd = 1,
                    .info = {.data = RESPONSE_INFO, .size = sizeof(RESPONSE_INFO)}},
    (ri_channel_t) {.add_msgs = 10,
                    .msg_size = sizeof(msg_event_t),
                    .eventfd = 1,
                    .info = {.data = EVENT_INFO, .size = sizeof(EVENT_INFO)}},
    {0},
};

int main(int argc, char *argv[]) {
  const ri_config_t config = {
      .consumers = server2client_channels,
      .producers = client2server_channels,
  };
  sd_event *event = NULL;
  sd_bus *bus = NULL;

  int r = sd_event_default(&event);
  if (r < 0)
    goto fail_event;

  r = sd_bus_open_user(&bus);
  if (r < 0)
    goto fail_bus;

  r = sd_bus_attach_event(bus, event, 0);
  if (r < 0) {
    goto fail_attach;
  }

  ri_vector_t* vec = rtipc_connect(bus, &config);
  if (!vec) {
    goto fail_vec;
  }

  client_new(vec, event);

  sd_event_loop(event);

fail_vec:
fail_attach:
fail_bus:
  sd_event_unref(event);
fail_event:
    return r < 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}
