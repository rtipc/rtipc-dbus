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



static int* read_fds(sd_bus_message *m, unsigned *n)
{
  unsigned n_fds = 0;

  int r = sd_bus_message_enter_container(m, SD_BUS_TYPE_ARRAY, "h");
  if (r < 0) {
    LOG_ERR("sd_bus_message_enter_container failed %i", r);
    goto fail_cnt;
  }

  for (;;) {
    r = sd_bus_message_read_basic(m, 'h', NULL);
    if (r > 0) {
      n_fds++;
    } else if (r == 0) {
      break;
    } else {
      LOG_ERR("sd_bus_message_read_basic failed %i", r);
      goto fail_cnt;
    }
  }


  if (n_fds == 0) {
    LOG_ERR("message contains no file descriptoy");
    goto fail_cnt;
  }

  r = sd_bus_message_rewind(m, false);
  if (r < 0) {
    LOG_ERR("sd_bus_message_rewind failed %i", r);
    goto fail_cnt;
  }

  int *fds = malloc(n_fds * sizeof(int));
  if (!fds) {
    goto fail_alloc;
  }

  /* initialize fds with invalid file descriptors */
  for (int i = 0; i < n_fds; i++)
    fds[i] = -1;

  for (int i = 0; i < n_fds; i++) {
    int fd;

    r = sd_bus_message_read_basic(m, 'h', &fd);
    if (r < 0) {
      LOG_ERR("sd_bus_message_read_basic failed %i", r);
      goto fail_read;
    }


    fds[i] = dup(fd);
    if (fds[i] < 0) {
      const char *strerr = strerror(errno);
      LOG_ERR("dup of fd[%i] failed: %s", i, strerr);
      goto fail_read;
    }
  }

  r = sd_bus_message_exit_container(m);

  if (r < 0) {
    LOG_ERR("sd_bus_message_exit_container failed %i", r);
    goto fail_read;
  }
  if (n)
    *n = n_fds;

  return fds;

fail_read:
  for (int i = 0; i < n_fds; i++) {
    if (fds[i] < 0)
      break;
     close(fds[i]);
  }

  free(fds);

fail_alloc:
fail_cnt:
  return NULL;
}


static int method_connect(sd_bus_message *m, void *userdata, sd_bus_error *ret_error)
{
  service_t *service = userdata;
  int r = -1;
  const void *request;
  size_t request_size;
  const void *msg_fds;

  if (service->server) {
    LOG_ERR("already connected");
    goto fail_parse;
  }

  r = sd_bus_message_read_array(m, 'y', &request, &request_size);
  if (r < 0) {
    LOG_ERR("failed to read request %s", strerror(-r));
    goto fail_parse;
  }

  unsigned n_fds;
  int *fds = read_fds(m, &n_fds);
  if (!fds)
    goto fail_fds;

  ri_vector_t* vec = ri_vector_deserialize(request, request_size, fds, &n_fds);
  if (!vec) {
    LOG_ERR("ri_vector_deserialize failed");
    goto fail_vec;
  }

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

  ri_vector_delete(vec);
  free(fds);

  return r;

fail_server:
  ri_vector_delete(vec);
fail_vec:
  free(fds);
fail_fds:
fail_parse:
  return r;
}


static const sd_bus_vtable rtipc_vtable[] = {
    SD_BUS_VTABLE_START(0),
    SD_BUS_METHOD_WITH_ARGS("Connect",
                            SD_BUS_ARGS("ay", request, "ah", fds),
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
