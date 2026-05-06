#include <stdlib.h>

#include <systemd/sd-bus.h>

#include <rtipc.h>

#include "service.h"




int main(int argc, char *argv[]) {
  sd_event *event = NULL;

  int r = sd_event_default(&event);
  if (r < 0)
    goto fail_event;


  service_t *service = service_new(event);
  if (!service) {
    r = -1;
    goto fail_service;
  }

  sd_event_loop(event);

fail_service:
  sd_event_unref(event);
fail_event:
    return r < 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}
