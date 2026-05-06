#pragma once

#include <systemd/sd-bus.h>

typedef struct service service_t;

service_t* service_new(sd_event* event);

void service_delete(service_t *service);
