#pragma once


#include <systemd/sd-event.h>

#include <rtipc.h>

typedef struct client client_t;

client_t* client_new(ri_vector_t *vec, sd_event *event);
