#pragma once

#include <systemd/sd-event.h>

#include <rtipc.h>

typedef struct server server_t;


server_t* server_new(sd_event *event, ri_vector_t *vec);
void server_delete(server_t* server);
