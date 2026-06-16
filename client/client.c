#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "client.h"
#include "messages.h"


typedef struct client {
    ri_producer_t *command;
    ri_consumer_t *response;
    ri_consumer_t *event;
    sd_event_source *event_rsp;
    sd_event_source *event_evt;
    sd_event_source *event_exit;
    const msg_command_t *cmd_list;
} client_t;


static const msg_command_t cmd_list[] = {
  (msg_command_t) {
      .id = CMDID_HELLO,
      .args = {1, 2, 0},
  },
  (msg_command_t) {
      .id = CMDID_SENDEVENT,
      .args = {11, 20, 0},
  },
  (msg_command_t) {
      .id = CMDID_SENDEVENT,
      .args = {12, 20, 1},
  },
  (msg_command_t) {
      .id = CMDID_DIV,
      .args = {100, 7, 0},
  },
  (msg_command_t) {
      .id = CMDID_DIV,
      .args = {100, 0, 0},
  },
  (msg_command_t) {
      .id = CMDID_STOP,
      .args = {0, 0, 0},
  },
  (msg_command_t) {
      .id = CMDID_UNKNOWN,
  },
};


static void client_delete(client_t *client)
{
  if (client->event_rsp) {
    sd_event_source_set_enabled(client->event_rsp, SD_EVENT_OFF);
    sd_event_source_unref(client->event_rsp);
  }
  if (client->event_evt) {
    sd_event_source_set_enabled(client->event_evt, SD_EVENT_OFF);
    sd_event_source_unref(client->event_evt);
  }
  if (client->event_exit) {
    sd_event_source_set_enabled(client->event_exit, SD_EVENT_OFF);
    sd_event_source_unref(client->event_exit);
  }
  if (client->command)
    ri_producer_delete(client->command);
  if (client->response)
    ri_consumer_delete(client->response);
  if (client->event)
    ri_consumer_delete(client->event);
  free(client);
}


static int exit_handler(sd_event_source *s, void *userdata)
{
  client_t *client = userdata;
  client_delete(client);
  return 0;
}


static void send_cmd(client_t *client)
{
  const msg_command_t *cmd = client->cmd_list;

  if (cmd->id == CMDID_UNKNOWN)
    return;

  *(msg_command_t*)ri_producer_msg(client->command) = *cmd;
  ri_producer_force_push(client->command);
  client->cmd_list++;
}

static int rsp_handler(sd_event_source *es, int fd, uint32_t revents, void *userdata)
{
  client_t *client = userdata;
  int r = 0;
  ri_pop_result_t r_pop = ri_consumer_pop(client->response);

  if ((r_pop == RI_POP_RESULT_NO_MSG) || (r_pop == RI_POP_RESULT_NO_UPDATE)) {
    return 0;
  }

  const msg_response_t *rsp = ri_consumer_msg(client->response);
  LOG_INF("client received:");
  msg_response_print(rsp);

  send_cmd(client);

  return r;
}

int evt_handler(sd_event_source *es, int fd, uint32_t revents, void *userdata)
{
  client_t *client = userdata;

  ri_pop_result_t r_pop = ri_consumer_pop(client->event);

  if ((r_pop == RI_POP_RESULT_NO_MSG) || (r_pop == RI_POP_RESULT_NO_UPDATE)) {
    return 0;
  }

  const msg_event_t *evt = ri_consumer_msg(client->event);
  msg_event_print(evt);


  return 0;
}


client_t* client_new(ri_vector_t *vec, sd_event *event)
{
  client_t *client = calloc(1, sizeof(client_t));

  if (!client)
    goto fail_alloc;

  client->cmd_list = cmd_list;

  client->command = ri_vector_take_producer(vec, 0);
  if (!client->command)
    goto fail_channel;

  client->response = ri_vector_take_consumer(vec, 0);
  if (!client->response)
    goto fail_channel;

  client->event = ri_vector_take_consumer(vec, 1);
  if (!client->event)
    goto fail_channel;

  int fd = ri_consumer_eventfd(client->response);

  int r = sd_event_add_io(event, &client->event_rsp, fd, EPOLLIN, rsp_handler, client);
  if (r < 0)
    goto fail_channel;

  fd = ri_consumer_eventfd(client->event);
  r = sd_event_add_io(event, &client->event_evt, fd, EPOLLIN, evt_handler, client);
  if (r < 0)
    goto fail_channel;

  r = sd_event_add_exit(event, &client->event_exit, exit_handler, client);
  if (r < 0)
    goto fail_channel;

  send_cmd(client);

  return client;

fail_channel:
  client_delete(client);
fail_alloc:
  return NULL;
}


