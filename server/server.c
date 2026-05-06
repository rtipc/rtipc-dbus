#include "server.h"

#include <stdlib.h>
#include <unistd.h>


#include "rtipc.h"
#include "messages.h"

#define MAX_CYCLES 10000

struct server {
    ri_consumer_t *command;
    ri_producer_t *response;
    ri_producer_t *async;
    sd_event_source *event_io;
    sd_event_source *event_exit;
};



static int32_t server_send_async(ri_producer_t *producer, uint32_t id, unsigned num, bool force)
{
  for (unsigned i = 0; i < num; i++) {
    msg_event_t *event = ri_producer_msg(producer);
    event->id = id;
    event->nr = i;
    if (force) {
      ri_producer_force_push(producer);
    } else {
      if (ri_producer_try_push(producer) == RI_TRY_PUSH_RESULT_FAIL) {
        return i;
      }
    }
  }
  return num;
}


static int32_t server_div(int32_t a, int32_t b, int32_t *res)
{
  if (b == 0) {
    return -1;
  } else {
    *res = a / b;
    return 0;
  }
}

static int exit_handler(sd_event_source *s, void *userdata)
{
  server_t *server = userdata;
  server_delete(server);
  return 0;
}

static int cmd_handler(sd_event_source *es, int fd, uint32_t revents, void *userdata)
{
  server_t *server = userdata;
  int r = 0;
  ri_pop_result_t r_pop = ri_consumer_pop(server->command);

  if ((r_pop == RI_POP_RESULT_NO_MSG) || (r_pop == RI_POP_RESULT_NO_UPDATE)) {
    return 0;
  }

  const msg_command_t *cmd = ri_consumer_msg(server->command);
  LOG_INF("server received:");
  msg_command_print(cmd);

  msg_response_t *rsp = ri_producer_msg(server->response);

  rsp->id = cmd->id;
  switch (cmd->id) {
    case CMDID_HELLO:
      rsp->result = 0;
      break;
    case CMDID_STOP:
      r = -1;
      rsp->result = 0;
      break;
    case CMDID_SENDEVENT:
      rsp->result = server_send_async(server->async, cmd->args[0], cmd->args[1], cmd->args[2]);
      break;
    case CMDID_DIV:
      rsp->result = server_div(cmd->args[0], cmd->args[1], &rsp->data);
      break;
    default:
      rsp->result = -1;
      break;
  }
  ri_producer_force_push(server->response);

  return r;
}



static void server_print_info(const server_t* server)
{
  ri_info_t info = ri_consumer_info(server->command);
  LOG_INF("command name = %s", (const char*)info.data);

  info = ri_producer_info(server->response);
  LOG_INF("response name = %s", (const char*)info.data);

  info = ri_producer_info(server->async);
  LOG_INF("async name = %s", (const char*)info.data);

}


void server_delete(server_t* server)
{
  if (server->event_io) {
    sd_event_source_set_enabled(server->event_io, SD_EVENT_OFF);
    sd_event_source_unref(server->event_io);
  }
  if (server->event_exit) {
    sd_event_source_set_enabled(server->event_exit, SD_EVENT_OFF);
    sd_event_source_unref(server->event_exit);
  }
  if (server->command)
    ri_consumer_delete(server->command);
  if (server->response)
    ri_producer_delete(server->response);
  if (server->async)
    ri_producer_delete(server->async);
  free(server);
}


server_t* server_new(sd_event *event, ri_vector_t *vec)
{
  server_t *server = calloc(1, sizeof(server_t));

  if (!server)
    goto fail_alloc;

  server->command = ri_vector_take_consumer(vec, 0);
  if (!server->command)
    goto fail_channel;

  server->response = ri_vector_take_producer(vec, 0);
  if (!server->response)
    goto fail_channel;

  server->async = ri_vector_take_producer(vec, 1);
  if (!server->async)
    goto fail_channel;

  int fd = ri_consumer_eventfd(server->command);

  int r = sd_event_add_io(event, &server->event_io, fd, EPOLLIN, cmd_handler, server);
  if (r < 0)
    goto fail_channel;

  r = sd_event_add_exit(event, &server->event_exit, exit_handler, server);
  if (r < 0)
    goto fail_channel;

  server_print_info(server);

  return server;

fail_channel:
  server_delete(server);
fail_alloc:
  return NULL;
}





