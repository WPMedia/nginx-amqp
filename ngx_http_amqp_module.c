#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_md5.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <syslog.h>

#define NGX_AMQP_SOCKET_CREATE_FAILURE 1
#define NGX_AMQP_SOCKET_OPEN_FAILURE 2
#define MAX_PUBLISH_RETRIES 5

typedef struct {
  ngx_str_t         amqp_ip;
  ngx_uint_t        amqp_port;
  ngx_str_t         amqp_exchange;
  ngx_str_t         amqp_queue;
  ngx_str_t         amqp_user;
  ngx_str_t         amqp_password;
} amqp_connection_config_t;

typedef struct {
  amqp_socket_t*                socket;
  amqp_connection_state_t       conn;
  ngx_uint_t 			is_connected;
} amqp_connection_t;

typedef struct{
    amqp_connection_config_t    connection_config;
    amqp_connection_t           connection;
    ngx_uint_t                  amqp_debug;
    ngx_str_t                   message_to_publish;
    ngx_array_t*                message_lengths;
    ngx_array_t*                message_values;
} ngx_http_amqp_conf_t;

ngx_str_t application_types[] = {
  ngx_string("application/json")
};

static void ngx_http_amqp_exit(ngx_cycle_t* cycle);
static char * ngx_http_amqp(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void* ngx_http_amqp_create_conf(ngx_conf_t *cf);
static char* ngx_http_amqp_merge_conf(ngx_conf_t *cf, void* parent, void* child);
static ngx_int_t ngx_http_amqp_handler(ngx_http_request_t* r);
static ngx_int_t ngx_http_amqp_message_eval(ngx_http_request_t*, ngx_http_amqp_conf_t*, ngx_str_t*);

static ngx_command_t ngx_http_amqp_commands[] = {
    {
        ngx_string("amqp_publish"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_amqp,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },
    {
        ngx_string("amqp_ip"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, connection_config.amqp_ip),
        NULL
    },
    {
        ngx_string("amqp_port"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, connection_config.amqp_port),
        NULL
    },
    {
        ngx_string("amqp_exchange"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, connection_config.amqp_exchange),
        NULL
    },
    {
        ngx_string("amqp_queue"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, connection_config.amqp_queue),
        NULL
    },
    {
        ngx_string("amqp_user"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, connection_config.amqp_user),
        NULL
    },
    {
        ngx_string("amqp_password"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, connection_config.amqp_password),
        NULL
    },
    {
        ngx_string("amqp_debug"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_amqp_conf_t, amqp_debug),
        NULL
    },
    ngx_null_command
};

static ngx_http_module_t ngx_http_amqp_module_ctx = {
    NULL,                          /* preconfiguration */
    NULL,                         /* postconfiguration */

    NULL,                          /* create main configuration */
    NULL,                          /* init main configuration */

    NULL,                          /* create server configuration */
    NULL,                          /* merge server configuration */

    ngx_http_amqp_create_conf,   /* create location configuration */
    ngx_http_amqp_merge_conf     /* merge location configuration */
};

ngx_module_t ngx_http_amqp_module = {
    NGX_MODULE_V1,
    &ngx_http_amqp_module_ctx,    /* module context */
    ngx_http_amqp_commands,       /* module directives */
    NGX_HTTP_MODULE,               /* module type */
    NULL,                          /* init master */
    NULL,                          /* init module */
    NULL,                          /* init process */
    NULL,                          /* init thread */
    NULL,                          /* exit thread */
    ngx_http_amqp_exit,            /* exit process */
    NULL,                          /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
amqp_is_connection_error(amqp_rpc_reply_t amqp_reply, ngx_http_request_t* r)
{
    switch (amqp_reply.reply_type) {
      case AMQP_RESPONSE_NORMAL:
        return NGX_OK;
      break;
      case AMQP_RESPONSE_NONE:
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                        "Missing RPC reply type!");
      break;
      case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                        "%s", amqp_error_string2(amqp_reply.library_error));
      break;
      case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (amqp_reply.reply.id) {
          case AMQP_CONNECTION_CLOSE_METHOD: {
            amqp_connection_close_t *m = (amqp_connection_close_t*) amqp_reply.reply.decoded;
              ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                              "Server connection error %d, message: %.*s",
            m->reply_code,
            (int) m->reply_text.len,
            (char*) m->reply_text.bytes);
          break;
          }
      }
      break;
      default:
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                        "Unknown server error, method id 0x%08X\n",
        amqp_reply.reply.id);
      break;
    }

    return NGX_ERROR;
}

static ngx_int_t
connect_amqp(amqp_connection_config_t* amqp_connection_config, amqp_connection_t* amqp_connection, ngx_http_request_t* r){
    amqp_rpc_reply_t    reply;
    int                 status;

    //If we're already connected, carry on.
    if (amqp_connection->is_connected) {
      return NGX_OK;
    }

    amqp_connection->conn = amqp_new_connection();
    amqp_connection->socket = amqp_tcp_socket_new(amqp_connection->conn);

    if (!amqp_connection->socket) {
      amqp_destroy_connection(amqp_connection->conn);
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "Could not create TCP socket");
      return NGX_ERROR;
    }

    status = amqp_socket_open(amqp_connection->socket, (char*)amqp_connection_config->amqp_ip.data, (int)amqp_connection_config->amqp_port);

    if (status != AMQP_STATUS_OK) {
      ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "Could not open TCP Socket");
      return NGX_ERROR;
    }

    reply = amqp_login(amqp_connection->conn,
                      "/",
                       0,
                       131072,
                       0,
                       AMQP_SASL_METHOD_PLAIN,
                       (char*)amqp_connection_config->amqp_user.data,
                       (char*)amqp_connection_config->amqp_password.data);

    if (amqp_is_connection_error(reply, r)) {
      return NGX_ERROR;
    }

    //Does the channel need to be destroyed?
    amqp_channel_open(amqp_connection->conn, 1);

    if (amqp_is_connection_error(amqp_get_rpc_reply(amqp_connection->conn), r)) {
      return NGX_ERROR;
    }

    amqp_connection->is_connected = 1;

    return NGX_OK;
}

static ngx_int_t
ngx_http_amqp_message_eval(ngx_http_request_t *r, ngx_http_amqp_conf_t* amcf, ngx_str_t* message_body) {
    //Either this is a string with script variables or it's not, in which case
    //just return the conf string.
    if (amcf->message_lengths == NULL) {
      message_body = &(amcf->message_to_publish); //message_to_publish will persist, no need to allocate
      return NGX_OK;
    } else {
      if (ngx_http_script_run(r, message_body, amcf->message_lengths->elts, 0, amcf->message_values->elts) != NULL) {
        return NGX_OK;
      } else {
        return NGX_ERROR;
      }
    }
}

static ngx_int_t
ngx_amqp_publish(amqp_connection_config_t* amqp_connection_config, amqp_connection_t* amqp_connection, ngx_str_t message) {
    amqp_bytes_t              exchange, queue, message_as_bytes; //This should be converted at configuration time
    amqp_basic_properties_t   props;
    ngx_int_t                 count;
    int                       error;

    count = 0;
    exchange = amqp_cstring_bytes((char*)amqp_connection_config->amqp_exchange.data);
    queue = amqp_cstring_bytes((char*)amqp_connection_config->amqp_queue.data);
    message_as_bytes = amqp_cstring_bytes((char*)message.data);
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2;

    while (count < MAX_PUBLISH_RETRIES) {
      error = amqp_basic_publish(amqp_connection->conn, 1, exchange, queue, 0, 0, &props, message_as_bytes);

      if (!error) {
        break;
      }

      count++;
    }

    //Somethings up, reconnect next time around.
    if (count >= MAX_PUBLISH_RETRIES) {
      amqp_connection->is_connected = 0;
      return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_http_amqp_handler(ngx_http_request_t* r) {
    //ngx_chain_t             out;
    //ngx_buf_t               *b;
    //u_char                  *response;
    //ngx_int_t               response_len;
    ngx_str_t                 message_body;
    //ngx_int_t               rc;
    //u_char                  *empty_response;
    ngx_http_amqp_conf_t      *amcf;
    ngx_http_complex_value_t  cv;

    amcf = ngx_http_get_module_loc_conf(r, ngx_http_amqp_module);

    if (ngx_http_amqp_message_eval(r, amcf, &message_body) == NGX_ERROR) {
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    if (connect_amqp(&(amcf->connection_config), &(amcf->connection), r) == NGX_ERROR) {
      goto error;
    }

    if (ngx_amqp_publish(&(amcf->connection_config), &(amcf->connection), message_body) == NGX_ERROR) {
      goto error;
    }

    ngx_memzero(&cv, sizeof(ngx_http_complex_value_t));
    cv.value.data = (u_char*)"{\"ok\": true}";
    cv.value.len = ngx_strlen(cv.value.data);
    return ngx_http_send_response(r, NGX_HTTP_OK, &(application_types[0]), &cv);

////////////////////////////////
    error:
      amcf->connection.is_connected = 0;
      amqp_destroy_connection(amcf->connection.conn);
      return NGX_HTTP_INTERNAL_SERVER_ERROR;
}

static char* ngx_http_amqp(ngx_conf_t *cf, ngx_command_t *cmd, void *conf){
    ngx_http_core_loc_conf_t      *clcf;
    ngx_http_script_compile_t     sc;
    ngx_uint_t                    num_script_variables;
    ngx_str_t*                    val;
    ngx_http_amqp_conf_t*         amcf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_amqp_handler;

    val = cf->args->elts;
    amcf = conf;

    if (cf->args->nelts != 2) {
      return NGX_CONF_ERROR;
    }

    num_script_variables = ngx_http_script_variables_count(&val[1]);
    amcf->message_to_publish.data = val[1].data;
    amcf->message_to_publish.len = val[1].len;

    if (num_script_variables > 0) {
        ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

        sc.cf = cf;
        sc.source = &val[1];
        sc.lengths = &amcf->message_lengths;
        sc.values = &amcf->message_values;
        sc.variables = num_script_variables;
        sc.complete_lengths = 1;
        sc.complete_values = 1;

        if (ngx_http_script_compile(&sc) != NGX_OK) {
          return NGX_CONF_ERROR;
        }
    }

    return NGX_CONF_OK;
}

static void*
ngx_http_amqp_create_conf(ngx_conf_t *cf){
    ngx_http_amqp_conf_t* conf;

    conf = (ngx_http_amqp_conf_t*)ngx_pcalloc(cf->pool, sizeof(ngx_http_amqp_conf_t));
    conf->connection_config.amqp_port = NGX_CONF_UNSET_UINT;

    if(conf == NULL){
        return NULL;
    }

    return conf;
}

static char* ngx_http_amqp_merge_conf(ngx_conf_t *cf, void* parent, void* child){

    ngx_http_amqp_conf_t* prev=parent;
    ngx_http_amqp_conf_t* conf=child;

    ngx_conf_merge_str_value(conf->connection_config.amqp_ip, prev->connection_config.amqp_ip, "127.0.0.1");
    ngx_conf_merge_uint_value(conf->connection_config.amqp_port, prev->connection_config.amqp_port, 5672);
    ngx_conf_merge_str_value(conf->connection_config.amqp_exchange, prev->connection_config.amqp_exchange, "rumExchange");
    ngx_conf_merge_str_value(conf->connection_config.amqp_queue, prev->connection_config.amqp_queue, "rumQueue");
    ngx_conf_merge_str_value(conf->connection_config.amqp_user, prev->connection_config.amqp_user, "guest");
    ngx_conf_merge_str_value(conf->connection_config.amqp_password, prev->connection_config.amqp_password, "guest");
    ngx_conf_merge_uint_value(conf->connection.is_connected, conf->connection.is_connected, 0);
    ngx_conf_merge_uint_value(conf->amqp_debug, prev->amqp_debug, 0);

    return NGX_CONF_OK;
}

static void ngx_http_amqp_exit(ngx_cycle_t* cycle){
    ngx_http_amqp_conf_t* amcf = (ngx_http_amqp_conf_t*)ngx_get_conf(cycle->conf_ctx, ngx_http_amqp_module);
    amqp_channel_close(amcf->connection.conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(amcf->connection.conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(amcf->connection.conn);
}
