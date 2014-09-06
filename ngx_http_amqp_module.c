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
  ngx_uint_t        amqp_exchange;
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
    amqp_connection_config_t	connection_config;
    amqp_connection_t           connection
    ngx_uint_t                  amqp_debug;
    ngx_str_t 			message_to_publish;
    ngx_array_t*                message_lengths;
    ngx_array_t*                message_values;
} ngx_http_amqp_conf_t;


static void ngx_http_amqp_exit(ngx_cycle_t* cycle);
static char * ngx_http_amqp(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void* ngx_http_amqp_create_conf(ngx_conf_t *cf);
static char* ngx_http_amqp_merge_conf(ngx_conf_t *cf, void* parent, void* child);
ngx_int_t ngx_http_amqp_handler(ngx_http_request_t* r);


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
amqp_is_connection_error(amqp_rpc_reply_t amqp_reply)
{
    if (AMQP_RESPONSE_NORMAL) {
	return 1;
    } else {
	return 0;
    }
}

static ngx_uint_t
initialize_connection(amqp_connection_config_t* amqp_connection_config, amqp_connection_t* amqp_connection) {
    amqp_connection->conn = amqp_new_connection();
    amqp_connection->socket = amqp_tcp_socket_new(amqp_connection_config->conn);

    //If we failed to connect to the socket, destroy the connection
    //and *shoud* destroy socket too
    if (!amqp_connection->socket) {
	amqp_destroy_connection(amqp_connection->conn);
	return 0;
    }

    return 1;
}

static ngx_int_t 
connect_amqp(amqp_connection_config_t* amqp_connection_config, amqp_connection_t* amqp_connection, ngx_http_request_t* r){
    amqp_rpc_reply_t reply;

    //If we're already connected, carry on.
    if (amqp_connection->is_connected) {
	return 1;
    }

    if (intialize_connection(amqp_connection_config, amqp_connection) {
	return 0;
    }

    status = amqp_socket_open(amqp_connection->socket, (char*)amqp_connection_config->amqp_ip.data, (int)amqp_connection_config->amqp_port);

    if (!status) {
	return 0;
    }

    reply = amqp_login(amqp_connection->conn,
                      "/",
                       0,
                       131072,
                       0,
                       AMQP_SASL_METHOD_PLAIN,
                       (char*)amqp_connection_config->amqp_user.data,
                       (char*)amqp_connection_config->amqp_password.data);

    if (amqp_is_connection_error(reply)) {
	return 0;
    }

    //Does the channel need to be destroyed?
    amqp_channel_open(amcf->conn, 1);

    if (amqp_is_connection_error(amqp_get_rpc_reply(amqp_connection->conn)) {
	return 0;
    }

    amqp_connection->is_connected = 1;

    return 1;
}

static ngx_str_t
ngx_http_amqp_message_eval(ngx_http_request_t *r, ngx_http_amqp_conf_t* amcf) {
    ngx_str_t message_body;

    //Either this is a string with script variables or it's not, in which case
    //just return the conf string.
    if (amcf->message_lengths == NULL) {
	return amcf->message_values;
    } else {
	if (ngx_http_script_run(r, &message_body, amcf->message_lengths, 0, amcf->message_values->elts) != NULL) {
	    return message_body;
	} else {
	    return NULL;
	}
    }
}

static ngx_int_t
ngx_amqp_publish(amqp_connection_config_t* amqp_connection_config, amqp_connection_t* amqp_connection, ngx_str_t message) {
    amqp_bytes_t 		exchange, queue, message_as_bytes; //This should be converted at configuration time
    amqp_basic_properties_t 	props;
    ngx_int_t 			count;
    int 			error;
    
    count = 0;
    exchange = amqp_cstring_bytes((char*)amqp_connection_config->amqp_exchange.data);
    queue = amqp_cstring_bytes((char*)amqp_connection_config->amqp_queue.data);
    message_as_bytes = amqp_cstring_bytes((char*)message);
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2;

    while (i < MAX_PUBLISH_RETRIES) {
	error = amqp_basic_publish(amqp_connection->conn, 1, exchange, queue, 0, 0, &props, message_as_bytes);
	if (!error) {
	    break;
	}
	i++;
    }

    //Somethings up, reconnect next time around.
    if (i == MAX_PUBLISH_RETRIES) {
	amqp_connection->is_connected = 0;
    }

    return 1;
}

static ngx_int_t 
ngx_http_amqp_handler(ngx_http_request_t* r) {
    ngx_chain_t 		out;
    ngx_buf_t 			*b;
    ngx_str_t 			response;
    ngx_str_t 			message_body;
    ngx_int_t 			rc;
    u_char 			*empty_response;
    u_char 			*msg;
    u_char 			*msgbody;
    ngx_http_amqp_conf_t 	*amcf;

    amcf = ngx_http_get_module_loc_conf(r, ngx_http_amqp_module);

    if (!(message_body = ngx_http_amqp_message_eval(r, amcf)) {
	return NGX_HTTP_INTERNAL_SERVER_ERROR;	    
    }

    if (!connect_amqp(&amcf->connection_config, &amcf->connection, r)) {
	goto error;
    }

    if (!ngx_amqp_publish(amcf->connection_config, amcf->connection, message_body)) {
	goto error;
    }

    r->headers_out.content_type_len = sizeof("text/html") - 1;
    r->headers_out.content_type.data = (u_char *) "text/html";

    response.data = ngx_palloc(r->pool, message_body.len + amcf->connection_config->amqp_exchange.len + amcf->connection_config->amqp_queue.len + 19);
    ngx_sprintf(response.data, "%s::%s\nmessagebody: %s\n%s\n", amcf->amqp_exchange.data, amcf->amqp_queue.data, msgbody, msg);
    response.len = ngx_strlen(response.data);

    b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
    
    if ( b == NULL) {
	return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    out.buf = b;
    out.next = NULL;

    if (amcf->amqp_debug){
	response.data = ngx_palloc(r->pool, message_body.len + amcf->connection_config->amqp_exchange.len + amcf->connection_config->amqp_queue.len + 19);
	ngx_sprintf(response.data, "%s::%s\nmessagebody: %s\n%s\n", amcf->amqp_exchange.data, amcf->amqp_queue.data, msgbody, msg);
	response.len = ngx_strlen(response.data);

        b->pos = response.data;
        b->last = response.data + response.len;
        r->headers_out.content_length_n = response.len;
    } else {
        empty_response = (u_char*)"\n";
        b->pos = empty_response;
        b->last = empty_response+sizeof(empty_response);
        r->headers_out.content_length_n = sizeof(empty_response);
    }

    b->memory = 1;
    b->last_buf = 1;

    r->headers_out.status = NGX_HTTP_OK;

    rc = ngx_http_send_header(r);
    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only){
        return rc;
    }

    return ngx_http_output_filter(r, &out);
////////////////////////////////
    error:
	amcf->connection->is_connected = 0;
	return NGX_HTTP_INTERNAL_SERVER_ERROR;
}

static char* ngx_http_amqp(ngx_conf_t *cf, ngx_command_t *cmd, void *conf){
    ngx_http_core_loc_conf_t 	*clcf;
    ngx_http_script_compile_t 	sc;
    ngx_uint_t 			num_script_variables;
    ngx_str_t* 			val;
    ngx_http_amqp_conf_t* 	amcf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_amqp_handler;

    val = cf->args->elts;
    amcf = conf;

    if (cf->args->nelts != 2) {
      return NGX_CONF_ERROR;
    }

    amcf->init = 0; //What does init do?
    num_script_variables = ngx_http_script_variables_count(&val[1]);
    amcf->message_to_publish.data = val[1].data;
    amcf->message_to_publish.len = val[1].len;

    if (num_script_variables > 0) {
        ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

        sc.cf = cf;
        sc.source = &val[1];
        sc.lengths = &amcf->message_lengths;
        sc.values = &amcf->message_values;
        sc.variables = n;
        sc.complete_lengths = 1;
        sc.complete_values = 1;

        if (ngx_http_script_compile(&sc) != NGX_OK) {
          return NGX_CONF_ERROR;
        }
    }

    return NGX_CONF_OK;
}


static void* ngx_http_amqp_create_conf(ngx_conf_t *cf){
    ngx_http_amqp_conf_t* conf;

    conf=(ngx_http_amqp_conf_t*)ngx_pcalloc(cf->pool,
        sizeof(ngx_http_amqp_conf_t));
    if(conf==NULL){
        return NULL;
    }

    conf->amqp_debug=NGX_CONF_UNSET_UINT;
    conf->init=NGX_CONF_UNSET_UINT;
    conf->socket=NULL;
    conf->amqp_port=NGX_CONF_UNSET_UINT;
    conf->lengths=NULL;
    conf->values=NULL;

    return conf;
}

static char* ngx_http_amqp_merge_conf(ngx_conf_t *cf, void* parent, void* child){

    ngx_http_amqp_conf_t* prev=parent;
    ngx_http_amqp_conf_t* conf=child;



    ngx_conf_merge_str_value(conf->amqp_ip, prev->amqp_ip, "127.0.0.1");
    ngx_conf_merge_uint_value(conf->amqp_port, prev->amqp_port, 5672);
    ngx_conf_merge_str_value(conf->amqp_exchange, prev->amqp_exchange, "rumExchange");
    ngx_conf_merge_str_value(conf->amqp_queue, prev->amqp_queue, "rumQueue");
    ngx_conf_merge_str_value(conf->amqp_user, prev->amqp_user, "guest");
    ngx_conf_merge_str_value(conf->amqp_password, prev->amqp_password, "guest");
    ngx_conf_merge_uint_value(conf->init, prev->init, 0);
    ngx_conf_merge_uint_value(conf->amqp_debug, prev->amqp_debug, 0);



    return NGX_CONF_OK;
}
static void ngx_http_amqp_exit(ngx_cycle_t* cycle){
    ngx_http_amqp_conf_t* amcf=(ngx_http_amqp_conf_t*)ngx_get_conf(cycle->conf_ctx, ngx_http_amqp_module);
    amqp_channel_close(amcf->conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(amcf->conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(amcf->conn);
}
