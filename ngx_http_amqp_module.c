#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_md5.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <syslog.h>

typedef struct{
	ngx_str_t amqp_ip;
	ngx_uint_t amqp_port;
	ngx_str_t amqp_exchange;
	ngx_str_t amqp_queue;
	ngx_str_t amqp_user;
	ngx_str_t amqp_password;
	amqp_socket_t* socket;
	amqp_connection_state_t conn;
	ngx_uint_t init;
	ngx_uint_t amqp_debug;
}ngx_http_amqp_conf_t;


static char * ngx_http_amqp(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void* ngx_http_amqp_create_conf(ngx_conf_t *cf);
static char* ngx_http_amqp_merge_conf(ngx_conf_t *cf, void* parent, void* child);
ngx_int_t ngx_http_amqp_handler(ngx_http_request_t* r);


static ngx_command_t ngx_http_amqp_commands[] = {
	{
		ngx_string("amqp_publish"),
		NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
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
		offsetof(ngx_http_amqp_conf_t, amqp_ip),
		NULL
	},
	{
		ngx_string("amqp_port"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_num_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_amqp_conf_t, amqp_port),
		NULL
	},
	{
		ngx_string("amqp_exchange"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_amqp_conf_t, amqp_exchange),
		NULL
	},
	{
		ngx_string("amqp_queue"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_amqp_conf_t, amqp_queue),
		NULL
	},
	{
		ngx_string("amqp_user"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_amqp_conf_t, amqp_user),
		NULL
	},
	{
		ngx_string("amqp_password"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_amqp_conf_t, amqp_password),
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
    NULL,                          /* exit process */
    NULL,                          /* exit master */
	NGX_MODULE_V1_PADDING
};


int get_error(int x, char const *context, char* error)
{
	if (x < 0) {
		syslog(LOG_ERR, "%s: %s\n", context, amqp_error_string2(x));
		sprintf(error, "%s: %s\n", context, amqp_error_string2(x));
		return 1;
	}
	return 0;
}


int get_amqp_error(amqp_rpc_reply_t x, char const *context, char* error)
{
	error=(char*)malloc(1024);
	switch (x.reply_type) {
		case AMQP_RESPONSE_NORMAL:
		return 0;

		case AMQP_RESPONSE_NONE:
		syslog(LOG_ERR, "%s: missing RPC reply type!", context);
		sprintf(error, "%s: missing RPC reply type!\n", context);
		break;

		case AMQP_RESPONSE_LIBRARY_EXCEPTION:
		syslog(LOG_ERR, "%s: %s", context, amqp_error_string2(x.library_error));
		sprintf(error, "%s: %s\n", context, amqp_error_string2(x.library_error));
		break;

		case AMQP_RESPONSE_SERVER_EXCEPTION:
		switch (x.reply.id) {
			case AMQP_CONNECTION_CLOSE_METHOD: {
				amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
				syslog(LOG_ERR, "%s: server connection error %d, message: %.*s",
					context,
					m->reply_code,
					(int) m->reply_text.len, (char *) m->reply_text.bytes);
				sprintf(error, "%s: server connection error %d, message: %.*s\n",
					context,
					m->reply_code,
					(int) m->reply_text.len, (char *) m->reply_text.bytes);
				break;
			}
			case AMQP_CHANNEL_CLOSE_METHOD: {
				amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
				syslog(LOG_ERR,  "%s: server channel error %d, message: %.*s",
					context,
					m->reply_code,
					(int) m->reply_text.len, (char *) m->reply_text.bytes);
				sprintf(error, "%s: server channel error %d, message: %.*s\n",
					context,
					m->reply_code,
					(int) m->reply_text.len, (char *) m->reply_text.bytes);
				break;
			}
			default:
			syslog(LOG_ERR, "%s: unknown server error, method id 0x%08X", context, x.reply.id);
			sprintf(error, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
			break;
		}
		break;
	}

	return 1;
}

int connect_amqp(ngx_http_amqp_conf_t* amcf, char* error){
	int status;
	amqp_rpc_reply_t reply;
	//initialize connection and channel
	amcf->conn=amqp_new_connection();
	amcf->socket=amqp_tcp_socket_new(amcf->conn);
	if(!amcf->socket){
		error="Creating TCP socket";
		return -1;
	}
	status=amqp_socket_open(amcf->socket, (char*)amcf->amqp_ip.data, (int)amcf->amqp_port);
	if(status){
		error="Opening TCP socket";
		return -1;
	}
	reply=amqp_login(amcf->conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,(char*)amcf->amqp_user.data, (char*)amcf->amqp_password.data);
	if(get_amqp_error(reply, "Logging in", error)){
		return -1;
	}
	amqp_channel_open(amcf->conn, 1);
	if(get_amqp_error(amqp_get_rpc_reply(amcf->conn), "Opening channel", error)){
		return -1;
	}
	return 0;
}





ngx_int_t ngx_http_amqp_handler(ngx_http_request_t* r){

	ngx_http_amqp_conf_t* amcf=ngx_http_get_module_loc_conf(r, ngx_http_amqp_module);
	ngx_chain_t out;
	ngx_buf_t* b;
	ngx_str_t response;


	char* exchange;
	char* routingkey;
	char* messagebody;

	ngx_int_t rc;
	ngx_str_t rum, referer, user_agent;
	ngx_str_t create_time;

	u_char* ref;
	size_t len;
	time_t date;
	ngx_str_t client_ip;

	//time created
	date=time(0);

	char* msg;
	int init=(int)amcf->init;

	create_time.data=ngx_pcalloc(r->pool, 40);
	ngx_memzero(create_time.data, sizeof(create_time.data)+1);
	ngx_http_time(create_time.data, date);
	create_time.len=ngx_strlen(create_time.data);

	//user-agent
	if(r->headers_in.user_agent!=NULL){
		user_agent.data=r->headers_in.user_agent->value.data;
		user_agent.len=r->headers_in.user_agent->value.len;
	}
	else{
		user_agent.data=(u_char*)"null";
		user_agent.len=sizeof(user_agent)-1;
	}

	//referer
	if(r->headers_in.referer!=NULL){
		ref=r->headers_in.referer->value.data;
		len=r->headers_in.referer->value.len;
		if(ngx_strncasecmp(ref, (u_char*)"http://", 7)==0){
			ref+=7;
			len-=7;
		}
		else if(ngx_strncasecmp(ref, (u_char*)"https://", 8)==0){
			ref+=8;
			len-=8;
		}
		referer.data=ref;
		referer.len=len;
	}
	else{
		referer.data=(u_char*)"null";
		referer.len=sizeof(referer)-1;
	}


	//client ip
	client_ip.data=ngx_pcalloc(r->pool, r->connection->addr_text.len);
	client_ip.len=r->connection->addr_text.len;
	ngx_memzero(client_ip.data, sizeof(client_ip)+1);
	ngx_memcpy(client_ip.data, r->connection->addr_text.data, r->connection->addr_text.len);


	//parse for messagebody
	rc=ngx_http_arg(r, (u_char*) "rum", 3, &rum);
	if(rc!=NGX_OK){
		syslog(LOG_ERR, "Cannot parsing rum arg from %s", client_ip.data);
		return NGX_ERROR;
	}

	messagebody=(char*)malloc(rum.len+referer.len+user_agent.len+create_time.len+client_ip.len+36);

	memset(messagebody, 0, rum.len+referer.len+user_agent.len+create_time.len+client_ip.len+36+1);
	memcpy(messagebody, rum.data, rum.len);
	strcat(messagebody, "DELIMITER");
	strcat(messagebody, (char*)referer.data);
	strcat(messagebody, "DELIMITER");
	strcat(messagebody, (char*)user_agent.data);
	strcat(messagebody, "DELIMITER");
	strcat(messagebody, (char*)create_time.data);
	strcat(messagebody, "DELIMITER");
	strcat(messagebody, (char*)client_ip.data);

	//amqp variables

	exchange=(char*)malloc(amcf->amqp_exchange.len);
	memset(exchange, 0, sizeof(exchange)+1);
	memcpy(exchange, amcf->amqp_exchange.data, amcf->amqp_exchange.len+1);

	routingkey=(char*)malloc(amcf->amqp_queue.len);
	memset(routingkey, 0, sizeof(routingkey)+1);
	memcpy(routingkey, amcf->amqp_queue.data, amcf->amqp_queue.len+1);


	msg=(char*)malloc(1024);

	if(!amcf->init){
		amcf->init=1;

		if(connect_amqp(amcf, msg)<0) goto error;
	}

	amqp_basic_properties_t props;

	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = 2;
	if(get_error(amqp_basic_publish(amcf->conn, 1, amqp_cstring_bytes(exchange), amqp_cstring_bytes(routingkey), 0, 0, &props, amqp_cstring_bytes(messagebody)), "Publishing", msg)){
		syslog(LOG_WARNING, "Cannot publish. Try to republish.");
		memset(msg, 0, sizeof(msg)+1);
		if(connect_amqp(amcf, msg)<0) goto error;
		if(get_error(amqp_basic_publish(amcf->conn, 1, amqp_cstring_bytes(exchange), amqp_cstring_bytes(routingkey), 0, 0, &props, amqp_cstring_bytes(messagebody)), "Publishing", msg)){
			goto error;
		}

	}
	sprintf(msg, "NO ERROR init=%d", init);


  	r->headers_out.content_type_len = sizeof("text/html") - 1;
  	r->headers_out.content_type.data = (u_char *) "text/html";


  	response.data=ngx_pcalloc(r->pool, 1024);
  	ngx_sprintf(response.data, "%s::%s\nmsg: %s\n%s\n", exchange, routingkey, messagebody, msg);
  	response.len=ngx_strlen(response.data);

  	b=ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
  	if(b==NULL) return NGX_HTTP_INTERNAL_SERVER_ERROR;
  	out.buf=b;
  	out.next=NULL;

  	b->pos=response.data;
  	b->last=response.data+response.len;
  	b->memory=1;
  	b->last_buf=1;

  	r->headers_out.status=NGX_HTTP_OK;
  	r->headers_out.content_length_n=response.len;

  	rc=ngx_http_send_header(r);
  	if(rc==NGX_ERROR||rc>NGX_OK||r->header_only){
  		return rc;
  	}
  	return amcf->amqp_debug? ngx_http_output_filter(r, &out): NGX_OK;
////////////////////////////////
error:
  	amcf->init=0;
  	response.data=ngx_pcalloc(r->pool, 1024);
  	ngx_sprintf(response.data, "Error: %s\n", msg);
  	response.len=ngx_strlen(response.data);
  	b=ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
  	if(b==NULL) return NGX_HTTP_INTERNAL_SERVER_ERROR;
  	out.buf=b;
  	out.next=NULL;

  	b->pos=response.data;
  	b->last=response.data+response.len;
  	b->memory=1;
  	b->last_buf=1;

  	r->headers_out.status=NGX_HTTP_OK;
  	r->headers_out.content_length_n=response.len;

  	rc=ngx_http_send_header(r);
  	if(rc==NGX_ERROR||rc>NGX_OK||r->header_only){
  		return rc;
  	}
  	return amcf->amqp_debug? ngx_http_output_filter(r, &out): NGX_OK;



  }

  static char * ngx_http_amqp(ngx_conf_t *cf, ngx_command_t *cmd, void *conf){
  	ngx_http_amqp_conf_t* amcf=conf;
  	amcf->init=0;
  	openlog("nginx-amqp", LOG_CONS|LOG_PID, LOG_LOCAL0);
  	ngx_http_core_loc_conf_t *clcf;


	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
	clcf->handler = ngx_http_amqp_handler;

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
