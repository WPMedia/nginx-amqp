nginx-amqp
==========

Require librabbitmq from rabbit-c: <br />
https://github.com/alanxz/rabbitmq-c 

1. Download nginx from http://nginx.org/en/download.html
2. Unzip file and use following command <br />
   ./configure --add-module=/path/to/nginx/amqp <br />
   make <br />
   sudo make <br />
3. Using above nginx.conf (which should be put in /usr/local/nginx/conf/)

==========
You may need to declare exchange and queue first.
