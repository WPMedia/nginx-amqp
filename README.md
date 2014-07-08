nginx-amqp
==========

Require librabbitmq from rabbit-c: <br />
https://github.com/alanxz/rabbitmq-c 

1. Install rabbit-c driver from above link.
2. Download nginx from http://nginx.org/en/download.html
3. Unzip file and use following command <br />
   ./configure --add-module=/path/to/nginx/amqp <br />
   make <br />
   sudo make <br />
4. Using above nginx.conf (which should be put in /usr/local/nginx/conf/)

==========
You may need to declare exchange and queue first.
