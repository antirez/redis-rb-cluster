FROM redis:latest

RUN apt-get update
RUN apt-get install -y ruby
RUN apt-get install -y supervisor

WORKDIR /

RUN echo 'daemonize no\npidfile 7000.pid\ncluster-config-file /7000/node.conf\nport 7000\ncluster-enabled yes\ncluster-node-timeout 5000\nappendonly yes\nappendonly no\ndbfilename dump.rdb\ndir /7000' > conf.tmpl

RUN mkdir 7000 7001 7002 7003 7004 7005

RUN cp conf.tmpl 7000/redis.conf

RUN cp conf.tmpl 7001/redis.conf
RUN sed -i 's/7000/7001/' 7001/redis.conf

RUN cp conf.tmpl 7002/redis.conf
RUN sed -i 's/7000/7002/' 7002/redis.conf

RUN cp conf.tmpl 7003/redis.conf
RUN sed -i 's/7000/7003/' 7003/redis.conf

RUN cp conf.tmpl 7004/redis.conf
RUN sed -i 's/7000/7004/' 7004/redis.conf

RUN cp conf.tmpl 7005/redis.conf
RUN sed -i 's/7000/7005/' 7005/redis.conf

RUN gem install redis

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY redis-trib.rb /redis-trib.rb
COPY create_cluster.sh /create_cluster.sh

EXPOSE 7002
EXPOSE 7003
EXPOSE 7004
EXPOSE 7005
EXPOSE 7006
EXPOSE 7007

CMD ["/usr/bin/supervisord"]
