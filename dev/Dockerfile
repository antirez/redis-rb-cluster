FROM debian:wheezy

RUN apt-get update
RUN apt-get install -y openssh-server wget gcc make vim ruby

ENV LOCAL_SRC /usr/local/src
WORKDIR ${LOCAL_SRC}
RUN wget http://download.redis.io/releases/redis-3.0.0.tar.gz
RUN tar -zxvf redis-3.0.0.tar.gz

ENV REDIS_DIR ${LOCAL_SRC}/redis-3.0.0
WORKDIR ${REDIS_DIR}
RUN make

WORKDIR /

RUN echo 'daemonize no\npidfile 7000.pid\ncluster-config-file /7000/node.conf\nport 7000\ncluster-enabled yes\ncluster-node-timeout 5000\nappendonly yes\nappendonly no\ndbfilename dump.rdb\ndir /7000' > conf.tmpl

RUN mkdir 7002 7003 7004 7005 7006 7007 7008 7009 7010 7011 7012 7013

RUN cp conf.tmpl 7002/redis.conf
RUN sed -i 's/7000/7002/' 7002/redis.conf

RUN cp conf.tmpl 7003/redis.conf
RUN sed -i 's/7000/7003/' 7003/redis.conf

RUN cp conf.tmpl 7004/redis.conf
RUN sed -i 's/7000/7004/' 7004/redis.conf

RUN cp conf.tmpl 7005/redis.conf
RUN sed -i 's/7000/7005/' 7005/redis.conf

RUN cp conf.tmpl 7006/redis.conf
RUN sed -i 's/7000/7006/' 7006/redis.conf

RUN cp conf.tmpl 7007/redis.conf
RUN sed -i 's/7000/7007/' 7007/redis.conf

RUN cp conf.tmpl 7008/redis.conf
RUN sed -i 's/7000/7008/' 7008/redis.conf

RUN cp conf.tmpl 7009/redis.conf
RUN sed -i 's/7000/7009/' 7009/redis.conf

RUN cp conf.tmpl 7010/redis.conf
RUN sed -i 's/7000/7010/' 7010/redis.conf

RUN cp conf.tmpl 7011/redis.conf
RUN sed -i 's/7000/7011/' 7011/redis.conf

RUN cp conf.tmpl 7012/redis.conf
RUN sed -i 's/7000/7012/' 7012/redis.conf

RUN cp conf.tmpl 7013/redis.conf
RUN sed -i 's/7000/7013/' 7013/redis.conf


RUN gem install redis

RUN apt-get install -y supervisor

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 7002
EXPOSE 7003
EXPOSE 7004
EXPOSE 7005
EXPOSE 7006
EXPOSE 7007

CMD ["/usr/bin/supervisord"]
