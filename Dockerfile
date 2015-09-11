FROM ruby

WORKDIR /
RUN wget https://raw.githubusercontent.com/antirez/redis/3.0/src/redis-trib.rb
RUN chmod o+x redis-trib.rb

RUN apt-get update
RUN apt-get install -y openssh-server
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

# install supervisor
RUN apt-get install -y supervisor
RUN touch /var/run/supervisor.sock
RUN chmod 777 /var/run/supervisor.sock
ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ADD . /code

CMD ["/usr/bin/supervisord"]
