FROM python:3.6
WORKDIR /
RUN curl https://www.python.org/ftp/python/3.6.1/Python-3.6.1.tar.xz | tar -xJ
WORKDIR Python-3.6.1
RUN ./configure --prefix=/var/lang
RUN make -j$(getconf _NPROCESSORS_ONLN) libinstall inclinstall
WORKDIR /src
RUN rm -rf /Python-3.6.1
RUN pip install patchelf-wrapper
RUN pip3 install staticx
ADD . /src
RUN pip install -r requirements.txt
