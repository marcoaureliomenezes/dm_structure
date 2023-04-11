FROM apache/nifi:1.11.4

RUN mkdir -p hadoop drivers/mysql

RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.29.tar.gz && \
    tar -zxvf mysql-connector-java-8.0.29.tar.gz && \
    rm -r mysql-connector-java-8.0.29.tar.gz && \
    mv mysql-connector-java-8.0.29 drivers/mysql
    
COPY ./conf hadoop/
