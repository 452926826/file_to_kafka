**打包方式**
mvn clean package
启动命令：nohup java -jar spring-boot2-file-parse-2.0.0.RELEASE.jar --dirctory-path=./data --spring-redis-db=0 --spring-redis-host=redis.hdsp.hand.com > ./file.log 2>&1 &