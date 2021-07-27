!/bin/sh


##参数
main_class=""
jar_name=""
db_name=""

rm -rf ${jar_name}.jar
rm -rf ${jar_name}
mkdir ${jar_name}
hadoop -get /apps/hduser1594/sx_lidrp_safe/

cirCount=1
while [[ true ]];do
ps ef | grep &{jar_name}.jar |grep ${main_class} | grep -v grep
  if [ $? -ne 0 ]; then
      echo '第&{cirCount}次启动程序'
      export SPARK_CONF_DIR=
      export SPARK_CONF_DIR=
      if [ $? -ne 0 ]; then
          if ((cirCount < 1)); then
              cirCount=&((cirCount+1))
          elif
              break
          fi
          else
            cirCount=1
            break

      fi
      else
        echo "程序运行中-----汇总任务----------《"
        sleep 60
  fi
done



