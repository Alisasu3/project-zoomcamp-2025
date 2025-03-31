Use docker to run kestra image:

docker run --pull=always --rm -it -p 8090:8080 --user=root \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp kestra/kestra:latest server local

reference: https://kestra.io/docs/installation/docker

Download and upzip file using [download_sydney_data.sh](https://github.com/Alisasu3/project-zoomcamp-2025/blob/main/Kaggledata/download_sydney_data.sh)