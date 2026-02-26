#!/bin/bash
set -e

host="$1"
shift
cmd="$@"

until curl -s "http://$host" > /dev/null 2>&1 || nc -z $(echo $host | cut -d: -f1) $(echo $host | cut -d: -f2); do
  >&2 echo "Kafka n'est pas encore prêt"
  sleep 2
done

>&2 echo "Kafka est prêt - exécution"
exec $cmd 