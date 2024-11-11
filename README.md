# Tugas Pengganti Quiz 2 - Big Data Kelas A

## Kelompok 7

| Nama                    | NRP        |
| ----------------------- | ---------- |
| Dwiyasa Nakula   | 5027221001 |
| Wilson Matthew Thendry | 5027221024 |
| Muhammad Afif | 5027221032 |

## Up Kafka

```bash
docker compose -f zk-kafka-docker-compose.yml up -d
```

## Create Topic

```bash
docker-compose exec kafka1 kafka-topics.sh --create --topic quiz-2 --partitions 1 --replication-factor 1 --bootstrap-server kafka1:19092
```

## 