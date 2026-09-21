# ДЗ 6. Сервис телеметрии для мобильных приложений — решение

> Условие: [../Задание.md](../Задание.md)

Проектируем сервис сбора, обработки и хранения телеметрии (логи, крашиз, перформанс-метрики) для мобильных приложений. Аналогичен Crashlytics, Sentry, DataDog Mobile.

---

## 1. Требования

### Функциональные требования

- **Сбор telemetry:** логи, крашиз, ANR, перформанс-метрики (CPU, память, батарея, трафик)
- **Обогащение:** географические данные, модель устройства, версия OS, версия приложения
- **Хранение:** запись в БД, индексация для поиска, лонг-терм архивирование
- **Запросы:** аналитика (тренды, гистограммы ошибок), фильтрация по версии/устройству/стране
- **Алёрты:** срабатывание при всплеске крашей, регрессии перформанса
- **API:** приём события от SDK (мобильные приложения), REST API для дашбордов
- **SDKs:** поддержка iOS, Android с offline-буфером и батарейным режимом

### Нефункциональные требования

| Параметр | Значение | Обоснование |
|---|---|---|
| **DAU** | 50 млн | Типовое глобальное мобильное приложение |
| **Сигналов в день** | 500 млн (10 на DAU) | SDK буферирует, часть совмещается в batch |
| **RPS пик** | 100 000 | 500M / 86400 s = 5.8K base, пик ~17x |
| **Размер события** | 2 КБ avg | Логи 500Б, крашдампы 10КБ, батарея 100Б |
| **Throughput** | 200 ГБ/день | 1 ТБ raw → 200 ГБ compressed |
| **Latency приёма** | < 100 мс p99 | SDK быстро отправить и забыть |
| **Горячие данные** | 30 дней | Месячный цикл разбора инцидентов |
| **Холодные данные** | 1 год | GDPR compliance |
| **Доступность** | 99,5 % | 4.3 часа downtime/месяц |
| **Консистентность** | Eventually consistent | Counts в аналитике актуальны через час |

### Риски и ограничения

- SDK на мобильных с плохой сетью — буферируем локально
- Не каждое событие стоит хранить — sampling по важности и rate
- Крашдампы 1–100 МБ — нужна дедупликация и сжатие
- GDPR: удаление данных за 30 дней

### Backlog

- ML на тренды (предсказание регрессий)
- Auto-closing issues при релизе
- Mobile UI для просмотра крашей

---

## 2. Концептуальная архитектура

### Диаграмма контейнеров

```
┌──────────────────────────────────────────────────────┐
│             Telemetry System                         │
├──────────────────────────────────────────────────────┤
│                                                      │
│  [SDK] ──HTTP/2──> [API Gateway]                   │
│                          │                           │
│                          v                           │
│                    ┌─────────────┐                   │
│                    │ Kafka Queue │ (topics:          │
│                    │ - crashes   │ crashes, logs,    │
│                    │ - logs      │ perf, battery)    │
│                    │ - perf      │                   │
│                    │ - battery   │                   │
│                    └──────┬──────┘                   │
│                           │                           │
│        ┌──────────────────┼──────────────────┐      │
│        │                  │                  │      │
│        v                  v                  v      │
│   ┌────────┐         ┌─────────┐       ┌────────┐ │
│   │Processor│        │Indexer  │       │Archiver│ │
│   │dedup    │        │(search) │       │(cold)  │ │
│   │enrich   │        │         │       │        │ │
│   └────┬───┘         └────┬────┘       └──┬─────┘ │
│        │                  │               │       │
│        v                  v               v       │
│   [Postgres]      [Elasticsearch]    [S3]        │
│   (30 дн)         (30 дн, search)  (1 год)      │
│                                              │
│   ┌──────────┐           ┌──────────────┐   │
│   │Aggregator│─────────> │ClickHouse    │   │
│   │          │           │ (analytics)  │   │
│   └──────────┘           └──────────────┘   │
│                                              │
│   ┌──────────────────────────────────────┐  │
│   │ Dashboards + Alert Manager           │  │
│   │ (REST API + WebSocket)               │  │
│   └──────────────────────────────────────┘  │
│                                              │
└──────────────────────────────────────────────┘
```

### Компоненты

1. **API Gateway** — приём событий от SDKs по HTTP/2, rate limit по app_id, fast-fail 503
2. **Event Buffer (Kafka)** — асинхронная буферизация, at-least-once, ретеншен 7 дней
3. **Processor** — обогащение (GeoIP), дедупликация крашей, sampling
4. **Hot Storage (PostgreSQL)** — 30 дней горячих данных, индексы
5. **Timeseries DB (ClickHouse)** — агрегаты: крашей/день, P95 CPU/память, аналитика
6. **Cold Storage (S3)** — полные краш-дампы за год
7. **Search Index (Elasticsearch)** — текстовый поиск по сообщениям
8. **Alert Manager** — правила на метриках, webhook-уведомления
9. **Dashboards** — web UI для просмотра трендов

### Обоснование архитектуры

**Event Sourcing + CQRS:** Events в Kafka как source of truth, трансформируются в несвязанные модели (хот-хранилище для поиска, холод для архива, аналитические агрегаты). Отказ одного не топит другой.

**Асинхронный приём:** SDK не ждёт записи в БД, обогащение процессируется параллельно.

**Партиционирование по app_id:** обеспечивает упорядоченность событий в рамках приложения.

### ADR: 3 ключевых решения

**ADR-1: Kafka для буферизации вместо синхронной записи в БД**

Контекст: 100K RPS, большой variance, мобильные SDKs должны отправить и забыть.

Решение: Asynchronous ingestion. Gateway отвечает 202 за 10мс (publish в Kafka), Processor обрабатывает позже.

Альтернативы: Синхронная запись в Postgres (не выдержит пики), прямо в S3 (потеряем hot queries).

Последствия: +архитектурная сложность, но resilience и scalability.

---

**ADR-2: PostgreSQL для хот-хранилища + ClickHouse для аналитики**

Контекст: Два типа запросов: (1) point queries по событию (OLTP), (2) агрегация по 30 дням (OLAP).

Решение: Dual-model. PostgreSQL для точных запросов, ClickHouse для count-distinct, percentiles. Processor пишет в оба.

Альтернативы: Только PostgreSQL (медленно на аггах), только ClickHouse (нет транзакций).

Последствия: +стоимость, +операционная сложность, но query performance в 100x.

---

**ADR-3: S3 для холодного архива вместо долгосрочного Postgres**

Контекст: 1 год за 1 ТБ, поиск в архиве редко, GDPR требует хранения.

Решение: Архивируем в S3 каждый месяц, Postgres чистим. Поиск по архиву — batch job.

Альтернативы: Долгосрочный Postgres (дорого).

Последствия: Медленный доступ к архиву за дешевизну.

---

## 3. Сайзинг

### RPS и пропускная способность

| Сценарий | Расчёт | Значение |
|---|---|---|
| **RPS base** | 500M DAU × 10 evt / 86400 s | 5 800 RPS |
| **RPS peak** | 5800 × 17x | 100 000 RPS |
| **Throughput raw** | 500M × 2 КБ / 86400 s | 12 ГБ/сек |
| **Throughput compressed** | 12 ГБ × 0.2 | 2.4 ГБ/сек (~200 ГБ/день) |
| **Краш-дампы (дедуп+сжим)** | 1 ПБ/год → 100 ТБ/год | 8 ТБ/месяц |

### Хранилище

| Слой | Период | Объём | Тип |
|---|---|---|---|
| **PostgreSQL** | 30 дн | 9 ТБ (data + indices) | OLTP |
| **ClickHouse** | 30 дн agg | 500 ГБ | OLAP |
| **S3** | 12 месяцев | 2.4 ТБ | Archive |
| **Elasticsearch** | 30 дн | 500 ГБ | Search |
| **Redis** | 1 ч cache | 50 ГБ | In-memory |

### Инфраструктура и стоимость

| Компонент | Кол-во | Конфиг | Годовая стоимость |
|---|---|---|---|
| API Gateway | 10 pods | 8 vCPU / 16 ГБ | $50K |
| Processor | 15–40 pods | 4 vCPU / 8 ГБ | $100K |
| PostgreSQL | 6 (3+3 standby) | 16 vCPU / 64 ГБ / 2 ТБ | $200K |
| ClickHouse | 3 nodes | 32 vCPU / 128 ГБ / 1 ТБ | $300K |
| Kafka | 5 brokers | 8 vCPU / 32 ГБ / 500 ГБ | $150K |
| Elasticsearch | 6 (3D+3M) | 8 vCPU / 16 ГБ / 1 ТБ | $150K |
| S3 | 2.4 ТБ | — | $50K |
| Redis | 3 cluster | 4 vCPU / 32 ГБ | $60K |
| CDN / LB / Monitoring | — | — | $70K |
| **Итого** | | | **$1.13M/год** |

---

## 4. Хранение данных

### Выбор БД

| БД | Роль | Причина |
|---|---|---|
| **PostgreSQL** | Хот-хранилище (30 дн) | ACID, complex индексы, point queries быстрые |
| **ClickHouse** | Аналитика + timeseries | Vectorized, count-distinct, P95 за сек |
| **Elasticsearch** | Full-text поиск | Быстрый поиск по stacktrace и сообщениям |
| **S3** | Долгосрочный архив | Дешево + compliance |
| **Redis** | Дедупликация в памяти | Fast HyperLogLog и cache хешей |

### Схема шардирования

**PostgreSQL:** Hash по app_id + Range по дате (месячные партиции)

```sql
CREATE TABLE events (
  app_id TEXT NOT NULL,
  date DATE NOT NULL,
  timestamp BIGINT NOT NULL,
  event_id UUID NOT NULL,
  data JSONB,
  PRIMARY KEY (app_id, date, timestamp, event_id)
) PARTITION BY RANGE (date);

CREATE INDEX idx_app_ts ON events (app_id, timestamp);
CREATE INDEX idx_app_type ON events (app_id, event_type, date);

-- Старые партиции архивируются в S3
```

**ClickHouse:** ReplicatedMergeTree, RF=2

```sql
CREATE TABLE analytics_agg (
  date Date,
  app_id String,
  version String,
  os_type String,
  country String,
  error_type String,
  crash_count UInt64,
  affected_users HyperLogLog(4),
  cpu_p95 Float32,
  memory_p95 Float32
) ENGINE = ReplicatedSummingMergeTree()
PARTITION BY date
ORDER BY (app_id, version, os_type)
TTL date + INTERVAL 30 DAY;
```

**S3:** Partitioned by month, Parquet format

```
s3://telemetry-archive/2025-08/app_id=com.example.app/events.parquet
```

### Стратегия кэширования

| Уровень | Данные | TTL | Размер |
|---|---|---|---|
| **Redis** | Stacktrace хеши (дедупликация) | 1 час | 10 ГБ |
| **Redis** | Device metadata | 24 часа | 5 ГБ |
| **Redis** | Top-20 версий/день | 1 час | 100 МБ |
| **Elasticsearch** | Query result cache | 1 час | 1 ГБ |
| **ClickHouse** | Current month partitions | Persistent | 500 ГБ |

---

## 5. Взаимодействие

### Схема взаимодействия сервисов

```
SDK [batch, offline buffer]
  │ HTTP/2 POST /api/v1/ingest
  v
[API Gateway] ──rate limit─┐
  │                         v Kafka [publish]
  ├──────────────────────────────────────┐
  │                                      v
  ├──> Processor ──> Postgres [hot]
  │    (dedup,enrich,sample)
  │
  ├──> Indexer ──> Elasticsearch [search]
  │
  └──> Archiver ──> S3 [cold]
  
  ┌──────────────────┐
  │ Aggregator ──────> ClickHouse [agg]
  └──────────────────┘
  
  REST API <─ Dashboards / Alert Manager
```

### Выбор протоколов

| Связь | Протокол | Причина |
|---|---|---|
| SDK → Gateway | HTTP/2 + TLS 1.3 | Мобильная сеть, мультиплексирование |
| SDK offload | Protocol Buffers | -50% размер, быстрее парсинг |
| Gateway → Kafka | Binary (librdkafka) | Скорость + atomicity |
| Processor → Postgres | TCP (pq) | ACID |
| Processor → Elasticsearch | HTTP/REST | Batch indexing |
| Alert → Slack | Webhook HTTP | Async retry-able |

### API контракты (5 шт)

**1. Ingest Event**

```protobuf
POST /api/v1/ingest
Authorization: Bearer <api_token>
Content-Type: application/protobuf

message TelemetryBatch {
  string app_id = 1;
  string app_version = 2;
  repeated TelemetryEvent events = 3;
}

message TelemetryEvent {
  int64 timestamp_ms = 1;
  enum Type { LOG=0; CRASH=1; PERF=2; }
  Type type = 2;
  string message = 3;
  bytes stacktrace = 5;
}

Response: 202 Accepted
Errors: 400, 401, 429 (SDK buffers), 503
```

**2. Query Analytics**

```graphql
POST /api/v1/analytics/query

query {
  crashes(app_id: "com.example.app", from: "2025-09-01", to: "2025-09-21") {
    totalCount
    trend(bucket: "1d") { date count }
    topErrors(limit: 10) { message count }
  }
}
```

**3. Crash Detail**

```http
GET /api/v1/crashes/{crash_id}

Response:
{
  "crash_id": "uuid",
  "stacktrace": "...",
  "breadcrumbs": [...],
  "device": { "model": "iPhone 14", "os_version": "17.1" },
  "first_seen": "2025-09-15T08:00:00Z",
  "last_seen": "2025-09-21T14:32:00Z",
  "duplicate_count": 1234
}
```

**4. Create Alert Rule**

```json
POST /api/v1/alerts/rules

{
  "name": "High crash rate iOS",
  "app_id": "com.example.app",
  "condition": "crashes_per_min > 100 && os_type='iOS'",
  "time_window_minutes": 5,
  "notification": {
    "channels": ["slack:#oncall"],
    "message": "{{count}} crashes in {{window}} min"
  }
}
```

**5. Export Events**

```http
POST /api/v1/exports

{
  "app_id": "com.example.app",
  "start_date": "2025-09-01",
  "end_date": "2025-09-21",
  "format": "parquet"
}

Response: 202 Accepted
GET /api/v1/exports/{export_id} ──> download_url
```

---

## 6. Надёжность

### RTO / RPO

| Компонент | RTO | RPO | Обоснование |
|---|---|---|---|
| **API Gateway** | 1 мин | N/A | Stateless; переключение на здоровые поды |
| **Kafka** | 2 мин | 0 | RF=3, MIR=2; потребители переподключаются |
| **PostgreSQL** | 5 мин | 0 в DC | Patroni failover; sync-standby в AZ-B |
| **ClickHouse** | 30 мин | 15 мин | Пересчёт из Kafka; потеря часа допустима |
| **Elasticsearch** | 15 мин | 5 мин | Переиндексирование из Kafka |
| **S3** | N/A | N/A | Кросс-регион репликация |
| **Redis** | 5 мин | N/A | Только кеш; отказ = рост нагрузки |

### Репликация

```
Region: ru-central (3 AZ: A, B, C)

PostgreSQL:
  Primary (AZ-A) <--sync--> Standby (AZ-B)
                 \--async-->  DR-replica (ru-west)
  Patroni DCS: etcd (5 узлов)
  Failover TTL: 30 сек
  
Kafka (5 brokers):
  RF=3, min.insync=2, acks=all
  MirrorMaker 2 → DR (async, 7d lag)
  
ClickHouse:
  RF=2 (AZ-B + AZ-C)
  Восстановление: ~30 мин из Kafka
  
Elasticsearch:
  RF=1 + replica/shard в разных AZ
  Snapshots в S3 ежедневно
```

### Паттерны отказоустойчивости

| Паттерн | Где | Конфиг |
|---|---|---|
| **Timeout** | SDK → Gateway | connect 200ms, request 2s |
| **Timeout** | Gateway → Kafka | 1 sec |
| **Retry** | SDK | exponential 1s/10s/60s + jitter, max 3 |
| **Retry** | Processor → Postgres | 2 повтора по deadlock → DLQ |
| **Circuit Breaker** | Processor → Elasticsearch | fail-open после 10% ошибок |
| **Bulkhead** | Gateway | отдельные потоки ingest vs analytics |
| **Load shedding** | Gateway | drop analytics при RPS > 80K |
| **Idempotency** | Всюду | event_id (client UUID) |

**SDK offline-буфер:** 5 МБ на диске, батарейный mode sampling 0.1

### План DR (RTO 30 мин)

| Шаг | Действие | Время |
|---|---|---|
| 0 | Объявлен инцидент, баннер | 0–2 мин |
| 1 | Fencing основного региона | 2–5 мин |
| 2 | Промоут DR-реплик Postgres | 5–10 мин |
| 3 | Масштабирование сервисов в DR | 10–18 мин |
| 4 | Переключение DNS + GSLB | 15–20 мин |
| 5 | Переключение Kafka потребителей | 18–25 мин |
| 6 | Синтетическая проверка | 25–28 мин |
| 7 | Снятие баннера | 28–30 мин |

---

## 7. Безопасность

### Аутентификация и авторизация

**SDK (приложения):**
- API token (40 байт random), Bearer header
- TLS 1.3 + certificate pinning

**Пользователи (веб):**
- OAuth 2.0 + OpenID Connect (Google / GitHub)
- Access JWT RS256, 1 час
- Refresh в httpOnly Secure cookie
- MFA обязательна для admins

**Авторизация:**
- RBAC: `viewer` (читать), `admin` (управлять)
- Resource-level: видит только свои приложения

**Сервис-сервис:** mTLS + SPIFFE ID

### Шифрование

| Слой | Механизм |
|---|---|
| **SDK → Gateway** | TLS 1.3 + certificate pinning |
| **Между сервисами** | mTLS 1.3 |
| **На диске** | Прозрачное шифрование (KMS) |
| **Персональные данные** | AES-256-GCM приложение |
| **Бэкапы** | Шифрование pgBackRest |
| **S3 архив** | KMS encryption |

**Ключи:** Vault, ротация ежегодно

**GDPR:** Retention 30 дн + 1 год архив; soft-delete по user_id

---

## 8. Observability

### Метрики (15 шт)

| # | Компонент | Тип | Метрика | Зачем |
|---|---|---|---|---|
| 1 | API Gateway | RED | RPS по типу | Профиль нагрузки |
| 2 | API Gateway | RED | Error rate (4xx, 5xx) | Различить клиента и сервера |
| 3 | API Gateway | RED | p99 latency | SLO 100ms |
| 4 | Kafka | USE | Consumer lag по группам | Indexer/Archiver отставание |
| 5 | Kafka | USE | Partition leader elections/min | Нестабильность |
| 6 | Postgres | USE | Pool connections | Исчерпание → таймауты |
| 7 | Postgres | USE | Table size | Рост > 50%/день → проблема |
| 8 | Postgres | RED | Deadlocks/min | Конкуренция за app_id |
| 9 | Processor | RED | Redis dedup cache size | Утечка памяти? |
| 10 | Processor | RED | Events processed/min | Сравнить с Kafka lag |
| 11 | Elasticsearch | USE | JVM heap % | > 85% → OOM risk |
| 12 | Elasticsearch | USE | Rejected searches | Раньше, чем latency |
| 13 | ClickHouse | RED | Query latency p95 | < 1 сек для дашбордов |
| 14 | ClickHouse | USE | Table rows | Нужны ли TTL? |
| 15 | Продукт | бизнес | Уникальные app/day | Рост/отток |

### Алерты (7 шт)

```
1. Обрыв приёма (CRITICAL)
   Условие: ingest_requests < 1K за 5 мин
   Означает: SDK не могут отправлять
   Реакция: Gateway logs → Kafka status → network

2. Высокий lag Processor (WARNING)
   Условие: kafka lag > 100K за 10 мин
   Означает: Процессор отстаёт, индексирование замедляется
   Реакция: Processor logs → Postgres deadlocks → add инстансы

3. Недостаток диска Postgres (CRITICAL)
   Условие: disk < 20% за 2 мин
   Реакция: Проверить архивизацию, расширить диск

4. Elasticsearch red status (WARNING)
   Условие: status = red за 1 мин
   Означает: Нет primary shard, поиск неполный
   Реакция: ES logs, JVM heap, disk; restart nodes

5. Spike crash rate (CRITICAL)
   Условие: crashes/min > 1000 за 5 мин
   Означает: App ломается в масс-порядке
   Реакция: Какая версия? Откатить? Filter по OS/device

6. ClickHouse query timeout (WARNING)
   Условие: query latency > 10s за 5 мин
   Реакция: Какие запросы? Индекс? Масштабировать

7. S3 upload failures (WARNING)
   Условие: failures > 10/min за 5 мин
   Реакция: AWS status, IAM role, retry backoff
```

### Логирование и трейсинг

**Логирование:** JSON → Loki (30 дн) → S3
- Обязательно: timestamp, level, service, app_id, event_id, trace_id
- Не логируем: device_id, user_id (маскируем)

**Трейсинг:** OpenTelemetry → Tempo
- 100% sampling для crashes, 1% для логов, tail-based для ошибок
- Traced paths: SDK → Gateway → Kafka → Processor → DB

---

## 9. Тестирование

### План нагрузочного тестирования

**Инструмент:** k6. **Стенд:** 1M apps, 100M events/месяц

| # | Тип | Сценарий | Длит-ть | Проверяем |
|---|---|---|---|---|
| 1 | Smoke | 1 VU на каждый flow | 5 мин | Работоспособность |
| 2 | Load | 100K RPS, равномерно | 30 мин | SLO (p99 < 100ms) |
| 3 | Stress | Ступени до отказа | до отказа | Точка насыщения |
| 4 | Spike | 20K → 100K за 30s | 15 мин | Load shedding, HPA |
| 5 | Soak | 60K RPS непрерывно | 6 часов | Утечки памяти |
| 6 | Дедупликация | Один stacktrace 10K раз | 5 мин | Redis cache работает |
| 7 | Analytics storm | 1K параллельных запросов | 10 мин | ClickHouse не упал |
| 8 | Failover | Kill broker Kafka | 15 мин | Lag не растёт |

**Критерии приёмки:**

| Показатель | Порог |
|---|---|
| p99 ingest latency | ≤ 100 ms |
| Error 5xx rate | ≤ 0.1 % |
| Дедупликация (сценарий 6) | записей ≤ 100 (не 10K) |
| ClickHouse query p95 | ≤ 1 сек |
| Дропы при spike | ≤ 0.01 % |

### Chaos Engineering (3 эксперимента)

```
Эксперимент 1: Деградация Kafka
Steady state:   p99 < 100ms; дедупликация работает
Гипотеза:       при kill одного из 5 брокеров события не теряются, failover < 10s
Метод:          k6 load + kubectl kill pod
Наблюдаем:      ingest_latency, error_rate, lag
Rollback:       если 5xx > 1%

Эксперимент 2: Failover PostgreSQL
Steady state:   события пишутся; history доступна; RPO=0
Гипотеза:       Patroni failover за < 60s; sync-standby обеспечивает RPO=0
Метод:          kill -9 primary pod
Наблюдаем:      время failover, lag реплик, консистентность
Rollback:       если failover > 2 мин

Эксперимент 3: Cache stampede (Redis crash)
Steady state:   дедупликация через Redis; 10K одинаковых → 100 в Postgres
Гипотеза:       при краше Redis fallback через Postgres медленнее, но консистентно
Метод:          остановить Redis, продолжить нагрузку
Наблюдаем:      дедупликация (записей на stacktrace), latency Processor
Rollback:       если > 1K записей
```

---

**Дата:** 2025-09-21  
**Формат:** полная документация по 9 пунктам задания, строго и конкретно
