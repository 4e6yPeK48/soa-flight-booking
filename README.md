# Flight Booking: gRPC и Redis

Два Go-сервиса бронируют места на авиарейсах: Booking принимает REST-запросы, Flight управляет рейсами через gRPC. У каждого сервиса своя PostgreSQL. Flight кеширует чтение в Redis с Sentinel, Booking восстанавливает незавершённые операции из журнала в БД.

[Архитектура](docs/architecture.md) объясняет границы, транзакции и гарантии при сбоях. [QA](docs/qa.md) связывает десять пунктов задания с проверками. [Защита](docs/defense.md) содержит порядок демонстрации и ответы на вопросы. Исходное задание сохранено в [assignment.ru.txt](docs/assignment.ru.txt).

## Запуск

Нужен Docker с Compose и доступный Docker daemon. Запуск из корня репозитория:

```bash
docker compose up --build
```

Compose поднимает две PostgreSQL, применяет Flyway-миграции, запускает Redis master, replica и три Sentinel, затем оба приложения. Для запуска в фоне с ожиданием готовности:

```bash
make up
curl --fail-with-body http://127.0.0.1:8081/health
```

На локальной машине автора Docker работает в контексте Colima distsys. Если он нужен в вашей среде, сначала выполните `export DOCKER_CONTEXT=colima-distsys`. В CI используется контекст default. Приложения собираются внутри Docker: Go, protoc и Node.js на хосте для запуска не нужны. На Apple Silicon Flyway запускается через эмуляцию linux/amd64.

Файл .env необязателен: Compose уже содержит учебные значения по умолчанию. Для изменения настроек скопируйте [.env.example](.env.example) в .env. PostgreSQL и Redis доступны только внутри сети Compose; REST и gRPC публикуются на loopback хоста. Учебные пароли и API key из примера не предназначены для внешнего развёртывания.

```bash
make logs  # логи всех контейнеров
make down  # остановка с сохранением данных в named volumes
```

## REST: полный пример

Миграция создаёт рейс SU1234 SVO → LED на 1 апреля 2030 года: 100 мест, цена 150050 копеек, то есть 1500,50 RUB за место. Все цены в REST, gRPC и БД — целые копейки RUB. В примере для извлечения ID нужен Python 3; можно также скопировать ID из JSON вручную.

```bash
BASE=http://127.0.0.1:8081
FLIGHT_ID=11111111-1111-4111-8111-111111111111
KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl --fail-with-body -sS "$BASE/flights?origin=SVO&destination=LED&date=2030-04-01"
curl --fail-with-body -sS "$BASE/flights/$FLIGHT_ID"

BOOKING=$(curl --fail-with-body -sS "$BASE/bookings" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" \
  -d '{"user_id":1,"flight_id":"11111111-1111-4111-8111-111111111111","passenger_name":"Матвей Чупрына","passenger_email":"matvey@example.test","seat_count":2}')
printf '%s\n' "$BOOKING"
BOOKING_ID=$(printf '%s' "$BOOKING" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl --fail-with-body -sS "$BASE/bookings/$BOOKING_ID"
curl --fail-with-body -sS "$BASE/bookings?user_id=1"
curl --fail-with-body -sS -X POST "$BASE/bookings/$BOOKING_ID/cancel"
curl --fail-with-body -sS -X POST "$BASE/bookings/$BOOKING_ID/cancel"
curl --fail-with-body -sS "$BASE/flights/$FLIGHT_ID"
```

Создание возвращает 201, CONFIRMED и total_price=300100 копеек. Отмена возвращает 200 и CANCELLED; повторная отмена не возвращает места второй раз. Итоговая сумма сохраняется после изменения цены рейса. При ранее изменённых демоданных цена и число мест могут отличаться.

Заголовок `Idempotency-Key` необязателен. Без него сервер создаёт новую операцию и возвращает сгенерированный ключ в заголовке ответа. Для безопасного повтора после таймаута задавайте собственный UUID заранее и повторяйте тот же запрос с тем же ключом. Другой payload с занятым ключом возвращает 409. Новый запрос без ключа может создать ещё одно бронирование: гарантии exactly-once для таких повторов нет.

Ответ 503 с кодом UNKNOWN_OUTCOME означает, что исход COMMIT неизвестен. Не создавайте новую операцию: повторите запрос с прежним ключом. OPERATION_IN_PROGRESS также требует повтора с прежним ключом. Ответы об ошибке могут содержать operation_id и booking_id для диагностики.

| Метод и путь | Результат |
|---|---|
| GET /flights?origin=SVO&destination=LED&date=2030-04-01 | Запланированные рейсы; без date — все даты |
| GET /flights/{id} | Рейс по UUID, включая незапланированный статус |
| POST /bookings | Создание бронирования |
| GET /bookings/{id} | Бронирование по UUID |
| GET /bookings?user_id=1 | Бронирования указанного пользователя |
| POST /bookings/{id}/cancel | Идемпотентная отмена |
| GET /health | Доступность Booking и его БД |
| GET /openapi.json | JSON-контракт REST |

REST-контракт: [openapi.json](internal/api/openapi.json), в запущенном приложении — [GET /openapi.json](http://127.0.0.1:8081/openapi.json). REST не аутентифицирует конечного пользователя; user_id — параметр учебного API, а фильтрация по нему не заменяет проверку доступа. Дата поиска — календарный день UTC.

## gRPC и изменение рейса

Контракт и коды бизнес-ошибок описаны в [flight.proto](proto/flight/v1/flight.proto). Во всех пяти методах требуется metadata x-api-key. Для ручных вызовов установите grpcurl; reflection для примеров не нужен.

```bash
grpcurl -plaintext -import-path proto -proto flight/v1/flight.proto \
  -H 'x-api-key: local-demo-key' \
  -d '{"flight_id":"11111111-1111-4111-8111-111111111111"}' \
  localhost:50051 flight.v1.FlightService/GetFlight

# 180050 копеек = 1800,50 RUB за место
grpcurl -plaintext -import-path proto -proto flight/v1/flight.proto \
  -H 'x-api-key: local-demo-key' \
  -d '{"flight_id":"11111111-1111-4111-8111-111111111111","price":"180050"}' \
  localhost:50051 flight.v1.FlightService/UpdateFlight

grpcurl -plaintext -import-path proto -proto flight/v1/flight.proto \
  -H 'x-api-key: local-demo-key' \
  -d '{"flight_id":"11111111-1111-4111-8111-111111111111","status":"CANCELLED"}' \
  localhost:50051 flight.v1.FlightService/UpdateFlight

# Вернуть деморейс в исходное состояние
grpcurl -plaintext -import-path proto -proto flight/v1/flight.proto \
  -H 'x-api-key: local-demo-key' \
  -d '{"flight_id":"11111111-1111-4111-8111-111111111111","price":"150050","status":"SCHEDULED"}' \
  localhost:50051 flight.v1.FlightService/UpdateFlight
```

UpdateFlight меняет цену и/или статус. Отмена рейса исключает его из поиска и запрещает новые резервы; существующие бронирования автоматически не отменяются.

## Настройки и проверки

| Переменная | Значение по умолчанию и смысл |
|---|---|
| COMPOSE_PROJECT_NAME | soa-flight-booking; изоляция контейнеров и volumes |
| BOOKING_PORT / FLIGHT_PORT | 8081 / 50051 на хосте |
| FLIGHT_API_KEY | local-demo-key; одинаковый ключ обоих сервисов |
| RPC_ATTEMPTS / RPC_BACKOFF | До 3 вызовов invoker в interceptor; паузы 100ms, 200ms |
| RPC_ATTEMPT_TIMEOUT / RPC_TIMEOUT | 500ms на попытку, 2s на логический RPC |
| CB_FAILURE_THRESHOLD / CB_WINDOW | 5 последовательных инфраструктурных ошибок в окне 60s |
| CB_OPEN_TIMEOUT | 15s до единственного пробного вызова |
| HTTP_TIMEOUT / OPERATION_TIMEOUT | 8s на контекст обработки и чтение HTTP-запроса, 10s до восстановления зависшей CREATE |
| RECOVERY_INTERVAL | 1s между проходами восстановления |

Адреса БД задаются через DATABASE_URL отдельно для каждого приложения. Внутри Compose используются GRPC_ADDR=:50051, HEALTH_ADDR=:9090 у Flight, HTTP_ADDR=:8080 и FLIGHT_ADDR=flight:50051 у Booking. Redis-клиент получает REDIS_MASTER_NAME=flights и REDIS_SENTINEL_ADDRS с адресами трёх Sentinel.

```bash
make check        # на хосте: Go 1.26, protoc 36.2; генерация, race tests, vet
make integration  # в Docker: PostgreSQL и Redis, без запуска приложений
make e2e          # в Docker: реальные HTTP/gRPC и сбои контейнеров
npm ci            # Node.js 24+, нужен только для HTML-отчёта
npm test          # проверка выбора артефактов отчёта
make e2e-report
make e2e-open
```

Лимит retry относится к попыткам приложения. В gRPC 1.85.0 WithDisableRetry отключает retry по service config, но сохраняет transparent retry для ещё не обработанного сервером запроса. Поэтому три вызова invoker и один логический HALF_OPEN probe не ограничивают абсолютное число сетевых передач.

HTTP_TIMEOUT также ограничивает чтение тела запроса через server ReadTimeout. Синхронная компенсация после ошибки использует отдельный background-бюджет; полный ответ клиенту может занять больше 8s.

У обращений к необязательному кешу отдельный фиксированный бюджет: до 50ms на GET и до 50ms на SET. Он оставляет время для PostgreSQL в пределах RPC deadline. Redis-клиент учитывает context deadline; внутренние retry кеша отключены.

E2E останавливает и перезапускает контейнеры своего Compose-проекта. Запускайте его в выделенном окружении, например с COMPOSE_PROJECT_NAME=soa-flight-booking-qa и свободными BOOKING_PORT/FLIGHT_PORT. Результаты Allure находятся в artifacts/e2e/<RUNID>/allure-results, HTML — рядом в allure-report. Команда make e2e запускает именованный контейнер и после завершения копирует /artifacts через docker cp, в том числе при падении тестов. Это работает и с удалённым Docker context: bind mount каталога артефактов на хосте daemon не требуется. Исходный код выхода проваленных тестов сохраняется. Указатель latest-run выбирает последний запуск. Наличие тестов и workflow не подтверждает их успешный прогон; фактический статус фиксируется в QA-документе.
