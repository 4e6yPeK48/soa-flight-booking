# Flight Booking: gRPC и Redis

Booking принимает REST-запросы на бронирование, Flight управляет рейсами через gRPC. У каждого Go-сервиса своя PostgreSQL. Redis с Sentinel кеширует чтение; журнал Booking восстанавливает незавершённые операции.

[Архитектура](docs/architecture.md) — данные и гарантии при сбоях; [QA](docs/qa.md) — проверки задания; [защита](docs/defense.md) — сценарий демонстрации. [Исходное задание](docs/assignment.ru.txt).

## Запуск

Нужен Docker с Compose и работающим daemon. Из корня репозитория:

```bash
docker compose up --build
# В другом терминале с теми же переменными окружения:
curl --fail-with-body "http://127.0.0.1:${BOOKING_PORT:-8081}/health"
# Остановка с сохранением данных:
docker compose down
```

`make up` запускает проект в фоне и ждёт готовности. Логи: make logs; остановка: make down. Используется активный Docker context. Приложения собираются внутри Docker; на Apple Silicon Flyway работает через эмуляцию linux/amd64.

Файл .env необязателен; настройки можно скопировать из [.env.example](.env.example). По умолчанию REST доступен на 127.0.0.1:8081, gRPC — на 127.0.0.1:50051. PostgreSQL и Redis доступны внутри Compose. Учебные пароли и API key рассчитаны на это окружение.

## REST

Деморейс SU1234 SVO → LED отправляется 1 апреля 2030 года: 100 мест по 150050 копеек. Цены в API и БД — целые копейки RUB; дата поиска — день UTC. Для извлечения ID в примере нужен Python 3.

```bash
BASE="http://127.0.0.1:${BOOKING_PORT:-8081}"
FLIGHT_ID=11111111-1111-4111-8111-111111111111
KEY=$(python3 -c 'import uuid; print(uuid.uuid4())')

curl --fail-with-body -sS "$BASE/flights?origin=SVO&destination=LED&date=2030-04-01"
curl --fail-with-body -sS "$BASE/flights/$FLIGHT_ID"
BOOKING=$(curl --fail-with-body -sS "$BASE/bookings" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" \
  -d '{"user_id":1,"flight_id":"11111111-1111-4111-8111-111111111111","passenger_name":"Иван Иванов","passenger_email":"passenger@example.test","seat_count":2}')
printf '%s\n' "$BOOKING"
BOOKING_ID=$(printf '%s' "$BOOKING" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl --fail-with-body -sS "$BASE/bookings/$BOOKING_ID"
curl --fail-with-body -sS "$BASE/bookings?user_id=1"
curl --fail-with-body -sS -X POST "$BASE/bookings/$BOOKING_ID/cancel"
curl --fail-with-body -sS -X POST "$BASE/bookings/$BOOKING_ID/cancel"
curl --fail-with-body -sS "$BASE/flights/$FLIGHT_ID"
```

На исходных данных создание возвращает 201, CONFIRMED и total_price=300100 (3001 RUB); отмена — 200 и CANCELLED. Повторная отмена не возвращает места дважды. Цена брони сохраняется при изменении тарифа.

Для повтора после таймаута заранее задавайте UUID в `Idempotency-Key`. Тот же ключ и payload возвращают прежний результат; другой payload — 409. Без ключа сервер создаёт его сам и возвращает в заголовке ответа, но новый запрос без ключа может создать вторую бронь.

При 503 UNKNOWN_OUTCOME исход COMMIT неизвестен: повторите запрос с прежним ключом. OPERATION_IN_PROGRESS требует такого же повтора. Ошибка может содержать operation_id и booking_id.

Все пути и схемы: [OpenAPI](internal/api/openapi.json), в приложении — [GET /openapi.json](http://127.0.0.1:8081/openapi.json). Поиск без date охватывает все даты; GET рейса по ID возвращает и незапланированный статус. REST не аутентифицирует пользователя: user_id фильтрует список, но не проверяет доступ. GET /health проверяет Booking и его БД.

## gRPC

Все пять методов [flight.proto](proto/flight/v1/flight.proto) требуют metadata x-api-key. Пример для grpcurl без reflection меняет цену на 180050 копеек (1800,50 RUB) и отменяет рейс:

```bash
grpcurl -plaintext -import-path proto -proto flight/v1/flight.proto \
  -H 'x-api-key: local-demo-key' \
  -d '{"flight_id":"11111111-1111-4111-8111-111111111111","price":"180050","status":"CANCELLED"}' \
  "localhost:${FLIGHT_PORT:-50051}" flight.v1.FlightService/UpdateFlight

# Восстановить деморейс:
grpcurl -plaintext -import-path proto -proto flight/v1/flight.proto \
  -H 'x-api-key: local-demo-key' \
  -d '{"flight_id":"11111111-1111-4111-8111-111111111111","price":"150050","status":"SCHEDULED"}' \
  "localhost:${FLIGHT_PORT:-50051}" flight.v1.FlightService/UpdateFlight
```

UpdateFlight принимает цену и/или статус. CANCELLED исключает рейс из поиска и запрещает новые резервы; существующие брони автоматически не отменяются.

## Проверки

```bash
make check        # Go 1.26, protoc 36.2: генерация, race tests, vet
make integration  # Docker: PostgreSQL и Redis
make e2e          # Docker: HTTP/gRPC и сбои контейнеров
npm ci            # Node.js 24+, только для отчёта
npm test          # проверки скриптов отчёта
make e2e-report
make e2e-open
```

E2E останавливает контейнеры: используйте отдельный COMPOSE_PROJECT_NAME и свободные BOOKING_PORT/FLIGHT_PORT. Запуск, изоляция и расположение Allure-артефактов описаны в QA.
