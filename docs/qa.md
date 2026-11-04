# Проверка Flight Booking

Приёмка проходит через REST и gRPC с проверкой обеих PostgreSQL, затем через остановку и перезапуск контейнеров. HTTP 200 без проверки остатков, журнала и резерваций не доказывает завершение бронирования. Матрица ниже описывает зарегистрированные проверки. Оценку выставляет преподаватель после приёмки.

## Десять пунктов задания

| № | Реализация | Проверка в исходниках | Что должно быть подтверждено |
|---|---|---|---|
| 1. gRPC-контракт | proto/flight/v1/flight.proto, gen/flight/v1 | `make check`; `TestInvalidRequests`; E2E `All five gRPC methods require authentication` | Пять методов, Timestamp, enum, кодогенерация и коды ошибок |
| 2. ER и 3NF | docs/architecture.md, db/flight/V1__flight.sql, db/booking/V1__booking.sql | `TestPostgresReservationLifecycle`; E2E `Last seat has exactly one concurrent winner` | Ограничения схемы, один резерв на booking_id, остаток не ниже нуля; 3NF сверяется по схеме |
| 3. Два сервиса и БД | cmd/booking-service, cmd/flight-service, compose.yaml, db | E2E `REST lifecycle and user isolation`; `TestPostgresCreateAndReplay` | Flyway, REST, gRPC и независимые БД работают вместе |
| 4. gRPC при создании и отмене | internal/booking/server.go | `TestPostgresCreateAndReplay`; E2E `REST lifecycle and user isolation` | GetFlight → ReserveSeats → CONFIRMED; ReleaseReservation при отмене |
| 5. Транзакции | internal/flight/server.go, booking_operations | `TestPostgresReservationLifecycle` / `reservation write failure rolls back seats and revision`; `TestPostgresFailedUpstreamNeverCreatesBooking`; E2E `Last seat has exactly one concurrent winner` | Один победитель на последнее место, rollback локальных изменений, отсутствие booking при неудаче upstream |
| 6. Аутентификация | internal/transport/transport.go, FLIGHT_API_KEY в Compose | `TestServerAuth`; E2E `All five gRPC methods require authentication` | Все пять методов возвращают UNAUTHENTICATED без ключа и с неверным ключом |
| 7. Redis | cache_revision, internal/flight/server.go | `TestPostgresRedisRevisionCache`; E2E `Price snapshot and cache invalidation`, `Redis outage falls back to PostgreSQL` | TTL 300s, hit/miss, новые версии после трёх мутаций, fallback на БД |
| 8. Retry | internal/transport/transport.go | `TestRetryMetadataAndBudget`, `TestRetryOnlyInfrastructureErrors`, `TestAttemptLimitCannotExceedThree`; E2E `Reservation and release are idempotent` | Не более трёх попыток, backoff, повтор только инфраструктурных ошибок, отсутствие двойного резерва |
| 9. Sentinel | compose.yaml, redis/start.sh, cmd/flight-service/main.go | E2E `Sentinel promotes replica and cache recovers` | Promotion реплики и запись кеша приложением на новом master |
| 10. Circuit breaker | NewClientInterceptor | `TestHalfOpenSingleWireProbeAndRecovery`, `TestStaleCompletionCannotChangeNewGeneration`, `TestLogsExposeTransitionsWithoutCredentials`; E2E `Flight outage opens and recovers circuit` | CLOSED → OPEN → HALF_OPEN → CLOSED, один probe, быстрый 503, переходы в логах |

Кеш использует дополнительное чтение revision из PostgreSQL перед Redis и логическую инвалидацию вместо DEL. Эта разница с буквальной Cache-Aside в условии разобрана в архитектуре и должна быть явно показана при защите.

## Дополнительные гарантии и проверки

| Риск | Проверки | Граница доказательства |
|---|---|---|
| Повтор HTTP создаёт вторую бронь | `TestPostgresCreateAndReplay`; E2E `HTTP idempotency replay and payload conflict` | Один ключ с тем же payload возвращает прежний booking; запросы без ключа независимы |
| Поздний Reserve приходит после Release | `TestPostgresReservationLifecycle` / `release before reserve blocks late operation`; E2E `Reservation and release are idempotent` | Durable tombstone запрещает новый резерв с прежним booking_id |
| Booking упал после удалённого резерва | `TestPostgresCrashAfterReserveRecovery`; E2E `Booking crash after reserve is compensated` | E2E убивает контейнер после SQL-подтверждения резерва и проверяет восстановление после старта |
| Booking упал после удалённого Release | `TestPostgresCancelRecovery`; E2E `Booking crash after release finishes cancellation` | После рестарта CANCEL становится DONE, booking — CANCELLED, места возвращены один раз |
| Компенсация сама недоступна | `TestPostgresFailedCleanupRemainsDurableAndReplaysOriginalError` | Реальная Booking PostgreSQL и подменённый Flight-клиент; намерение сохраняется для повтора |
| Поздняя финализация спорит с recovery | `TestPostgresPendingRecoveryAndNoDuplicateRPC`, `TestPostgresRecoveryNeverReleasesCommittedBooking` | Проверка блокировок журнала в PostgreSQL с подменённым Flight-клиентом |
| UpdateFlight меняет историю покупки | E2E `Price snapshot and cache invalidation` | Цена booking сохранена, Get/Search обновлены, CANCELLED исчезает из поиска |
| Ошибка или переполнение денежного расчёта | `TestCheckedPrice`; E2E `REST strict JSON and input validation` | Точное умножение копеек, отказ до резерва при переполнении |
| Неверная дата или статус в поиске | `TestPostgresReservationLifecycle` / `UTC date filtering and stable search order`; E2E `Search uses UTC dates and scheduled status` | Граница дня UTC, только SCHEDULED, стабильный порядок |

Потеря сетевого ответа именно на COMMIT не считается доказанной сценариями остановки процесса. Реализация различает неизвестный исход и rollback, но для полного доказательства этого случая нужен отдельный fault injection на границе PostgreSQL COMMIT. Успех теста с mock-клиентом также не заменяет сетевой E2E.

## Как запускать

Для native-проверок нужны Go 1.26 и protoc 36.2. Генераторы устанавливаются закреплёнными версиями через Makefile. `make check` проверяет воспроизводимость сгенерированных файлов, запускает race detector и vet. Интеграционные тесты без переменных подключения пропускаются: обычный go test не доказывает PostgreSQL/Redis-поведение.

```bash
make check
make integration
```

Integration выполняется в Docker и получает адреса обеих PostgreSQL и Redis из Compose. Flight-тесты используют реальную PostgreSQL и Redis; Booking-тесты используют реальную PostgreSQL и подменяют Flight-клиент для управляемых ошибок.

E2E требует Docker socket: сценарии выполняют stop/start/kill контейнеров. Используйте отдельный Compose-проект и свободные порты, без параллельной ручной работы в этом окружении:

```bash
export COMPOSE_PROJECT_NAME=soa-flight-booking-qa
export BOOKING_PORT=18081
export FLIGHT_PORT=15051
# На машине автора также: export DOCKER_CONTEXT=colima-distsys
make up
make e2e
```

E2E запускается последовательно. В TestFlightBooking зарегистрированы 14 сценариев: девять функциональных, затем Redis outage, Sentinel failover, Flight outage и два падения Booking. Fixtures создаются с новыми UUID; cleanup удаляет только записи сценария. Имена сервисов Docker ищутся по меткам текущего Compose-проекта. Запущенные после сбоя контейнеры восстанавливаются cleanup-обработчиками, но при аварийной остановке самого тестового раннера состояние стоит проверить через docker compose ps.

Проверки SQL подтверждают остаток, число booking, статус резерва и состояние журнала. Для fault-сценариев тест сначала дожидается нужного состояния БД, затем убивает Booking и после рестарта ждёт сходимости. Таймаут ожидания завершает сценарий ошибкой.

## Артефакты и фактический результат

```bash
npm ci   # Node.js 24 или новее
npm test
make e2e-report
make e2e-open
```

Каждый запуск создаёт каталог artifacts/e2e/<RUNID>/allure-results. Allure содержит HTTP-запросы и ответы, SQL-наблюдения, действия с Docker, результаты ожиданий и очищенные логи сервисов. Ключ сервиса маскируется во вложениях. Команды отчёта читают указатель artifacts/e2e/latest-run и пишут HTML в allure-report рядом с результатами. Отчёт после проваленного прогона полезен для диагностики и не делает прогон успешным.

| Уровень | Статус при подготовке документа |
|---|---|
| Тесты зарегистрированы | Состав сверён с исходниками |
| Native unit / race | Частичный успешный прогон известен; окончательная проверка root verification pending |
| Полный make check с кодогенерацией | Root verification pending |
| Docker integration | Root verification pending |
| Полный E2E и fault-сценарии | Root verification pending |
| Allure HTML и npm test | Root verification pending |
| Удалённый CI | Результат не подтверждён |

В .github/workflows/tests.yml описаны отдельные этапы генерации, Go-проверок, integration, запуска Compose, E2E и HTML-отчёта. Workflow собирает диагностику и загружает artifacts/e2e; очищает только свой проект с отдельным именем. Наличие workflow не подтверждает его удалённое выполнение. Оценка «10/10» требует успешной приёмки всех блоков по порядку. Одного unit PASS недостаточно.
