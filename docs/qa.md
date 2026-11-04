# Проверка Flight Booking

Проверки связывают REST/gRPC-ответы с остатками, резервациями и журналом в обеих PostgreSQL. Гарантии, параметры retry и ограничения кеша описаны в [архитектуре](architecture.md).

## Покрытие задания

Исходники схем находятся в migrations/flight и migrations/booking, транспорт — в internal/transport, сценарии E2E — в tests/e2e.

| № | Проверки | Ожидаемый результат |
|---|---|---|
| 1. gRPC | `make check`, TestInvalidRequests; E2E gRPC validation and not-found errors | Пять методов, Timestamp, enum, воспроизводимая генерация, коды ошибок |
| 2. ER и 3NF | Схемы миграций, ER; TestPostgresReservationLifecycle | Ограничения БД, один резерв на booking_id, неотрицательный остаток; 3NF сверяется по схеме |
| 3. Два сервиса и БД | compose.yaml; TestPostgresCreateAndReplay; E2E REST lifecycle and user isolation | Flyway, независимые БД, REST и gRPC работают вместе |
| 4. Межсервисные операции | TestPostgresCreateAndReplay; тот же REST lifecycle | GetFlight → ReserveSeats → CONFIRMED; ReleaseReservation при отмене |
| 5. Транзакции | TestPostgresReservationLifecycle; TestPostgresFailedUpstreamNeverCreatesBooking; E2E Last seat has exactly one concurrent winner | Rollback мест и revision при ошибке записи; нет booking после неудачи upstream; один победитель на последнее место |
| 6. API key | TestServerAuth; E2E All five gRPC methods require authentication | Каждый метод возвращает UNAUTHENTICATED без ключа и с неверным ключом |
| 7. Redis | TestPostgresRedisRevisionCache; E2E Price snapshot and cache invalidation, Redis outage falls back to PostgreSQL | TTL 300s, hit/miss, инвалидация после Update/Reserve/Release, fallback на БД |
| 8. Retry | TestRetryMetadataAndBudget, TestRetryOnlyInfrastructureErrors, TestAttemptLimitCannotExceedThree; E2E Reservation and release are idempotent | До трёх вызовов invoker, backoff для инфраструктурных ошибок, отсутствие двойного списания |
| 9. Sentinel | deploy/redis/start.sh; E2E Sentinel promotes replica and cache recovers | Promotion реплики и заполнение кеша на новом master |
| 10. Circuit breaker | TestHalfOpenSingleWireProbeAndRecovery, TestStaleCompletionCannotChangeNewGeneration, TestLogsExposeTransitionsWithoutCredentials; E2E Flight outage opens and recovers circuit | CLOSED → OPEN → HALF_OPEN → CLOSED, один логический probe, быстрый 503, переходы в логах без ключа |

## Проверки восстановления и границ

| Сценарий | Проверка и инвариант |
|---|---|
| Повтор HTTP | TestPostgresCreateAndReplay; E2E HTTP idempotency replay and payload conflict: прежний booking для того же ключа/payload, 409 для другого payload |
| Поздний Reserve | TestPostgresReservationLifecycle; E2E Reservation and release are idempotent: Release до Reserve сохраняет запрет, места не занимаются |
| Падение после резерва | TestPostgresCrashAfterReserveRecovery; E2E Booking crash after reserve is compensated: CREATE FAILED, booking отсутствует, резерв RELEASED |
| Падение после Release | TestPostgresCancelRecovery; E2E Booking crash after release finishes cancellation: CANCEL DONE, booking CANCELLED, возврат мест один раз |
| Недоступная компенсация | TestPostgresFailedCleanupRemainsDurableAndReplaysOriginalError: намерение сохранено для повтора |
| Гонка финализации и recovery | TestPostgresPendingRecoveryAndNoDuplicateRPC, TestPostgresRecoveryNeverReleasesCommittedBooking: завершённый booking не компенсируется |
| Поздний SET кеша | TestPostgresRedisLateFill, GetFlight/SearchFlights: барьер чтения и SET hook записывают старый payload после мутаций; прежний ключ имеет TTL, следующее чтение свежее |
| Снимок цены и отмена рейса | E2E Price snapshot and cache invalidation: цена booking сохранена, Get/Search обновлены, CANCELLED исключён из поиска |
| Чтение при отказе Flight | E2E Flight outage opens and recovers circuit: GET booking и список работают при остановленном Flight и OPEN |
| Неполный HTTP body | E2E REST strict JSON and input validation: реальное TCP-соединение; 400/408 или закрытие до клиентского deadline |
| Неизвестный исход COMMIT | TestPostgresCreateLostCommitAcknowledgment, TestPostgresReserveLostCommitAcknowledgment: CONFIRMED без Release, единственный резерв, одно списание и увеличение revision |
| Задержка кеша | TestPostgresCacheOutageFitsRPCDeadline: при остановленном/зависшем Sentinel или зависшем Redis Get/Search успевают перейти на PostgreSQL |
| Деньги и входные данные | TestCheckedPrice; E2E REST strict JSON and input validation: точное умножение копеек, переполнение отклонено до резерва |
| Поиск | TestPostgresReservationLifecycle; E2E Search uses UTC dates and scheduled status: день UTC, только SCHEDULED, стабильный порядок |

COMMIT-fault в internal/testdb/commitfault.go возвращает driver.ErrBadConn после настоящего COMMIT; реальный обрыв TCP на этой границе не воспроизводится. Booking-тесты используют реальную PostgreSQL и подменённый Flight-клиент. Flight-тесты — PostgreSQL и Redis.

## Запуск

Для проверки на хосте нужны Go 1.26 и protoc 36.2; Makefile устанавливает генераторы закреплённых версий.

```bash
make check        # генерация без drift, race detector, vet
make integration  # PostgreSQL и Redis в Docker
```

Без переменных подключения интеграционные тесты пропускаются. Docker integration передаёт адреса обеих БД и трёх Sentinel. FLIGHT_TEST_REDIS_SENTINEL_ADDRS и FLIGHT_TEST_REDIS_MASTER_NAME выбирают текущий master, в том числе после promotion; запуск ждёт готовности Sentinel. Для запуска на хосте доступен FLIGHT_TEST_REDIS_URL.

E2E требует Docker socket и выполняет stop/start/kill. Выделите отдельный проект и свободные порты; во время E2E не работайте с этим окружением вручную:

```bash
export COMPOSE_PROJECT_NAME=soa-flight-booking-qa
export BOOKING_PORT=18081
export FLIGHT_PORT=15051
make up
make e2e
```

TestFlightBooking последовательно выполняет 15 сценариев: десять функциональных, Redis outage, Sentinel failover, Flight outage и два падения Booking. Fixtures получают новые UUID; cleanup удаляет записи сценария и восстанавливает контейнеры. Контейнеры выбираются по меткам проекта. После аварийной остановки раннера проверьте docker compose ps.

Перед kill тест дожидается нужного состояния БД, после старта — сходимости. SQL проверяет остаток, число booking, резерв и журнал; таймаут ожидания завершает тест ошибкой.

## Allure-отчёт

Node.js 24+ нужен только для отчёта:

```bash
npm ci
npm test          # 23 проверки скриптов отчёта
make e2e-report
make e2e-open
```

Результаты каждого запуска — в artifacts/e2e/<RUNID>/allure-results, HTML — рядом в allure-report. Указатель latest-run выбирает последний запуск. Вложения содержат HTTP, SQL, действия Docker, ожидания и очищенные логи; API key маскируется.

scripts/run-e2e.sh копирует /artifacts через docker cp и при падении тестов, сохраняя их код выхода. Затем удаляет только свой контейнер по проверенной метке. Это работает с удалённым Docker daemon без bind mount артефактов. Workflow .github/workflows/tests.yml выполняет проверки и E2E, загружает артефакты и очищает свой Compose-проект.
