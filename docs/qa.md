# Проверка Flight Booking

Приёмка проходит через REST и gRPC с проверкой обеих PostgreSQL, затем через остановку и перезапуск контейнеров. HTTP 200 без проверки остатков, журнала и резерваций не доказывает завершение бронирования. Матрица ниже описывает зарегистрированные проверки. Оценку выставляет преподаватель после приёмки.

## Десять пунктов задания

| № | Реализация | Проверка в исходниках | Что должно быть подтверждено |
|---|---|---|---|
| 1. gRPC-контракт | proto/flight/v1/flight.proto, gen/flight/v1 | `make check`; `TestInvalidRequests`; E2E `gRPC validation and not-found errors`, `All five gRPC methods require authentication` | Пять методов, Timestamp, enum, кодогенерация и коды ошибок |
| 2. ER и 3NF | docs/architecture.md, db/flight/V1__flight.sql, db/booking/V1__booking.sql | `TestPostgresReservationLifecycle`; E2E `Last seat has exactly one concurrent winner` | Ограничения схемы, один резерв на booking_id, остаток не ниже нуля; 3NF сверяется по схеме |
| 3. Два сервиса и БД | cmd/booking-service, cmd/flight-service, compose.yaml, db | E2E `REST lifecycle and user isolation`; `TestPostgresCreateAndReplay` | Flyway, REST, gRPC и независимые БД работают вместе |
| 4. gRPC при создании и отмене | internal/booking/server.go | `TestPostgresCreateAndReplay`; E2E `REST lifecycle and user isolation` | GetFlight → ReserveSeats → CONFIRMED; ReleaseReservation при отмене |
| 5. Транзакции | internal/flight/server.go, booking_operations | `TestPostgresReservationLifecycle` / `reservation write failure rolls back seats and revision`; `TestPostgresFailedUpstreamNeverCreatesBooking`; E2E `Last seat has exactly one concurrent winner` | Один победитель на последнее место, rollback локальных изменений, отсутствие booking при неудаче upstream |
| 6. Аутентификация | internal/transport/transport.go, FLIGHT_API_KEY в Compose | `TestServerAuth`; E2E `All five gRPC methods require authentication` | Все пять методов возвращают UNAUTHENTICATED без ключа и с неверным ключом |
| 7. Redis | cache_revision, internal/flight/server.go | `TestPostgresRedisRevisionCache`; E2E `Price snapshot and cache invalidation`, `Redis outage falls back to PostgreSQL` | TTL 300s, hit/miss, новые версии после трёх мутаций, fallback на БД |
| 8. Retry | internal/transport/transport.go | `TestRetryMetadataAndBudget`, `TestRetryOnlyInfrastructureErrors`, `TestAttemptLimitCannotExceedThree`; E2E `Reservation and release are idempotent` | Не более трёх вызовов invoker на логический RPC, backoff приложения только для инфраструктурных ошибок, отсутствие двойного резерва |
| 9. Sentinel | compose.yaml, redis/start.sh, cmd/flight-service/main.go | E2E `Sentinel promotes replica and cache recovers` | Promotion реплики и запись кеша приложением на новом master |
| 10. Circuit breaker | NewClientInterceptor | `TestHalfOpenSingleWireProbeAndRecovery`, `TestStaleCompletionCannotChangeNewGeneration`, `TestLogsExposeTransitionsWithoutCredentials`; E2E `Flight outage opens and recovers circuit` | CLOSED → OPEN → HALF_OPEN → CLOSED, один логический probe, быстрый 503, переходы в логах |

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
| Позднее заполнение после мутации возвращает старую цену | `TestPostgresRedisLateFill` / `GetFlight`, `SearchFlights` | Реальная PostgreSQL, барьер завершения чтения и Redis SET hook: старый payload физически записан после мутаций только под старым ключом; новое чтение свежее |
| Flight недоступен, но свои брони нужны клиенту | E2E `Flight outage opens and recovers circuit` | При остановленном Flight и OPEN работают GET booking и список; статус и снимок цены сохранены |
| Неполное тело HTTP удерживает обработчик | E2E `REST strict JSON and input validation`, helper slowPartialBody | Реальное TCP-соединение с недописанным телом; сервер возвращает 400/408 или закрывает соединение до клиентского deadline |
| Потеря подтверждения COMMIT после успешной записи | `TestPostgresCreateLostCommitAcknowledgment`, `TestPostgresReserveLostCommitAcknowledgment` | Настоящая PostgreSQL; тестовый драйвер возвращает ErrBadConn после COMMIT. Booking повторяет CONFIRMED без Release, Flight не повторяет списание и revision |
| Недоступный или зависший кеш расходует RPC deadline | `TestPostgresCacheOutageFitsRPCDeadline` | Реальная PostgreSQL, остановленный/зависший Sentinel или зависший Redis; Get/Search завершают fallback в пределах бюджета |
| Ошибка или переполнение денежного расчёта | `TestCheckedPrice`; E2E `REST strict JSON and input validation` | Точное умножение копеек, отказ до резерва при переполнении |
| Неверная дата или статус в поиске | `TestPostgresReservationLifecycle` / `UTC date filtering and stable search order`; E2E `Search uses UTC dates and scheduled status` | Граница дня UTC, только SCHEDULED, стабильный порядок |

Потеря подтверждения COMMIT проверяется контролируемым fault injection в internal/testdb/commitfault.go: реальная PostgreSQL успешно коммитит, затем обёртка драйвера возвращает driver.ErrBadConn. Это доказывает поведение приложения при неизвестном исходе после фактической записи, включая отсутствие компенсации подтверждённого booking и повторного списания мест. Обрыв реального TCP-соединения на ответе COMMIT отдельно не воспроизводился. Booking-тест использует подменённый Flight-клиент, поэтому его результат не заменяет сетевой E2E.

Контрольный Go-overlay mutant, который выбирает новую cache revision перед SET старого результата, провалил оба подслучая TestPostgresRedisLateFill. Это проверка чувствительности теста к поздней записи; массовый mutation score не измерялся.

В закреплённой gRPC 1.85.0 WithDisableRetry сохраняет transparent retry для запросов, ещё не обработанных сервером. Транспортные тесты ограничивают вызовы invoker в interceptor и число логических HALF_OPEN probes. Они не доказывают абсолютный предел сетевых передач.

## Как запускать

Для native-проверок нужны Go 1.26 и protoc 36.2. Генераторы устанавливаются закреплёнными версиями через Makefile. `make check` проверяет воспроизводимость сгенерированных файлов, запускает race detector и vet. Интеграционные тесты без переменных подключения пропускаются: обычный go test не доказывает PostgreSQL/Redis-поведение.

```bash
make check
make integration
```

Integration выполняется в Docker и получает адреса обеих PostgreSQL и трёх Sentinel из Compose. FLIGHT_TEST_REDIS_SENTINEL_ADDRS и FLIGHT_TEST_REDIS_MASTER_NAME выбирают текущий master; запуск ждёт готовности трёх Sentinel и продолжает работать после promotion реплики. Для native-проверок оставлен fallback FLIGHT_TEST_REDIS_URL. Flight-тесты используют реальную PostgreSQL и Redis; Booking-тесты используют реальную PostgreSQL и подменяют Flight-клиент для управляемых ошибок.

E2E требует Docker socket: сценарии выполняют stop/start/kill контейнеров. Используйте отдельный Compose-проект и свободные порты, без параллельной ручной работы в этом окружении:

```bash
export COMPOSE_PROJECT_NAME=soa-flight-booking-qa
export BOOKING_PORT=18081
export FLIGHT_PORT=15051
# На машине автора также: export DOCKER_CONTEXT=colima-distsys
make up
make e2e
```

E2E запускается последовательно. В TestFlightBooking зарегистрированы 15 сценариев: десять функциональных, затем Redis outage, Sentinel failover, Flight outage и два падения Booking. Fixtures создаются с новыми UUID; cleanup удаляет только записи сценария. Имена сервисов Docker ищутся по меткам текущего Compose-проекта. Запущенные после сбоя контейнеры восстанавливаются cleanup-обработчиками, но при аварийной остановке самого тестового раннера состояние стоит проверить через docker compose ps.

Проверки SQL подтверждают остаток, число booking, статус резерва и состояние журнала. Для fault-сценариев тест сначала дожидается нужного состояния БД, затем убивает Booking и после рестарта ждёт сходимости. Таймаут ожидания завершает сценарий ошибкой.

## Артефакты и фактический результат

```bash
npm ci   # Node.js 24 или новее
npm test
make e2e-report
make e2e-open
```

Каждый запуск создаёт каталог artifacts/e2e/<RUNID>/allure-results. Allure содержит HTTP-запросы и ответы, SQL-наблюдения, действия с Docker, результаты ожиданий и очищенные логи сервисов. Ключ сервиса маскируется во вложениях. Скрипт scripts/run-e2e.sh запускает именованный контейнер, после завершения выполняет docker cp и удаляет только этот контейнер по проверенной метке. Копирование выполняется также при ошибке тестов и работает через локальный или удалённый Docker daemon. Bind mount артефактов с хоста не требуется; исходный ненулевой код тестов сохраняется. Команды отчёта читают указатель artifacts/e2e/latest-run и пишут HTML в allure-report рядом с результатами. Отчёт после проваленного прогона полезен для диагностики и не делает прогон успешным.

Проверено 4 ноября 2026 года на финальном коде. Основная сводка сохранена в artifacts/verification/acceptance.json; логи и HTML — локальные результаты запуска, они не нужны для сборки репозитория.

| Уровень | Фактический результат |
|---|---|
| Native unit / race | PASS; финальный полный прогон, включая интеграционные сценарии против PostgreSQL 18 |
| Полный make check | PASS: кодогенерация без drift, Go race tests и vet; make-check-final.log |
| Compose integration / race | PASS на PostgreSQL 17.11 и Redis 7.4.12 через Sentinel, включая lost ACK и LateFill; integration-final.log |
| Integration после failover | PASS при master на redis-replica, без сброса ролей после предыдущего promotion |
| Полный E2E | RUNID F422184A70: 15/15 PASS, 71,679s; e2e-acceptance.log |
| Allure HTML | CLI exit 0; artifacts/e2e/F422184A70/allure-report/index.html существует |
| npm test | 23/23 PASS, без skipped; report-tests-final.log |
| Удалённый CI | Не запускался |

Логи из таблицы находятся в artifacts/verification. Для отчёта использовался Node.js 26.10; поддерживаемый минимум — 24. Все 15 result JSON финального Allure имеют статус passed. Среди фактических наблюдений: 12 конкурентов дали один ACTIVE-резерв; OPEN вернул ответ за 1ms; неполное тело запроса отклонено сервером с 400 через 8008ms. После падения CREATE журнал стал FAILED, booking отсутствовал, резерв RELEASED; после падения CANCEL журнал стал DONE, booking CANCELLED, резерв RELEASED. В обоих сценариях четыре исходных места восстановлены. Чтение booking и списка при остановленном Flight и OPEN также прошло.

В .github/workflows/tests.yml описаны отдельные этапы генерации, Go-проверок, integration, запуска Compose, E2E и HTML-отчёта. Workflow собирает диагностику и загружает artifacts/e2e; очищает только свой проект с отдельным именем. Наличие workflow не подтверждает его удалённое выполнение. Оценка «10/10» требует успешной приёмки всех блоков по порядку. Одного unit PASS недостаточно.
