# Демо Flight Booking

Демонстрация на 15–20 минут. Заранее выполните make up и make e2e в отдельном Compose-проекте, затем сформируйте Allure-отчёт по [инструкции QA](qa.md). Команды демонстрации выполняйте в терминале с теми же экспортированными COMPOSE_PROJECT_NAME, BOOKING_PORT и FLIGHT_PORT.

## 0–3 минуты: границы сервисов

Откройте [архитектуру](architecture.md), C4, ER и compose.yaml. Сопоставьте ДЗ-1 с отдельными сервисами и ДЗ-2 с общей PostgreSQL: Booking владеет покупкой и её ценой, Flight — предложением и местами. Теперь два процесса и две БД; вместо общей транзакции — локальные транзакции и долговечный журнал намерений.

## 3–7 минут: бронирование и отмена

Выполните [REST-пример](../README.md). На исходных данных два места стоят 300100 копеек: остаток меняется со 100 на 98 и возвращается к 100. Повторите отмену и создание с прежним Idempotency-Key; изменённый payload с тем же ключом должен дать 409.

Покажите proto/flight/v1/flight.proto, сгенерированный клиент и вызовы GetFlight, ReserveSeats, ReleaseReservation. В БД проверьте обе стороны:

```bash
docker compose exec booking-db psql -U booking -d booking -c \
  'SELECT id,seat_count,total_price,status FROM bookings ORDER BY created_at DESC LIMIT 5'
docker compose exec booking-db psql -U booking -d booking -c \
  'SELECT booking_id,kind,state,error_code FROM booking_operations ORDER BY created_at DESC LIMIT 10'
docker compose exec flight-db psql -U flight -d flight -c \
  'SELECT flight_number,available_seats,total_seats,price,status FROM flights'
docker compose exec flight-db psql -U flight -d flight -c \
  'SELECT booking_id,seat_count,status FROM seat_reservations LIMIT 10'
```

## 7–10 минут: конкуренция, цена, кеш

В Allure откройте Last seat has exactly one concurrent winner: 12 параллельных вызовов, один победитель, один ACTIVE, нулевой остаток. Покажите SELECT FOR UPDATE и транзакцию изменения остатка вместе с резервом.

В Price snapshot and cache invalidation покажите сохранённую цену брони после UpdateFlight, свежие Get/Search и исключение CANCELLED-рейса из поиска. По логам и TTL проверьте кеш. Объясните компромисс: даже cache hit требует чтения revision из PostgreSQL, а общая revision инвалидирует кеш всех рейсов. Взамен старый payload после мутации остаётся только под старым ключом.

## 10–15 минут: сбои

Fault-сценарии останавливают контейнеры и могут занять больше времени, чем демонстрация; их удобно разобрать по подготовленному отчёту:

| Сценарий Allure | Что показать |
|---|---|
| Redis outage falls back to PostgreSQL | Поиск и бронирование без Redis |
| Sentinel promotes replica and cache recovers | Новый master и заполнение кеша приложением |
| Flight outage opens and recovers circuit | Быстрый 503 в OPEN, чтение своих броней, переход через HALF_OPEN после восстановления |
| Booking crash after reserve is compensated | После рестарта CREATE FAILED, booking отсутствует, резерв RELEASED, места возвращены |
| Booking crash after release finishes cancellation | После рестарта CANCEL DONE, booking CANCELLED, места возвращены один раз |

Свяжите восстановление с диаграммой создания: CREATE компенсируется, CANCEL завершается вперёд. Tombstone запрещает поздний Reserve. Компенсация может требовать повторов; сходимость зависит от восстановления сервисов, БД и сохранности данных. До неё возможен CONFIRMED при уже освобождённом резерве.

## 15–20 минут: вопросы

| Вопрос | Ответ |
|---|---|
| Почему gRPC и отдельные БД? | Требование задания; типизированный контракт и раздельное владение данными. |
| Почему нет FK между сервисами? | Ссылки пересекают БД; резерв может существовать до booking. |
| Где 3NF? | Авиакомпания вынесена из рейса, резерв зависит от booking_id; IATA атомарен, справочных имён аэропортов нет. |
| Почему int64? | Копейки RUB умножаются на целое число мест с проверкой переполнения. |
| Какая цена при одновременном UpdateFlight? | Снимок GetFlight перед ReserveSeats. |
| Что делать при UNKNOWN_OUTCOME? | Повторить с прежним ключом: журнал отличает завершённую операцию от требующей восстановления. |
| Можно удалить tombstone по TTL? | Только после определения допустимого срока повторов; сейчас TTL нет. |
| Что значит EXPIRED? | Статус из контракта, автоматическое истечение не реализовано: срок удержания не задан. |
| Что остаётся при отказе PostgreSQL? | Кеш требует revision из БД. PostgreSQL и приложения не реплицированы; Sentinel покрывает Redis. |
| Защищает ли user_id чужие брони? | Нет, REST не аутентифицирует пользователя. |

Параметры retry, бюджеты, семантика breaker и граница COMMIT-тестов собраны в архитектуре. Матрица QA связывает показанные сценарии с требованиями задания.
