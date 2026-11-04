INSERT INTO airlines (code, name) VALUES ('SU', 'Demo Airlines');
INSERT INTO flights (id, airline_id, flight_number, origin, destination, departure_time, arrival_time, total_seats, available_seats, price, status) VALUES
('11111111-1111-4111-8111-111111111111', 1, 'SU1234', 'SVO', 'LED', '2030-04-01 09:00:00+00', '2030-04-01 10:30:00+00', 100, 100, 150050, 'SCHEDULED'),
('22222222-2222-4222-8222-222222222222', 1, 'SU1236', 'SVO', 'LED', '2030-04-02 12:00:00+00', '2030-04-02 13:30:00+00', 50, 50, 220000, 'SCHEDULED');
