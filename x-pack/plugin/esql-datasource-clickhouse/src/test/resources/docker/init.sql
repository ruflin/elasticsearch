-- Initialize test data for ClickHouse integration tests
-- This script runs when the ClickHouse container starts

CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.employees
(
    emp_no      Int32,
    first_name  String,
    last_name   String,
    salary      Int32,
    still_hired Bool,
    height      Float64
) ENGINE = MergeTree()
ORDER BY emp_no;

INSERT INTO test.employees (emp_no, first_name, last_name, salary, still_hired, height) VALUES
    (10001, 'Georgi', 'Facello', 60117, true, 1.78),
    (10002, 'Bezalel', 'Simmel', 65828, false, 1.72),
    (10003, 'Parto', 'Bamford', 40006, true, 1.65),
    (10004, 'Chirstian', 'Koblick', 40054, true, 1.81),
    (10005, 'Kyoichi', 'Maliniak', 78228, true, 1.74),
    (10006, 'Anneke', 'Preusig', 40000, true, 1.68),
    (10007, 'Tzvetan', 'Zielinski', 56724, false, 1.70),
    (10008, 'Saniya', 'Kalloufi', 46671, false, 1.62),
    (10009, 'Sumant', 'Peac', 60929, false, 1.77),
    (10010, 'Duangkaew', 'Piveteau', 72527, false, 1.59);
