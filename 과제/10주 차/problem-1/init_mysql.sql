CREATE DATABASE IF NOT EXISTS manufacturing;
USE manufacturing;
CREATE TABLE IF NOT EXISTS equipment_master (
    equipment_id INT PRIMARY KEY,
    equipment_name VARCHAR(50) NOT NULL,
    line_no INT NOT NULL,
    process_type VARCHAR(50) NOT NULL,
    install_date DATE NOT NULL
);
TRUNCATE TABLE equipment_master;
INSERT INTO equipment_master VALUES (1, 'EQ-001', 1, 'CNC가공', '2020-01-18');
INSERT INTO equipment_master VALUES (2, 'EQ-002', 2, '조립', '2020-02-04');
INSERT INTO equipment_master VALUES (3, 'EQ-003', 3, '도장', '2020-02-21');
INSERT INTO equipment_master VALUES (4, 'EQ-004', 4, '검사', '2020-03-09');
INSERT INTO equipment_master VALUES (5, 'EQ-005', 5, '포장', '2020-03-26');
INSERT INTO equipment_master VALUES (6, 'EQ-006', 1, 'CNC가공', '2020-04-12');
INSERT INTO equipment_master VALUES (7, 'EQ-007', 2, '조립', '2020-04-29');
INSERT INTO equipment_master VALUES (8, 'EQ-008', 3, '도장', '2020-05-16');
INSERT INTO equipment_master VALUES (9, 'EQ-009', 4, '검사', '2020-06-02');
INSERT INTO equipment_master VALUES (10, 'EQ-010', 5, '포장', '2020-06-19');
INSERT INTO equipment_master VALUES (11, 'EQ-011', 1, 'CNC가공', '2020-07-06');
INSERT INTO equipment_master VALUES (12, 'EQ-012', 2, '조립', '2020-07-23');
INSERT INTO equipment_master VALUES (13, 'EQ-013', 3, '도장', '2020-08-09');
INSERT INTO equipment_master VALUES (14, 'EQ-014', 4, '검사', '2020-08-26');
INSERT INTO equipment_master VALUES (15, 'EQ-015', 5, '포장', '2020-09-12');
INSERT INTO equipment_master VALUES (16, 'EQ-016', 1, 'CNC가공', '2020-09-29');
INSERT INTO equipment_master VALUES (17, 'EQ-017', 2, '조립', '2020-10-16');
INSERT INTO equipment_master VALUES (18, 'EQ-018', 3, '도장', '2020-11-02');
INSERT INTO equipment_master VALUES (19, 'EQ-019', 4, '검사', '2020-11-19');
INSERT INTO equipment_master VALUES (20, 'EQ-020', 5, '포장', '2020-12-06');
INSERT INTO equipment_master VALUES (21, 'EQ-021', 1, 'CNC가공', '2020-12-23');
INSERT INTO equipment_master VALUES (22, 'EQ-022', 2, '조립', '2021-01-09');
INSERT INTO equipment_master VALUES (23, 'EQ-023', 3, '도장', '2021-01-26');
INSERT INTO equipment_master VALUES (24, 'EQ-024', 4, '검사', '2021-02-12');
INSERT INTO equipment_master VALUES (25, 'EQ-025', 5, '포장', '2021-03-01');
INSERT INTO equipment_master VALUES (26, 'EQ-026', 1, 'CNC가공', '2021-03-18');
INSERT INTO equipment_master VALUES (27, 'EQ-027', 2, '조립', '2021-04-04');
INSERT INTO equipment_master VALUES (28, 'EQ-028', 3, '도장', '2021-04-21');
INSERT INTO equipment_master VALUES (29, 'EQ-029', 4, '검사', '2021-05-08');
INSERT INTO equipment_master VALUES (30, 'EQ-030', 5, '포장', '2021-05-25');
INSERT INTO equipment_master VALUES (31, 'EQ-031', 1, 'CNC가공', '2021-06-11');
INSERT INTO equipment_master VALUES (32, 'EQ-032', 2, '조립', '2021-06-28');
INSERT INTO equipment_master VALUES (33, 'EQ-033', 3, '도장', '2021-07-15');
INSERT INTO equipment_master VALUES (34, 'EQ-034', 4, '검사', '2021-08-01');
INSERT INTO equipment_master VALUES (35, 'EQ-035', 5, '포장', '2021-08-18');
INSERT INTO equipment_master VALUES (36, 'EQ-036', 1, 'CNC가공', '2021-09-04');
INSERT INTO equipment_master VALUES (37, 'EQ-037', 2, '조립', '2021-09-21');
INSERT INTO equipment_master VALUES (38, 'EQ-038', 3, '도장', '2021-10-08');
INSERT INTO equipment_master VALUES (39, 'EQ-039', 4, '검사', '2021-10-25');
INSERT INTO equipment_master VALUES (40, 'EQ-040', 5, '포장', '2021-11-11');
INSERT INTO equipment_master VALUES (41, 'EQ-041', 1, 'CNC가공', '2021-11-28');
INSERT INTO equipment_master VALUES (42, 'EQ-042', 2, '조립', '2021-12-15');
INSERT INTO equipment_master VALUES (43, 'EQ-043', 3, '도장', '2022-01-01');
INSERT INTO equipment_master VALUES (44, 'EQ-044', 4, '검사', '2022-01-18');
INSERT INTO equipment_master VALUES (45, 'EQ-045', 5, '포장', '2022-02-04');
INSERT INTO equipment_master VALUES (46, 'EQ-046', 1, 'CNC가공', '2022-02-21');
INSERT INTO equipment_master VALUES (47, 'EQ-047', 2, '조립', '2022-03-10');
INSERT INTO equipment_master VALUES (48, 'EQ-048', 3, '도장', '2022-03-27');