--Entire table
SELECT *
FROM employee;

--Highest salary
SELECT salary--, first_name, surname 
FROM employee
ORDER BY salary DESC
LIMIT 1;

--All with highest salary
SELECT first_name, surname--, salary
FROM employee
WHERE salary = (
    SELECT max(salary) 
    FROM employee
);
    
--All with lowest salary
SELECT first_name, surname--, salary
FROM employee
WHERE salary = (
    SELECT min(salary)
    FROM employee
);

--Top 10 earners
SELECT first_name, surname, salary
FROM employee
ORDER BY salary DESC
LIMIT 10;

-- Average salary of top 10 earners
SELECT avg(s)
FROM (
    SELECT salary as s
    FROM employee
    ORDER BY salary DESC
    LIMIT 10
);

--How many men over 50 at the company?
SELECT COUNT(id)
FROM employee
WHERE mf == 'M'
AND date_birth < '1966-05-05';

--How many women over 50 at the company?
SELECT COUNT(id)
FROM employee
WHERE mf == 'F'
AND date_birth < '1966-05-05';

--Date of employment of least well paid woman/women whose first name 
--contains an 'k' or surname contains a 'c'
SELECT date_started
FROM employee e
WHERE e.mf == 'F'
AND (
    instr(lower(e.first_name), 'k')
    OR
    instr(lower(e.surname), 'c')
)
AND salary = (
    SELECT min(salary)
    FROM employee e2
    WHERE e2.mf == 'F'
    AND (
        instr(lower(e2.first_name), 'k')
        OR
        instr(lower(e2.surname), 'c')
    )
);
