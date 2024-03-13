SELECT MAX(amount) AS Max_amount 
FROM {{tableName}} 
WHERE amount <> ({% include 'Code4.sql' %})