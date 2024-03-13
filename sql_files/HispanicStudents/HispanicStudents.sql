{% from 'macros/include.sql' import include_path as add -%}

SELECT MAX(amount) AS Max_amount 
FROM {{tableName}} 
WHERE amount <> ({{add('Code4/Code4.sql',ids= ids)}})