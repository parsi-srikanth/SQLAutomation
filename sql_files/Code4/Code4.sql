
SELECT MAX(amount) FROM {{tableName}} where transaction_id  in {{ids}}
