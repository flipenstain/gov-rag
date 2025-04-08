
CREATE OR REPLACE TEMP TABLE temp_propect_marketingnameplate AS 
SELECT
    *,
    CASE 
        WHEN LENGTH(
            CONCAT(
                CASE WHEN networth > 1000000 OR income > 200000 THEN 'HighValue+' ELSE '' END,
                CASE WHEN numberchildren > 3 OR numbercreditcards > 5 THEN 'Expenses+' ELSE '' END,
                CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END,
                CASE WHEN income < 50000 OR creditrating < 600 OR networth < 100000 THEN 'MoneyAlert+' ELSE '' END,
                CASE WHEN numbercars > 3 OR numbercreditcards > 7 THEN 'Spender+' ELSE '' END,
                CASE WHEN age < 25 AND networth > 1000000 THEN 'Inherited+' ELSE '' END
            )
        ) > 0 
        THEN LEFT(
            CONCAT(
                CASE WHEN networth > 1000000 OR income > 200000 THEN 'HighValue+' ELSE '' END,
                CASE WHEN numberchildren > 3 OR numbercreditcards > 5 THEN 'Expenses+' ELSE '' END,
                CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END,
                CASE WHEN income < 50000 OR creditrating < 600 OR networth < 100000 THEN 'MoneyAlert+' ELSE '' END,
                CASE WHEN numbercars > 3 OR numbercreditcards > 7 THEN 'Spender+' ELSE '' END,
                CASE WHEN age < 25 AND networth > 1000000 THEN 'Inherited+' ELSE '' END
            ),
            LENGTH(
                CONCAT(
                    CASE WHEN networth > 1000000 OR income > 200000 THEN 'HighValue+' ELSE '' END,
                    CASE WHEN numberchildren > 3 OR numbercreditcards > 5 THEN 'Expenses+' ELSE '' END,
                    CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END,
                    CASE WHEN income < 50000 OR creditrating < 600 OR networth < 100000 THEN 'MoneyAlert+' ELSE '' END,
                    CASE WHEN numbercars > 3 OR numbercreditcards > 7 THEN 'Spender+' ELSE '' END,
                    CASE WHEN age < 25 AND networth > 1000000 THEN 'Inherited+' ELSE '' END
                )
            ) - 1
        )
        ELSE NULL 
    END AS marketingnameplate
FROM temp_propect;
