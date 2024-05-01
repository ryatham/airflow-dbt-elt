

WITH src_reviews AS (
    SELECT * FROM AIRBNB.DEV.src_reviews
)

SELECT * FROM src_reviews
WHERE review_text is not null


  and review_date >= (select max(review_date) from AIRBNB.DEV.fct_reviews)
