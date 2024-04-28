with raw_reviews as (
    select * from airbnb.raw.raw_reviews
)

select
    LISTING_ID,
    DATE as REVIEW_DATE,
    REVIEWER_NAME,
    COMMENTS AS REVIEW_TEXT,
    SENTIMENT AS REVIEW_SENTIMENT
from raw_reviews