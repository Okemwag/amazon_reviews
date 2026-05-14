# Data Dictionary

## Raw Reviews

| Column | Type | Description |
| --- | --- | --- |
| `rating` | double | Review score, expected range 1 to 5. |
| `title` | string | Review title. |
| `text` | string | Review body text. |
| `asin` | string | Product identifier. |
| `parent_asin` | string | Parent product identifier. |
| `user_id` | string | Reviewer identifier. |
| `timestamp` | bigint | Review timestamp in epoch milliseconds. |
| `helpful_vote` | int | Count of helpful votes. |
| `verified_purchase` | boolean | Whether review came from a verified purchase. |
| `category` | string | Product category. |

## Silver Reviews

Adds cleaned text and engineered fields:

| Column | Description |
| --- | --- |
| `review_ts` | Renamed raw timestamp. |
| `sentiment_label` | `1` positive, `0` negative, `2` neutral. |
| `rating_class` | `positive`, `negative`, or `neutral`. |
| `review_length` | Character count of cleaned text. |
| `word_count` | Word count of cleaned text. |
| `helpful_vote_bucket` | `none`, `low`, `medium`, or `high`. |
| `verified_purchase_int` | Numeric verified-purchase flag. |
| `review_date` | Calendar date. |
| `review_year` | Partition year. |
| `review_month` | Partition month. |
