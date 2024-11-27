
# Data Engineer Tests

- Require coding in **Python 3** (using type hinting is a plus).
- Create your test repo on github and send the repo url to us.
- Complete it as much as you can, coding should be clean easy to read


## Algorithm tests
### Question for test_1.py 

Create ‘accum’ function and return value

Examples:

    accum("abcd") -> "A-Bb-Ccc-Dddd"

    accum("RqaEzty") -> "R-Qq-Aaa-Eeee-Zzzzz-Tttttt-Yyyyyyy"

    accum("cwAt") -> "C-Ww-Aaa-Tttt"

The parameter of accum is a string which includes only letters from a-z and A-Z.


### Question for test_2.py 

Write function for reverse string which parsed in function without using list built-in method

Examples:

    reverse_string("ABCD1234") -> "4321DCBA


### Question for test_3.py 

Write function for reverse number (integer) which parsed in function without convert to string or using list built-in method

Input will always be an integer which greater than 0

Examples:

    reverse_num(1234) -> 4321   # return int
    reverse_num(1) -> 1   # return int
    reverse_num(4444) -> 4444   # return int
    reverse_num(0) -> 0   # return int



### Question for test_4.py 

You will be given an array of strings. The words in the array should mesh together where one or more letters at the end of one word will have the same letters (in the same order) as the next word in the array. But, there are times when all the words won't mesh.

#### Examples of meshed words:

    "apply" and "plywood" (word "ply")

    "apple" and "each"  (word "e")

    "behemoth" and "mother" (word "moth")

#### Examples of words that do not mesh:

    "apply" and "playground"

    "apple" and "peggy"

    "behemoth" and "mathematics"

If all the words in the given array mesh together, then your code should return the meshed letters in a string. 
- You should return the longest common substring. 
- You won't know how many letters the meshed words have in common, but it will be at least one.

If any pair of consecutive words fails to mesh, then return `"failed to mesh"`.

**Input:** An array of strings. There will always be at least two words in the input array.

**Output:** Either a string of letters that mesh the words together or the string "failed to mesh".

#### Example 1:

    ["allow", "lowering", "ringmaster", "terror"] -> "lowringter"

because:
- the letters `"low"` in the first two words mesh together
- the letters `"ring"` in the second and third word mesh together
- the letters `"ter"` in the third and fourth words mesh together.

#### Example 2:
    
    ["kingdom", "dominator", "notorious", "usual", "allegory"] -> "failed to mesh"

Although the words `"dominator"` and `"notorious"` share letters in the same order, the last letters of the first word don't mesh with the first letters of the second word.


## Data Engineer Skill Tests

### Question for data_analysis_test.ipynb

Use `pandas` lib for read, extract and analysis data 
from `./ref/crop_data.csv` file 

There're 5 questions try to complete it as much as you can.


### Question for data_cleansing_and_transformation_test.ipynb

Use `pandas` lib or other lib if you want for cleansing data 
from `./ref/sample_user_data.csv`

There're 3 questions try to complete it as much as you can

---

### *** Setup database ***

Initialize database using Dockerfile or docker-compose file, 
you can choose any database you want e.g. PostgreSQL, MySQL, etc.

_Write some explanation such as which port that you use to connect or expose your database. <br>
Keep it simple, don't take too much time to do this step, we just make sure you understand docker technology._

### Extract User Info

We provide sample user info in `./ref/user_info.csv`.

#### What you have to do:
1. Write SQL statement to create `user_info` table
2. Implement an ELT process that:
    - Extract data from `user_info.csv`
    - Save extracted data in database


---


### Question for Incremental Data Load 

We provide sample 7 days sale transactions in `./ref/sales` folder. Place your result in `incremental_data_load_test` folder

#### What you have to do:
1.  Create `transaction` table with SQL statement for save raw data 
    - Fields: 
        - order_id (string)
        - user_id (string)
        - product_id (string)
        - quantity (number)
        - amount (number)
        - order_date (date)

2.  Implement an ELT process that:
    - Extracts data from csv files in `./ref/sales` folder.
    - Loads the daily transaction into a transaction table (without re-processing old files.)

_Tips you can track old transaction from order id_ 


#### Expectation:
1.  SQL statement file for create database table with given fields
2.  Python script for ELT process


### Question for Complex Data Transformation with Aggregation 

From last question you will have transaction table, it'll be used in this question, Place your result in `complex_data_transformation_test` folder

#### What you have to do:
1.  Create `user_transaction_amount`, `daily_transaction_amount` and `product_sales` tables with SQL statement for save calculated data

2.  Implement an ETL process that:
    - Extract: raw data from `transaction` table.
    - Transform: Group and aggregate the data to calculate:
        - Total and average sales (quantity x amount) by user.
        - Total, min, max, average sales and VAT(sales x 0.07) by day.
        - Number of transactions and Total sales per product.
    - Load: Save each aggregation as a separate table in database.

    You can decide field and type for created table

3.  Implement an ETL process that:
    - Extract: raw data from `user_info`, `transaction` or other table.
    - Transform: Group and aggregate the data to calculate:
        - Total, min, max, average transaction amount by user location and gender
        - Top 20 best selle in Chiangmai by day (require field product_id and sales (quantity x amount) )


#### Expectation:
1.  SQL statement file for create database tables
2.  Python script for ETL process





