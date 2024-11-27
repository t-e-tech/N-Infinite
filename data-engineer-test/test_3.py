def reverse_num(num: int):
    reversed_number = 0
    while num > 0:
        digit = num % 10  # Extract the last digit
        reversed_number = reversed_number * 10 + digit  # Append the digit to the reversed number
        num = num // 10  # Remove the last digit from the number
    return reversed_number

# Run this file for test
assert reverse_num(1234) == 4321
assert reverse_num(20903) == 30902
assert reverse_num(1_000_234) == 4320001
assert reverse_num(4444) == 4444
assert reverse_num(1) == 1
assert reverse_num(0) == 0