#!/usr/bin/env python
import argparse
from math import sqrt
from itertools import count, islice

# Python program to check if the input number is prime or not

def is_prime(n):

	return n > 1 and all(n%i for i in islice(count(2), int(sqrt(n)-1)))

if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument("number", help="display whether a number is prime or not",
		type=int)
	args = parser.parse_args()

	if is_prime(args.number):
		print('The number: {} is prime'.format(args.number))
	else:
		print('The number: {} is not prime'.format(args.number))
