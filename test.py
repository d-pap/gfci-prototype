import itertools        
import re

# JOBS = ['data analyst']
# CITIES = ['Chicago, IL', 'Detroit, MI']

# combinations = list(itertools.product(JOBS, CITIES))
# print(combinations)

desc = "JOB ROLE: Data Analyst\n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\nJOB SUMMARY: \n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\nRESPONSIBILITIES: \n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\nREQUIREMENTS: \n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\nQUALIFICATIONS: \n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\nABOUT US: \n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\nCOMPANY OVERVIEW: \n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n\n"
print(desc)

# Clean up extra whitespace
desc1 = re.sub(r'\n\s*\n', '\n\n', desc)  # Multiple newlines to double
desc2 = re.sub(r'\s+', ' ', desc)         # Multiple spaces to single

print('-'*100)
print(desc1)
print('-'*100)
print(desc2)