from datetime import date, datetime

def is_leap_year(year):
    """Determines whether a given year is a leap year."""
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

def convert_date(date_str):
    """Converts a date string to a year and day number."""
    year, month, day = map(int, date_str.split('-'))
    date_number = (date(year, month, day) - date(year, 1, 1)).days + 1
    return year, date_number

def get_total_days(start_date, end_date):
    """Calculates the total number of days between two dates."""
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_date_obj - start_date_obj
    return delta.days + 1  # Add 1 to include both start and end dates

