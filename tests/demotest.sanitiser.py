import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

from icecream import ic  # noqa: E402

import glyphdeck as gd  # noqa: E402

# Example data with targets for removal
data_example: gd.DataDict = {
    1: [
        r"I like apple bottom jeans 156.a19878, 11/10/2020, jimbo@gmail.com",
        "My birthday is 11/10/2021",
        "Product info: https://t.co/KNkANrdypk \r\r\nTo order",
    ],
    2: [
        "Nothing wrong with this",
        "My email is jeff@babe.com, my ip address is 192.158.1.38",
        "Go to this website: www.website.com.au",
    ],
    3: [
        r"Big number is 1896987, I store my files in C:\Users\username\Documents\GitHub",
        "I like blue jeans, my card number is 2222 4053 4324 8877",
        r"I was born 15/12/1990, a file path is C:\Users\username\Pictures\BYG0Djh.png",
    ],
}

# Initialise the sanitiser object, selecting groups during initialisation (optional)
# If you don't add an argument it will use all of them
print("\nInitialise the santiser_obj")
print("Not specifying the groups will make all the default groups be used")
santiser_obj = ic(gd.Sanitiser(data_example, pattern_groups=["number", "date", "email"]))

print("\nGroups based on __init__")
ic(santiser_obj.all_groups)
ic(santiser_obj.active_groups)
ic(santiser_obj.inactive_groups)

print("\nselect_groups method will change the selection")
ic(santiser_obj.select_groups(["number", "date", "email", "path", "url"]))

print("\nChange the placeholders to be inserted in place of sanitised data")
print("Placeholders are cleaned up beforehand")
ic(
    santiser_obj.set_placeholders(
        placeholder_dict={"email": "EMAILS>>", "date": "<DA>TES>"}
    )
)

print("\nAdd a new pattern to find and replace")
ic(
    santiser_obj.add_pattern(
        pattern_name="custom",
        group="custom_group",
        placeholder="Cust",
        rank=0.5,
        regex=r"jeans",
    )
)

print("\nGroups after changes")
ic(santiser_obj.all_groups)
ic(santiser_obj.active_groups)
ic(santiser_obj.inactive_groups)

print("\nRun the sanitiser with the selected patterns")
ic(santiser_obj.sanitise())

print("\nGet outputs")
ic(santiser_obj.group_matches)
ic(santiser_obj.total_matches)
ic(santiser_obj.output_data)

# Re-enable logging
logging.disable(logging.NOTSET)
