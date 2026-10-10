"""Hide data values in error text before it leaves the machine (email, HTML report).

Database errors often quote the row they choked on. The full text stays in the local logs and in
'status --failed'; what is mailed or put in the report keeps the error and loses the values.
"""
import re

HIDDEN = "***"

# Numbers that are part of the message itself, not data: error codes, row counts this tool wrote,
# positions. Anything glued to a letter or underscore is an identifier (a_table_p20260101) and is kept.
SAFE_NUMBER = r"(?<![A-Za-z_\d])(?<!ORA-)(?<!PLS-)(?<!oracle=)(?<!postgres=)(?<!line )(?<!position )(?<!code )"

RULES = [
    (re.compile(r"(Key \([^)]*\)=)\(.*?\)(?= |$|\.)"), rf"\1({HIDDEN})"),               # Key (id)=(42) already exists
    (re.compile(r"(Failing row contains )\(.*\)"), rf"\1({HIDDEN})"),
    (re.compile(r"(invalid input (?:syntax|value) for [^:]*: )\".*?\""), rf'\1"{HIDDEN}"'),
    (re.compile(r"(COPY \w+, line \d+(?:, column \w+)?: )\".*\""), rf'\1"{HIDDEN}"'),   # CONTEXT of a COPY error
    (re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+"), HIDDEN),                               # e-mail addresses
    (re.compile(SAFE_NUMBER + r"\d{4,}(?![A-Za-z_])"), HIDDEN),                          # any other long number
]


def redact(text):
    if not text:
        return text
    for pattern, replacement in RULES:
        text = pattern.sub(replacement, text)
    return text
