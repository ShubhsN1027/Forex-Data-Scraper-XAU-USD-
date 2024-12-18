# writefile config.py
ALLOWED_ELEMENT_TYPES = {
    "calendar__cell": "date",
    "calendar__cell calendar__date": "date",
    "calendar__cell calendar__time": "time",
    "calendar__cell calendar__currency": "currency",
    "calendar__cell calendar__impact": "impact",
    "calendar__cell calendar__event event": "event"
}

EXCLUDED_ELEMENT_TYPES = [
    "calendar__cell calendar__forecast",
    "calendar__cell calendar__graph",
    "calendar__cell calendar__previous"
]

ICON_COLOR_MAP = {
    "icon icon--ff-impact-yel": "yellow",
    "icon icon--ff-impact-ora": "orange",
    "icon icon--ff-impact-red": "red",
    "icon icon--ff-impact-gra": "gray"
}

ALLOWED_CURRENCY_CODES = ['USD', 'EUR', 'GBP', 'JPY', 'AUD']
ALLOWED_IMPACT_COLORS = ['red', 'orange', 'yellow', 'gray']
