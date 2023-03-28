from collections import namedtuple

# create a named tuple with city and state
City = namedtuple("City", ["city", "state"])

cities = {
    (45.52, -122.68): City("Portland", "OR"),
    (40.71, -74.01): City("New York", "NY"),
    (41.85, -87.65): City("Chicago", "IL"),
    (47.61, -122.33): City("Seattle", "WA"),
    (32.78, -96.81): City("Dallas", "TX"),
    (29.76, -95.37): City("Houston", "TX"),
    (39.95, -75.16): City("Philadelphia", "PA"),
    (33.44, -112.07): City("Phoenix", "AZ"),
    #   (35.23, -80.84): City('Charlotte', 'NC'),
    #   (36.17, -115.14): City('Las Vegas', 'NV'),
    #   (37.77, -122.42): City('San Francisco', 'CA'),
    #   (38.91, -77.04): City('Washington', 'DC'),
    #   (39.74, -104.99): City('Denver', 'CO'),
    #   (38.58, -121.49): City('Sacramento', 'CA'),
    #   (37.36, -121.93): City('San Jose', 'CA'),
}
