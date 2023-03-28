# from app.utils.weather import make_s3_weather_path, create_start_and_end_dts
# import datetime
# from typing import Tuple
# from unittest.mock import Mock, patch


# def test_make_s3_weather_path():
#     # Test case 1: Check if function returns the correct path for a given input
#     expected = "s3://my-bucket/dt=2022-03-28/new-york_ny_40.7128_-74.0060.csv"
#     result = make_s3_weather_path(
#         bucket="my-bucket",
#         city="New York",
#         state="NY",
#         lat=40.7128,
#         lon=-74.0060,
#         dt="2022-03-28",
#     )
#     assert result == expected, f"Expected: {expected}, got: {result}"


# def test_create_start_and_end_dts():
#     bucket = "my-s3-bucket"

#     # Mock the s3.list_objects function from the boto3 library
#     with patch("boto3.client") as mock_client:
#         mock_list_objects = Mock(return_value={
#             "Contents": [
#                 {"Key": "path/to/my-file/dt=2022-01-01/my-file-2022-01-01.csv"},
#                 {"Key": "path/to/my-file/dt=2022-01-02/my-file-2022-01-02.csv"}
#             ]
#         })
#         mock_client().list_objects_v2 = mock_list_objects

#         # Call the function being tested
#         result = create_start_and_end_dts(bucket)

#         # Assert the expected output
#         expected_start = datetime.datetime(2022, 1, 2, 0, 0)
#         expected_end = datetime.datetime(2023, 3, 27, 23, 0)
#         assert isinstance(result, Tuple)
#         assert len(result) == 2
#         assert result[0] == expected_start
#         assert result[1] == expected_end
