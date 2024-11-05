**API Documenta on: Vehicles Summary Endpoint:** 

POST https://rma.hontrack.com/api/external/vehicles/get\_vehicles\_resumen.php

**Content-Type:** 

application/json

**Request Body:** 

{

"from\_date": "2021-08-01 00:00:00",

"to\_date": "2021-08-07 23:59:59",

"api\_key": "a6e9c4d0-3b7b-21ec-5621-0242ac130203"

}

- from\_date: Start date for the summary (format: YYYY-MM-DD HH:MM:SS).
- to\_date: End date for the summary (format: YYYY-MM-DD HH:MM:SS).
- api\_key: Unique identi ier for API access.

**Expected Response:** 

HTTP Status Code: 200 (OK)

**Response Body:** 

{

"status": "OK",

"code": 200,

"request": {

"from\_date": "2021-08-01 00:00:00", "to\_date": "2021-08-07 23:59:59"

},

"payload": {

"enterprise\_id": "miempresa", "vehicles": [

{

"plate": "ABC123",

"name": "Vehiculo 1",

"model": "2021",

"type": "Sedan",

"date\_apl": "2021-08-01 00:00:00", "distance\_tot": 1000,

"speed\_max": 120,

"speed\_avg": 60,

"start\_time": "2021-08-01 00:00:00", "end\_time": "2021-08-01 23:59:59", "idle\_time": 100,

"started\_time": 10, "off\_time": 10, "trips\_cant": 10, "stops\_tot": 10

},

{

"plate": "DEF456",

"name": "Vehiculo 2",

"model": "2021",

"type": "Sedan",

"date\_apl": "2021-08-01 00:00:00", "distance\_tot": 1000,

"speed\_max": 120,

"speed\_avg": 60,

"start\_time": "2021-08-01 00:00:00", "end\_time": "2021-08-01 23:59:59", "idle\_time": 100,

"started\_time": 10,

"off\_time": 10,

"trips\_cant": 10,

"stops\_tot": 10

}

],

"total\_regs": 2

}

}

**Response Parameters:** 

- status: Status of the request (e.g., "OK").
- code: Status code for the API response (e.g., 200, 400, 401).
  - 200: Request was successful.
  - 400: Bad Request - The request could not be understood or was missing required

parameters.

- 401: Unauthorized - The provided API key is invalid or missing.
- request: Object containing the original request parameters (from\_date and to\_date).
- payload: Contains detailed information about vehicles.
  - enterprise\_id: The unique identi ier of the enterprise.
  - vehicles: An array of vehicle objects.
    - plate: Vehicle license plate number.
    - name: Vehicle name or identi ier.
    - model: Vehicle model year.
    - type: Vehicle type (e.g., Sedan).
    - date\_apl: Date of application (format: YYYY-MM-DD HH:MM:SS).
    - distance\_tot: Total distance traveled by the vehicle (in meters).
    - speed\_max: Maximum speed recorded (in km/h).
    - speed\_avg: Average speed (in km/h).
    - start\_time: Start time of the vehicle activity.
    - end\_time: End time of the vehicle activity.
    - idle\_time: Total idle time (in minutes).
    - started\_time: Time when the vehicle was started (in minutes).
    - off\_time: Time when the vehicle was turned off (in minutes).
    - trips\_cant: Number of trips taken.
    - stops\_tot: Total number of stops made.
- total\_regs: Total number of vehicles in the response.
