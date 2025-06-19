**API Documentation: Sensors Summary**

**Endpoint:**

POST https://rma.hontrack.com/api/external/vehicles/get\_sensors\_resumen.php

**Content-Type:** application/json

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

"status": "OK", "code": 200, "request": {

"from\_date": "2021-08-01 00:00:00", "to\_date": "2021-08-07 23:59:59"

}

},

"payload": {

"enterprise\_id": "miempresa",

"vehicles": [

{

"plate": "ABC123",

"name": "Vehiculo 1",

"model": "2021",

"type": "Sedan",

"date\_apl": "2021-08-01 00:00:00", "sensor\_cnt": 4,

"data": {

"FUEL1": {

"fuel\_h": 434, "fuel\_l": 6352, "fuel\_m1": 4114, "fuel\_m2": 3074, "fuel\_m3": 1960, "distance": 11653, "fuel\_cap": 75, "avg\_value": 33.37, "max\_value": 98.56, "min\_value": 19.92, "steps\_cnt": 553, "efficiency": {

"ef\_mL": 2099.64, "ef\_KmG": 7.95, "ef\_KmL": 2.1, "ef\_MiG": 4.94

},

"last\_value": 98.56, "consumption": 5.55

},

"SPEED": {

"avg\_value": 13.24, "max\_value": 55, "min\_value": 0, "steps\_cnt": 554

},

"IGNITION": {

"avg\_value": 0.34, "max\_value": 1, "min\_value": 0, "steps\_cnt": 1628

},

"BATT\_VOLT": {

"avg\_value": 12684.48, "max\_value": 13905, "min\_value": 11472, "steps\_cnt": 1628

}

}

}

"total\_regs": 1

}

}

**Response Parameters:**

- status: Status of the request (e.g., "OK").
  - code: Status code for the API response (e.g., 200, 400, 401).
    - 200: Request was successful.
    - 400: Bad Request - The request could not be understood or was missing required parameters.
    - 401: Unauthorized - The provided API key is invalid or missing.
- request: Object containing the original request parameters (from\_date and to\_date).
- payload: Contains detailed information about vehicles.
  - enterprise\_id: The unique identifier of the enterprise.
  - vehicles: An array of vehicle objects.
  - plate: Vehicle license plate number.
  - name: Vehicle name or identifier.
  - model: Vehicle model.
  - type: Vehicle type (e.g., motorcycle).
  - date\_apl: Date of application (format: YYYY-MM-DD HH:MM).
  - sensor\_cnt: Number of sensors attached to the vehicle.
  - data: Object containing elements sensor data. Sensor types with respective measurements and data points.
    - FUEL1: Fuel 1 in %
    - FUEL2: Fuel 2 in %
    - TEMP1: Temperature 1 in Celsius Grades
    - TEMP2: Temperature 2 in Celsius Grades
    - HUMIDITY1: Humidity 1 in %
    - HUMIDITY2: Humidity 2 in %
    - SPEED: Speed in KM/H
    - IGNITION: Ignition
    - BATT\_VOLT: Vehicle Battery voltaje
  - total\_regs: Total number of vehicles in the response.
