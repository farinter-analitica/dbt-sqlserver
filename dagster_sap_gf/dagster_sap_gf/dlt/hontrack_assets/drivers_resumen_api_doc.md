**API Documentation: Drivers Summary**

**Endpoint:**

POST https://rma.hontrack.com/api/external/vehicles/get\_drivers\_resumen.php

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

"drivers": [ {

"code": "500099",

"name": "NORMAN IGDALID CARCAMO AVILEZ", "group\_id": "c799b7bac8f09eeb3457371c00dc5662", "group\_name": "MOTORISTAS TGU FARMA", "data": [

{

"fchapl": "2024-11-18 00:00:00",

"start\_time": "2024-11-18 07:34:34",

"end\_time": "2024-11-18 17:05:23",

"in\_time": 404,

"out\_time": 4091

},

{

"fchapl": "2024-11-19 00:00:00", "start\_time": "2024-11-19 07:57:29", "end\_time": "2024-11-19 17:01:04", "in\_time": 339,

"out\_time": 2925

}, {

"fchapl": "2024-11-20 00:00:00", "start\_time": "2024-11-20 07:38:03",

"end\_time": "2024-11-20 17:03:36", "in\_time": 294,

"out\_time": 3102

},

{

"fchapl": "2024-11-21 00:00:00",

"start\_time": "2024-11-21 07:32:10", "end\_time": "2024-11-21 17:11:44",

"in\_time": 459,

"out\_time": 3020

}

]

},

 {

"code": "500160",

"name": "CARLOS AMILCAR ESCOBAR PEREZ", "group\_id": "c799b7bac8f09eeb3457371c00dc5662", "group\_name": "MOTORISTAS TGU FARMA", "data": [

{

"fchapl": "2024-11-18 00:00:00",

"start\_time": "2024-11-18 00:00:04", "end\_time": "2024-11-18 23:59:41",

"in\_time": 1743,

"out\_time": 12657

},

{

"fchapl": "2024-11-19 00:00:00", "start\_time": "2024-11-19 00:00:11", "end\_time": "2024-11-19 23:59:45", "in\_time": 1390,

"out\_time": 7249

}, {

"fchapl": "2024-11-20 00:00:00", "start\_time": "2024-11-20 00:00:15", "end\_time": "2024-11-20 23:59:52", "in\_time": 1358,

"out\_time": 2961

}, {

"fchapl": "2024-11-21 00:00:00", "start\_time": "2024-11-21 00:00:22", "end\_time": "2024-11-21 23:59:55", "in\_time": 1287,

"out\_time": 5912

}

]

}

]

}

**Response Parameters:**

- status: Status of the request (e.g., "OK").
  - code: Status code for the API response (e.g., 200, 400, 401).
    - 200: Request was successful.
    - 400: Bad Request - The request could not be understood or was missing required parameters.
    - 401: Unauthorized - The provided API key is invalid or missing.
- request: Object containing the original request parameters (from\_date and to\_date).
- payload: Contains detailed information about drivers.

|<p>- enterprise\_id: The unique identifier of the enterprise.</p><p>- drivers: An array of drivers objects.</p>|||
| - | :- | :- |
|<p>- code: Driver Unique Code</p><p>- name: Driver Name</p><p>- group\_id: Driver Group ID</p><p>- group\_name: Driver Group Name</p><p>- data: Dates Range Data Sessions</p>|||
|<p>- fchapl: Data Date</p><p>- start\_time: Initial Activity Time</p><p>- end\_time: End Activity Time</p><p>- in\_time: Time inside vehicle (in minutes)</p><p>- out\_time: Time outside vehicle (in minutes)</p>|||


