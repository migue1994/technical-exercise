# Technical Exercise

This project aims to migrate data from csv files to a MySQL database through a batch processing, using a java API rest built on java spark. This API let us to make some complexity sql queries too and it uses apache spark as main data engine.

## Download

For download the project, you must have git on your computer and type this comand in the command line prompt

´$ git clone https://github.com/migue1994/technical-exercise.git´

And open with you prefered IDLE

## Prerequisites:

If you wanto to execute the API, you will need to have this tools or dependencies:

* Java 8 SDK
* Apache Maven
* Git
* Postman or any application that let us to send a request to the API

## Execution

To launch the API rest, you must to be in the root project an type the next commands:

* ´$ mvn clean install´
* ´$ mvn java:exec´

And the API starts to wait for a request. We can use postman for send the next requests:

* http://localhost:4567/api/data/:fileName -> This request let us to migrate the csv file to an MySQL database, where :fileName is a parameter with the name of the csv file we want to migrate (departments.csv, hired_employees.csv, jobs.csv)
* http://localhost:4567/api/data/query/sql -> The request returns the data of the first query from the exercise statement
* http://localhost:4567/api/data/query/sql2 -> The request returns the data of the second query from the exercise statement

## Author

### Miguel Ángel Rivera Rojas

## Licence

[LICENSE.txt](LICENSE.txt)

## MySQL Database

You can conect to the MySql database through a database management with the next credentials:

* Host: globantmysqlserver.mysql.database.azure.com
* Database name: myglobantsql
* Database user: miguel
* Database password: Globant123
* Port number: 3306
