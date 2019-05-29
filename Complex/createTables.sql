-- add all your SQL setup statements here.

-- You can assume that the following base table has been created with data loaded for you when we test your submission
-- (you still need to create and populate it in your instance however),
-- although you are free to insert extra ALTER COLUMN ... statements to change the column
-- names / types if you like.

--FLIGHTS (fid int,
--         month_id int,        -- 1-12
--         day_of_month int,    -- 1-31
--         day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
--         carrier_id varchar(7),
--         flight_num int,
--         origin_city varchar(34),
--         origin_state varchar(47),
--         dest_city varchar(34),
--         dest_state varchar(46),
--         departure_delay int, -- in mins
--         taxi_out int,        -- in mins
--         arrival_delay int,   -- in mins
--         canceled int,        -- 1 means canceled
--         actual_time int,     -- in mins
--         distance int,        -- in miles
--         capacity int,
--         price int            -- in $
--         )

--- BOOK - Make sure you make the corresponding changes to the tables in case of a successful booking

-- Do we have to create itinneary table so that it only contains current itinery and delete all once loged out?
-- possibly firstname and lastname
CREATE TABLE Users(
  username nvarchar(20) PRIMARY KEY,
  password nvarchar(20) NOT NULL,
  balance int,
  CHECK (balance >= 0)
);


--There are many ways to implement this. One possibility is to define a "ID" table that stores the next ID to use, and
--update it each time when a new reservation is made successfully. Make sure you check the case where two different users try to book two different itineraries at the same time!
--MUST CREATE
CREATE TABLE Reservations(
  reservation_id int,
  username nvarchar(20) REFERENCES Users(username),
  paid int DEFAULT 0,
  date Int,
  UNIQUE(username, date),
  fid int REFERENCES flights(fid),
  PRIMARY KEY(reservation_id, fid)

);

-- for keeping record of number of boooking for each flighit
CREATE TABLE Booking(
flight_id INT PRIMARY KEY,
passengers_count INT,
FOREIGN KEY(flight_id) REFERENCES Flights(fid)
);

Create Table ReservationIDs(
    Id int PRIMARY KEY,
    condition int
);

-- INSERT INTO Users
INSERT INTO Users (username, password, balance) VALUES('jay','byunsu93', 10000);
INSERT INTO Users (username, password, balance) VALUES('jung','asdf', 1000);
INSERT INTO Users (username, password, balance) VALUES('brad','asdf', 100);

InSERT INTO ReservationIDs (id, condition) VALUES(0, 0);
