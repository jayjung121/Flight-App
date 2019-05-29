USE tempdb;

CREATE TABLE Reservation (
    ReservationID INT NOT NULL IDENTITY(1,1),
    UserID INT NOT NULL,
    FlightID INT NOT NULL,
    PRIMARY KEY (ReservationID),
    FOREIGN KEY (UserID) REFERENCES Users,
    FOREIGN KEY (FlightID) REFERENCES Flights
);

INSERT INTO Reservation (ReservationID, UserID, FlightID) VALUES
(1000, 1, 5), (1001, 2, 13), (1002, 3, 23), (1003, 4, 60)